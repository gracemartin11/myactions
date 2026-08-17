#!/usr/bin/env python3
"""
Bluesky auto-poster.

Coordination data (Credentials / Settings / PostPlan / LinkPlan / Report)
lives in a Google Sheet, read/written via the Sheets API v4. Media files
(images/videos) still live on Mega, accessed via rclone — unchanged.

WHY GOOGLE SHEETS DOESN'T NEED A WORKBOOK-WIDE LOCK
────────────────────────────────────────────────────
The old Mega-workbook version needed a real distributed lock because a
".xlsx on Mega" has no per-cell API: every write meant "download the whole
file, change it, upload the whole file back", so two runners touching it at
once could corrupt each other's changes.

The Sheets API has no such problem — `values.update` / `values.batchUpdate`
write to specific cells/rows without touching anything else, and Google
serializes concurrent writes to the same cell for you. So this version
drops the whole-workbook lock entirely. What's left is the same *business*
locks it always had:
  - a soft, TTL-based "which repo currently owns this account row" lock
    (LOCKED_BY / LOCKED_AT columns) — unchanged in spirit from before.
  - "claim" columns on PostPlan/LinkPlan rows so two runners don't post the
    same row twice — same read-then-write pattern as before.
Both are optimistic (read-fresh, then write) rather than hard locks, which
is an acceptable trade-off for a per-account content queue, and it's how
LinkPlan claiming already worked even in the Mega version.

QUOTA / RATE-LIMIT DESIGN
────────────────────────────
Google Sheets API default quotas are per-minute, per-project *and*
per-user (the same service account = the same "user" across every parallel
matrix job). Running 10+ accounts at once, each doing lots of small
reads/writes, is exactly how you blow through that. This file is built
around minimizing call *count*, not just being polite about it:

  1. BATCH READS: Credentials + Settings + Report are fetched together in
     ONE `values.batchGet` call per cycle (multiple ranges in one HTTP
     request only counts once against quota). PostPlan/LinkPlan are then
     fetched together in a second batchGet once we know their tab names
     from Settings. That's ~2 read calls per account per cycle, full stop.

  2. BATCH WRITES: related cell updates (e.g. LOCKED_BY + LOCKED_AT, or
     ASSIGNED_REPO + ASSIGNED_STATUS + ASSIGNED_AT) go out together via
     `values.batchUpdate` instead of one call per cell.

  3. RETRY WITH BACKOFF: every Sheets call is wrapped in `sheets_call()`,
     which catches 429 / RESOURCE_EXHAUSTED / rateLimitExceeded / transient
     5xx errors and retries with exponential backoff + jitter (capped at
     60s per wait, up to SHEETS_RETRY_BUDGET_SECONDS total). Since Google's
     quota window is per-minute, a short backoff loop is usually enough to
     just wait for the next window rather than treating it as a hard
     failure.

  4. STARTUP JITTER: each parallel account job sleeps a random amount
     (0..START_JITTER_MAX_SECONDS) before its very first Sheets call, so a
     10-account matrix doesn't fire 10 simultaneous bursts in the same
     second right when the workflow starts.

  5. NEVER GIVE UP ON THE SCHEDULE: if a cycle still can't get through after
     the retry budget is exhausted, `run_once()` raises, `main()`'s loop
     catches it, logs it, and sleeps until the next cycle. The account
     keeps trying every loop_interval instead of the job exiting.

PROXY SUPPORT (per-account, sheet-managed)
────────────────────────────────────────────────────
Each Credentials row can be given its own outbound HTTP(S) proxy, pulled
from a "Proxies" tab in the same spreadsheet. Design:

  - The Proxies tab is a pool of candidate proxies (IP / Port / optional
    ResponseTime(s) / Status), plus tracking columns this script manages:
    ASSIGNED_TO, ASSIGNED_AT, LAST_CHECKED, LAST_CHECK_OK.
  - An account keeps the SAME proxy across cycles once assigned (persisted
    in Credentials via PROXY_IP / PROXY_PORT / PROXY_ASSIGNED_AT) — it does
    NOT reshuffle every cycle.
  - Every cycle, before login, the currently-assigned proxy (if any) is
    re-checked for liveness. If it's dead, it's marked "dead" in the
    Proxies tab (so nobody else picks it) and released from the account;
    a new, not-yet-used proxy is then claimed.
  - Claiming a fresh proxy is optimistic-locked the same way LinkPlan rows
    are (re-read the Status cell right before writing "assigned" to narrow
    the race between two account jobs grabbing the same proxy).
  - A dead proxy is marked "dead" and never handed out again; a proxy that
    was released back to the pool (e.g. its account got banned) goes back
    to "" (free) so it can be reused elsewhere.

CALL-SITE CONVENTION FOR sheets_call()
────────────────────────────────────────
`sheets_call()` takes a zero-argument callable (`request_factory`) and
calls `request_factory().execute()` on every attempt — never
`request_factory(**kwargs)`. This matters for retries: if a network error
triggers `_build_sheets_client()` mid-retry, a request object built once
up front (e.g. a bound method reference captured before the loop) would
still point at the OLD, now-dead client. Every call site below therefore
wraps the actual Sheets API call in a `lambda: ...` so each retry
attempt re-reads the (possibly just-rebuilt) global `_sheets` and
re-evaluates the whole chain (including `.values()`) fresh, rather than
passing kwargs into `sheets_call` itself for it to forward — `sheets_call`
does not accept or forward extra kwargs.
"""
import http.client
import io
import json
import os
import random
import re
import socket
import ssl
import subprocess
import sys
import time
import uuid
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from atproto import Client, models
from atproto_client.utils import TextBuilder
from atproto_client.request import Request as AtprotoRequest

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

RUN_TAG      = os.getenv("GITHUB_RUN_ID") or f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
CLAIM_PREFIX = "CLAIMED_"

CURRENT_REPO     = os.getenv("GITHUB_REPOSITORY") or f"local-{socket.gethostname()}"
LOCK_TTL_MINUTES = 45   # business-level "which repo owns this account row" heartbeat TTL

# ═══════════════════════════════════════════════════════════════════════════
#  ENV / VALUE PARSING HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def get_env(name, required=True, default=""):
    v = os.getenv(name)
    if v is None or not v.strip():
        if required:
            raise RuntimeError(f"Missing required env var: {name}")
        return default
    return v.strip()

def _parse_bool(raw, default=False):
    if raw is None or not str(raw).strip():
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "on")

def _parse_pct(raw, default):
    if raw is None or not str(raw).strip():
        return default
    raw = str(raw).strip().rstrip("%")
    try:
        v = float(raw)
        return v / 100.0 if v > 1 else v
    except ValueError:
        return default

def _parse_plain_float(raw, default):
    if raw is None or not str(raw).strip():
        return default
    try:
        return float(str(raw).strip())
    except ValueError:
        return default

def _parse_int(raw, default):
    if raw is None or not str(raw).strip():
        return default
    try:
        return max(1, int(str(raw).strip()))
    except ValueError:
        return default

def get_bool_env(name, default=False):
    return _parse_bool(os.getenv(name), default)

def get_float_env(name, default):
    return _parse_pct(os.getenv(name), default)

def get_int_env(name, default):
    return _parse_int(os.getenv(name), default)


# ═══════════════════════════════════════════════════════════════════════════
#  STATIC WORKFLOW KNOBS
# ═══════════════════════════════════════════════════════════════════════════

ACCOUNT_ROW = get_int_env("ACCOUNT_ROW", 1)   # 1-based data row (header is row 1 in the sheet)

RCLONE_CONFIG_PATH = get_env("RCLONE_CONFIG_PATH", required=False) or "rclone.conf"
RCLONE_REMOTE_NAME = get_env("RCLONE_REMOTE_NAME", required=False) or "mega"

CREDS_TAB    = "Credentials"
SETTINGS_TAB = "Settings"
REPORT_TAB   = "Report"
PROXIES_TAB  = "Proxies"

SKIP_STATUS_MARKERS = ("banned", "suspended", "taken down", "auth failed")

REPORT_HEADER = ["Timestamp (UTC)", "Handle", "Followers", "Gained", "Top Post", "Engagement", "Status"]

CREDENTIALS_HEADER = [
    "BSKY_HANDLE", "BSKY_APP_PW", "HASHTAGS",
    "MEGA_UPLOAD_FOLDER", "MEGA_PROCESSED_FOLDER",
    "LINK_URL", "LINK_DISPLAY_TEXT",
    "LOCKED_BY", "LOCKED_AT",
    "ACCOUNT_STATUS", "ACCOUNT_STATUS_AT",
    "ASSIGNED_REPO", "ASSIGNED_STATUS", "ASSIGNED_AT",
    "PROXY_IP", "PROXY_PORT", "PROXY_ASSIGNED_AT",
]

# Base columns for a brand-new Proxies tab (only used if the tab doesn't
# exist yet at all). If you already created the tab yourself (as in this
# setup), ensure_extra_columns() below will just append the tracking
# columns it needs onto the end of your existing header — it never
# touches your existing IP/Port/ResponseTime/Status data.
PROXIES_BASE_HEADER = ["IP", "Port", "ResponseTime(s)", "Status"]
# Tracking columns this script owns and manages automatically. "Status" is
# included here (not just in PROXIES_BASE_HEADER) so it gets re-added if
# it's ever missing/deleted from an existing tab — without it there's no
# single authoritative "assigned"/"dead" field, which is what let dead
# proxies get reused instead of permanently skipped.
PROXY_TRACKING_COLUMNS = ["Status", "ASSIGNED_TO", "ASSIGNED_AT", "LAST_CHECKED", "LAST_CHECK_OK"]

DEFAULT_SETTINGS = [
    ("IMAGE_RATIO", "0.60"),
    ("VIDEO_RATIO", "0.40"),
    ("LINK_RATIO", "0.0"),
    ("HASHTAGS_ENABLED_IMAGE", "true"),
    ("HASHTAGS_ENABLED_VIDEO", "false"),
    ("HASHTAGS_ENABLED_LINK", "true"),
    ("LINK_ENABLED_IMAGE", "true"),
    ("LINK_ENABLED_VIDEO", "true"),
    ("LINK_PERCENTAGE", "1.0"),
    ("MAX_IMAGE_MB", "2.0"),
    ("CAPTION_ENABLED", "true"),
    ("AUTO_CAPTION_ENABLED_LINK", "true"),
    ("PREVIEW_FETCH_TIMEOUT", "15"),
    ("MAX_THUMB_MB", "1.0"),
    ("ENABLE_REPORT", "false"),
    ("REPORT_TIMES_PER_DAY", "1"),
    ("TOP_POSTS_COUNT", "1"),
    ("TOP_POSTS_WITHIN", "30"),
    ("POST_PLAN_SHEET_NAME", "PostPlan"),
    ("LINK_PLAN_SHEET_NAME", "LinkPlan"),
    ("LOOP_INTERVAL_SECONDS", "1800"),
    ("MAX_ACCOUNTS_PER_RUN", ""),
    ("USE_PROXY", "true"),
    ("PROXY_REQUIRED", "true"),
    ("PROXY_CHECK_TIMEOUT_SECONDS", "10"),
]

POSTED_STATUS_VALUE = "posted"
ASSIGN_STATUS_IN_USE = "In Use"

_URL_RE     = re.compile(r"https?://\S+")
_MENTION_RE = re.compile(r"@\S+")

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

CARDYB_EXTRACT_URL = "https://cardyb.bsky.app/v1/extract"
LINK_PREVIEW_MAX_RETRIES = 3
LINK_PREVIEW_RETRY_DELAY = 2

DEFAULT_LOOP_INTERVAL_SECONDS = 1800

# Used to test whether a proxy is actually usable for talking to Bluesky.
PROXY_CHECK_URL = "https://bsky.social/xrpc/com.atproto.server.describeServer"


# ═══════════════════════════════════════════════════════════════════════════
#  RCLONE HELPERS (media files only — images/videos on Mega, unchanged)
# ═══════════════════════════════════════════════════════════════════════════

def _rclone_run(args, timeout=120):
    return subprocess.run(
        ["rclone", "--config", RCLONE_CONFIG_PATH] + args,
        capture_output=True, text=True, timeout=timeout,
    )

def rclone_list_files(remote_folder):
    result = _rclone_run(["lsf", remote_folder, "--files-only"])
    if result.returncode != 0:
        print(f"Warning: rclone lsf failed for '{remote_folder}': {result.stderr.strip()[-300:]}")
        return []
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]

def rclone_claim(remote_folder, name):
    claimed_name = f"{CLAIM_PREFIX}{RUN_TAG}__{name}"
    result = _rclone_run(["moveto", f"{remote_folder}/{name}", f"{remote_folder}/{claimed_name}"])
    return claimed_name if result.returncode == 0 else None

def rclone_download(remote_folder, filename, local_path, timeout=120):
    result = _rclone_run(["copyto", f"{remote_folder}/{filename}", local_path], timeout=timeout)
    return result.returncode == 0

def rclone_move(src, dst, timeout=120):
    result = _rclone_run(["moveto", src, dst], timeout=timeout)
    return result.returncode == 0


# ═══════════════════════════════════════════════════════════════════════════
#  GOOGLE SHEETS: LOW-LEVEL CLIENT + QUOTA-AWARE RETRY WRAPPER
# ═══════════════════════════════════════════════════════════════════════════

GOOGLE_SHEET_ID                 = get_env("GOOGLE_SHEET_ID")
GOOGLE_APPLICATION_CREDENTIALS  = get_env("GOOGLE_APPLICATION_CREDENTIALS", required=False) or "google_creds.json"
SHEETS_RETRY_BUDGET_SECONDS     = get_int_env("SHEETS_RETRY_BUDGET_SECONDS", 240)   # per call
SHEETS_MAX_BACKOFF_SECONDS      = get_int_env("SHEETS_MAX_BACKOFF_SECONDS", 60)
START_JITTER_MAX_SECONDS        = get_int_env("START_JITTER_MAX_SECONDS", 45)
# Small pause between consecutive Sheets calls from this one process, purely
# to smooth out bursts (a cycle only makes a handful of calls, so this adds
# at most ~1-2s of latency per cycle — cheap insurance against quota spikes).
SHEETS_CALL_PACING_SECONDS      = _parse_plain_float(os.getenv("SHEETS_CALL_PACING_SECONDS"), 0.3)

_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
_creds  = service_account.Credentials.from_service_account_file(
    GOOGLE_APPLICATION_CREDENTIALS, scopes=_SCOPES
)
_service = None
_sheets  = None


def _build_sheets_client():
    """(Re)build the googleapiclient service + spreadsheets() resource.
    Called once at import, and again whenever a network-level error (as
    opposed to a clean HTTP error response) suggests the underlying
    connection is dead — e.g. after a long idle sleep between cycles, the
    server or an intermediate NAT/load-balancer may have silently closed
    the socket the client still thinks is usable, which surfaces as a raw
    SSL/connection error rather than a normal HTTP 4xx/5xx."""
    global _service, _sheets
    _service = build("sheets", "v4", credentials=_creds, cache_discovery=False)
    _sheets  = _service.spreadsheets()


_build_sheets_client()

# Exceptions that mean "the connection itself is broken", not "the server
# gave us a proper HTTP error". These are typically what you get when a
# long-idle keep-alive connection gets silently killed server-side or by a
# NAT/firewall in between (very common after a 20-60 minute sleep()).
_NETWORK_TRANSIENT_EXCEPTIONS = (
    ssl.SSLError,
    ConnectionError,           # covers ConnectionResetError, BrokenPipeError, etc.
    http.client.HTTPException, # covers http.client.RemoteDisconnected, BadStatusLine, etc.
    TimeoutError,
    socket.timeout,
    socket.error,
    OSError,
)


def _is_quota_error(exc):
    if not isinstance(exc, HttpError):
        return False
    status = getattr(exc.resp, "status", None)
    body = ""
    try:
        body = exc.content.decode("utf-8", "ignore") if isinstance(exc.content, bytes) else str(exc.content)
    except Exception:
        body = str(exc)
    return (
        status == 429
        or "RESOURCE_EXHAUSTED" in body
        or "rateLimitExceeded" in body
        or "Quota exceeded" in body
        or "quotaExceeded" in body
    )


def sheets_call(request_factory, budget_seconds=None):
    """Run a Sheets API request with exponential backoff + jitter on quota
    errors, transient 5xx, AND raw network/SSL errors from dead/idle
    connections. Retries for up to `budget_seconds` (default
    SHEETS_RETRY_BUDGET_SECONDS) before giving up and re-raising — at which
    point the caller (a cycle-level try/except in run_once/main) is
    expected to skip this cycle rather than crash the whole job.

    `request_factory` MUST be a zero-arg callable that BUILDS the request
    fresh each attempt (e.g. `lambda: _sheets.values().get(spreadsheetId=...,
    range=...)`), rather than a pre-bound method or a method reference with
    kwargs tacked onto this call. This matters: if a network error triggers
    a client rebuild mid-retry, a pre-bound method (or one captured before
    the retry loop starts) would still point at the old (dead) client
    object. A zero-arg lambda re-reads the (possibly just-rebuilt) global
    `_sheets` on every attempt. `sheets_call` itself does not accept or
    forward any Sheets API kwargs — all of that belongs inside the lambda
    passed in by the caller."""
    budget = budget_seconds if budget_seconds is not None else SHEETS_RETRY_BUDGET_SECONDS
    start = time.time()
    attempt = 0
    while True:
        try:
            result = request_factory().execute()
            if SHEETS_CALL_PACING_SECONDS > 0:
                time.sleep(SHEETS_CALL_PACING_SECONDS)
            return result
        except HttpError as exc:
            status = getattr(exc.resp, "status", None)
            transient = _is_quota_error(exc) or status in (408, 429, 500, 502, 503, 504)
            if not transient:
                raise
            attempt += 1
            elapsed = time.time() - start
            if elapsed > budget:
                print(f"[sheets] giving up after {attempt} attempts / {elapsed:.0f}s "
                      f"(still hitting {'quota' if _is_quota_error(exc) else f'HTTP {status}'}).")
                raise
            delay = min(SHEETS_MAX_BACKOFF_SECONDS, 2 * (2 ** (attempt - 1))) + random.uniform(0, 1.5)
            kind = "quota" if _is_quota_error(exc) else f"HTTP {status}"
            print(f"[sheets] {kind} error (attempt {attempt}, {elapsed:.0f}s elapsed) — "
                  f"retrying in {delay:.1f}s…")
            time.sleep(delay)
        except _NETWORK_TRANSIENT_EXCEPTIONS as exc:
            attempt += 1
            elapsed = time.time() - start
            if elapsed > budget:
                print(f"[sheets] giving up after {attempt} attempts / {elapsed:.0f}s "
                      f"(still hitting network error: {exc!r}).")
                raise
            delay = min(SHEETS_MAX_BACKOFF_SECONDS, 2 * (2 ** (attempt - 1))) + random.uniform(0, 1.5)
            print(f"[sheets] network/connection error (attempt {attempt}, {elapsed:.0f}s elapsed): "
                  f"{exc!r} — rebuilding client and retrying in {delay:.1f}s…")
            # The whole reason this happens is a stale/dead connection the
            # client is still holding onto (usually after a long sleep()
            # between cycles). Rebuilding gets a fresh connection instead
            # of retrying against the same dead socket.
            try:
                _build_sheets_client()
            except Exception as rebuild_exc:
                print(f"[sheets] warning: failed to rebuild client ({rebuild_exc}); will retry anyway.")
            time.sleep(delay)


def qrange(tab, a1):
    """Build an A1 range reference for a tab, quoting the tab name (safe
    even if it contains spaces or special characters)."""
    safe = tab.replace("'", "''")
    return f"'{safe}'!{a1}"


def col_letter(n):
    letters = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        letters = chr(65 + r) + letters
    return letters


def sheets_get_batch(ranges):
    return sheets_call(
        lambda: _sheets.values().batchGet(
            spreadsheetId=GOOGLE_SHEET_ID, ranges=ranges,
            valueRenderOption="UNFORMATTED_VALUE",
        )
    )


def sheets_get(rng):
    return sheets_call(
        lambda: _sheets.values().get(
            spreadsheetId=GOOGLE_SHEET_ID, range=rng,
            valueRenderOption="UNFORMATTED_VALUE",
        )
    )


def sheets_update(rng, values):
    return sheets_call(
        lambda: _sheets.values().update(
            spreadsheetId=GOOGLE_SHEET_ID, range=rng,
            valueInputOption="USER_ENTERED", body={"values": values},
        )
    )


def sheets_batch_update_values(data):
    """data: list of {'range': A1_range, 'values': [[...]]}. One HTTP call
    no matter how many ranges are included — always prefer this over
    several sheets_update() calls when writing related cells together."""
    if not data:
        return None
    return sheets_call(
        lambda: _sheets.values().batchUpdate(
            spreadsheetId=GOOGLE_SHEET_ID,
            body={"valueInputOption": "USER_ENTERED", "data": data},
        )
    )


def sheets_append(rng, values):
    return sheets_call(
        lambda: _sheets.values().append(
            spreadsheetId=GOOGLE_SHEET_ID, range=rng,
            valueInputOption="USER_ENTERED", insertDataOption="INSERT_ROWS",
            body={"values": values},
        )
    )


def sheets_existing_titles():
    meta = sheets_call(
        lambda: _sheets.get(spreadsheetId=GOOGLE_SHEET_ID, fields="sheets.properties.title")
    )
    return {s["properties"]["title"] for s in meta.get("sheets", [])}


REQUIRED_TABS = {
    CREDS_TAB:    CREDENTIALS_HEADER,
    SETTINGS_TAB: ["KEY", "VALUE"],
    REPORT_TAB:   REPORT_HEADER,
    PROXIES_TAB:  PROXIES_BASE_HEADER,
}


def ensure_required_tabs():
    """One-time (per job start) check that Credentials/Settings/Report/
    Proxies tabs and their headers exist.

    Matching against existing tab titles is done case-/whitespace-
    insensitively, because Google Sheets itself treats names like
    "Proxies" and "Proxies " (or different casing) as colliding when you
    try to create one — so an exact-match check can miss a tab that's
    really already there and then blow up trying to "create" it.

    Tabs are created one at a time (not as a single multi-request
    batchUpdate) and any "already exists" error is treated as a harmless
    no-op rather than a crash, since Sheets batchUpdate requests are
    all-or-nothing: one bad addSheet in a combined call would otherwise
    block every other tab in the same call from being created too."""
    existing = sheets_existing_titles()
    existing_norm = {str(t).strip().lower() for t in existing}

    header_writes = []
    for tab, header in REQUIRED_TABS.items():
        if tab.strip().lower() in existing_norm:
            continue
        try:
            sheets_call(
                lambda t=tab: _sheets.batchUpdate(
                    spreadsheetId=GOOGLE_SHEET_ID,
                    body={"requests": [{"addSheet": {"properties": {"title": t}}}]},
                )
            )
            header_writes.append({"range": qrange(tab, "A1"), "values": [header]})
            print(f"Created missing tab: {tab}")
        except HttpError as exc:
            body = ""
            try:
                body = exc.content.decode("utf-8", "ignore") if isinstance(exc.content, bytes) else str(exc.content)
            except Exception:
                body = str(exc)
            if "already exists" in body:
                print(f"Note: tab '{tab}' already exists (under a slightly different name/case) — skipping creation.")
            else:
                raise
    if header_writes:
        sheets_batch_update_values(header_writes)


def ensure_extra_columns(tab, required_headers):
    """Append any of `required_headers` that are missing from `tab`'s
    header row, as new trailing columns. Never touches or reorders any
    existing columns/data — safe to call on a tab you already populated
    yourself (e.g. a Proxies tab you built with IP/Port/ResponseTime(s)/
    Status already in it)."""
    res = sheets_get(qrange(tab, "1:1"))
    rows = res.get("values", [])
    existing_header = rows[0] if rows else []
    existing_upper = {str(h).strip().upper() for h in existing_header if str(h).strip()}

    missing = [h for h in required_headers if h.upper() not in existing_upper]
    if not missing:
        return

    start_col = len(existing_header) + 1
    sheets_update(qrange(tab, f"{col_letter(start_col)}1"), [missing])
    print(f"Added missing column(s) to '{tab}': {missing}")


def ensure_settings_defaults():
    core = load_core_data(force=True)
    settings_existing = set(core["settings"].keys())
    missing_rows = [[k, v] for k, v in DEFAULT_SETTINGS if k not in settings_existing]
    if missing_rows:
        sheets_append(qrange(SETTINGS_TAB, "A:B"), missing_rows)
        print(f"Added {len(missing_rows)} missing default setting(s).")
        load_core_data(force=True)   # refresh cache so this cycle sees them


# ═══════════════════════════════════════════════════════════════════════════
#  SHEET VALUE HELPERS (operate on cached 2D lists from batchGet)
# ═══════════════════════════════════════════════════════════════════════════

def hmap(values):
    """Header map: {UPPERCASE_HEADER_NAME: 1-based column index}."""
    if not values:
        return {}
    header = {}
    for i, v in enumerate(values[0], start=1):
        if v is not None and str(v).strip():
            header[str(v).strip().upper()] = i
    return header

def cell(values, row, col):
    """1-based row/col cell lookup into a 2D list from batchGet (rows and
    trailing empty cells are omitted by the API, so this pads safely)."""
    if col is None or row < 1 or row > len(values):
        return ""
    r = values[row - 1]
    if col > len(r):
        return ""
    v = r[col - 1]
    return str(v).strip() if v is not None else ""


# ═══════════════════════════════════════════════════════════════════════════
#  CYCLE-LEVEL DATA LOADING (batched, cached per cycle)
# ═══════════════════════════════════════════════════════════════════════════

_core_cache = None   # {"creds": [[...]], "settings": {...}, "report": [[...]]}
_plan_cache = None   # {"key": (post_tab, link_tab), "by_tab": {tab: [[...]]}}


def load_core_data(force=False):
    """ONE batchGet call fetching Credentials + Settings + Report together.
    This is the single read that covers everything needed for a normal
    posting cycle except the plan sheets."""
    global _core_cache
    if _core_cache is not None and not force:
        return _core_cache

    ranges = [qrange(CREDS_TAB, "A:ZZ"), qrange(SETTINGS_TAB, "A:ZZ"), qrange(REPORT_TAB, "A:ZZ")]
    res = sheets_get_batch(ranges)
    vr = res.get("valueRanges", [])

    def vals(i):
        return vr[i].get("values", []) if i < len(vr) else []

    creds_values, settings_values, report_values = vals(0), vals(1), vals(2)

    settings = {}
    for row in settings_values[1:]:
        if row and str(row[0]).strip():
            key = str(row[0]).strip().upper()
            val = str(row[1]).strip() if len(row) > 1 and row[1] is not None else ""
            settings[key] = val

    _core_cache = {"creds": creds_values, "settings": settings, "report": report_values}
    return _core_cache


def load_plan_data(post_plan_tab, link_plan_tab, force=False):
    """ONE batchGet call fetching PostPlan + LinkPlan together (only the
    tabs that are actually configured, and only once per unique pair of
    names per cycle)."""
    global _plan_cache
    key = (post_plan_tab, link_plan_tab)
    if _plan_cache is not None and _plan_cache["key"] == key and not force:
        return _plan_cache

    tabs = []
    if post_plan_tab:
        tabs.append(post_plan_tab)
    if link_plan_tab and link_plan_tab not in tabs:
        tabs.append(link_plan_tab)

    by_tab = {}
    if tabs:
        ranges = [qrange(t, "A:ZZ") for t in tabs]
        res = sheets_get_batch(ranges)
        vr = res.get("valueRanges", [])
        for t, r in zip(tabs, vr):
            by_tab[t] = r.get("values", [])

    _plan_cache = {"key": key, "by_tab": by_tab}
    return _plan_cache


# ═══════════════════════════════════════════════════════════════════════════
#  ACCOUNT CONFIG + LIVE SETTINGS
# ═══════════════════════════════════════════════════════════════════════════

_account_config = None


def load_global_settings(force_refresh=False):
    return load_core_data(force=force_refresh)["settings"]


def load_account_config(force_refresh=False):
    global _account_config

    if _account_config is not None and not force_refresh:
        return _account_config

    core = load_core_data(force=force_refresh)
    creds_values = core["creds"]
    header = hmap(creds_values)

    excel_row = ACCOUNT_ROW + 1
    if excel_row > len(creds_values):
        raise RuntimeError(
            f"ACCOUNT_ROW={ACCOUNT_ROW} but '{CREDS_TAB}' only has "
            f"{max(0, len(creds_values) - 1)} data row(s)."
        )

    def col(*names):
        for n in names:
            c = header.get(n.upper())
            if c is not None:
                return cell(creds_values, excel_row, c)
        return ""

    shared = core["settings"]
    def setting(key):
        return col(key) or shared.get(key, "")

    raw_link     = col("LINK_URL") or "https://foodiesposts.com"
    link_url     = raw_link if raw_link.startswith("http") else f"https://{raw_link}"
    link_display = col("LINK_DISPLAY_TEXT") or link_url.replace("https://", "").replace("http://", "")

    img_ratio_raw  = _parse_pct(setting("IMAGE_RATIO"), 0.60)
    vid_ratio_raw  = _parse_pct(setting("VIDEO_RATIO"), 0.40)
    link_ratio_raw = _parse_pct(setting("LINK_RATIO"), 0.0)
    ratio_sum      = img_ratio_raw + vid_ratio_raw + link_ratio_raw
    if ratio_sum > 0:
        image_ratio = img_ratio_raw / ratio_sum
        video_ratio = vid_ratio_raw / ratio_sum
        link_ratio  = link_ratio_raw / ratio_sum
    else:
        image_ratio, video_ratio, link_ratio = 0.60, 0.40, 0.0

    cfg = {
        "handle":                col("BSKY_HANDLE"),
        "app_pw":                col("BSKY_APP_PW"),
        "link_url":              link_url,
        "link_display_text":     link_display,
        "hashtags_raw":          col("HASHTAGS"),
        "mega_upload_folder":    col("MEGA_UPLOAD_FOLDER"),
        "mega_processed_folder": col("MEGA_PROCESSED_FOLDER"),
        "row_num":               ACCOUNT_ROW,

        "image_ratio":              image_ratio,
        "video_ratio":              video_ratio,
        "link_ratio":               link_ratio,
        "hashtags_enabled_image":   _parse_bool(setting("HASHTAGS_ENABLED_IMAGE"), True),
        "hashtags_enabled_video":   _parse_bool(setting("HASHTAGS_ENABLED_VIDEO"), False),
        "hashtags_enabled_link":    _parse_bool(setting("HASHTAGS_ENABLED_LINK"), True),
        "link_enabled_image":       _parse_bool(setting("LINK_ENABLED_IMAGE"), True),
        "link_enabled_video":       _parse_bool(setting("LINK_ENABLED_VIDEO"), True),
        "link_percentage":          _parse_pct(setting("LINK_PERCENTAGE"), 1.0),
        "max_image_bytes":          int(_parse_plain_float(setting("MAX_IMAGE_MB"), 2.0) * 1024 * 1024),
        "caption_enabled":          _parse_bool(setting("CAPTION_ENABLED"), True),
        "auto_caption_enabled_link": _parse_bool(setting("AUTO_CAPTION_ENABLED_LINK"), True),
        "preview_timeout":          _parse_int(setting("PREVIEW_FETCH_TIMEOUT"), 15),
        "max_thumb_bytes":          int(_parse_plain_float(setting("MAX_THUMB_MB"), 1.0) * 1024 * 1024),
        "enable_report":            _parse_bool(setting("ENABLE_REPORT"), False),
        "report_times_per_day":     _parse_int(setting("REPORT_TIMES_PER_DAY"), 1),
        "top_posts_count":          _parse_int(setting("TOP_POSTS_COUNT"), 1),
        "top_posts_within":         _parse_int(setting("TOP_POSTS_WITHIN"), 30),
        "post_plan_sheet_name":     setting("POST_PLAN_SHEET_NAME") or "PostPlan",
        "link_plan_sheet_name":     setting("LINK_PLAN_SHEET_NAME") or "LinkPlan",
        "loop_interval_seconds":    _parse_int(setting("LOOP_INTERVAL_SECONDS"),
                                                DEFAULT_LOOP_INTERVAL_SECONDS),

        "locked_by": col("LOCKED_BY"),
        "locked_at": col("LOCKED_AT"),
        "account_status": col("ACCOUNT_STATUS"),
        "has_lock_columns": ("LOCKED_BY" in header and "LOCKED_AT" in header),

        # ── Proxy config (per-account persisted assignment + global knobs) ──
        "use_proxy":            _parse_bool(setting("USE_PROXY"), True),
        "proxy_required":       _parse_bool(setting("PROXY_REQUIRED"), True),
        "proxy_check_timeout":  _parse_int(setting("PROXY_CHECK_TIMEOUT_SECONDS"), 10),
        "proxy_ip":             col("PROXY_IP"),
        "proxy_port":           col("PROXY_PORT"),
        "proxy_assigned_at":    col("PROXY_ASSIGNED_AT"),
    }

    if not cfg["handle"]:
        raise RuntimeError(
            f"BSKY_HANDLE is empty for account row {ACCOUNT_ROW} in '{CREDS_TAB}'."
        )

    _account_config = cfg
    return cfg

def _cfg():
    return load_account_config()

def refresh_account_config():
    return load_account_config(force_refresh=True)


# ═══════════════════════════════════════════════════════════════════════════
#  AUTO ACCOUNT-ROW ASSIGNMENT
#  (only exercised when ACCOUNT_ROW isn't set explicitly — the shipped
#  workflow always passes it via the matrix, so this is a fallback path)
# ═══════════════════════════════════════════════════════════════════════════

def resolve_account_row():
    explicit = get_env("ACCOUNT_ROW", required=False)
    if explicit:
        row = _parse_int(explicit, 1)
        print(f"ACCOUNT_ROW={row} was explicitly set — using it as a manual override "
              f"(auto-assignment skipped).")
        return row

    core = load_core_data(force=True)
    creds_values = core["creds"]
    header = hmap(creds_values)

    def hidx(*names):
        for n in names:
            if n.upper() in header:
                return header[n.upper()]
        return None

    handle_col = hidx("BSKY_HANDLE")
    repo_col   = hidx("ASSIGNED_REPO")
    status_col = hidx("ASSIGNED_STATUS")
    at_col     = hidx("ASSIGNED_AT")

    if handle_col is None or repo_col is None or status_col is None:
        raise RuntimeError(
            f"Auto row-assignment needs 'BSKY_HANDLE', 'ASSIGNED_REPO' and "
            f"'ASSIGNED_STATUS' columns in '{CREDS_TAB}'. Add any missing ones "
            f"to the header row, or set ACCOUNT_ROW manually for this run."
        )

    max_row = len(creds_values)

    for excel_row in range(2, max_row + 1):
        if cell(creds_values, excel_row, repo_col) == CURRENT_REPO:
            handle = cell(creds_values, excel_row, handle_col)
            data_idx = excel_row - 1
            print(f"Repo '{CURRENT_REPO}' already owns Credentials row {data_idx} "
                  f"({handle or 'no handle'}) — reusing it.")
            return data_idx

    for excel_row in range(2, max_row + 1):
        handle_val = cell(creds_values, excel_row, handle_col)
        status_val = cell(creds_values, excel_row, status_col)
        if not handle_val:
            continue
        if status_val.lower() == ASSIGN_STATUS_IN_USE.lower():
            continue

        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        data = [
            {"range": qrange(CREDS_TAB, f"{col_letter(repo_col)}{excel_row}"), "values": [[CURRENT_REPO]]},
            {"range": qrange(CREDS_TAB, f"{col_letter(status_col)}{excel_row}"), "values": [[ASSIGN_STATUS_IN_USE]]},
        ]
        if at_col is not None:
            data.append({"range": qrange(CREDS_TAB, f"{col_letter(at_col)}{excel_row}"), "values": [[now]]})
        sheets_batch_update_values(data)   # 1 write call for all 2-3 cells

        data_idx = excel_row - 1
        print(f"Claimed Credentials row {data_idx} ({handle_val}) for repo '{CURRENT_REPO}'.")
        return data_idx

    raise RuntimeError(
        f"No available account rows left in '{CREDS_TAB}' — every configured "
        f"row is already marked '{ASSIGN_STATUS_IN_USE}'. Add a new account "
        f"row, or clear ASSIGNED_REPO/ASSIGNED_STATUS on one you want to free up."
    )


# ═══════════════════════════════════════════════════════════════════════════
#  CROSS-REPO SOFT LOCK (business-level "who owns this account row right now")
# ═══════════════════════════════════════════════════════════════════════════

class AccountLockedElsewhereError(Exception):
    """Non-fatal — another repo currently owns this account row."""


def try_acquire_account_lock():
    global _account_config

    core = load_core_data(force=True)   # fresh read right before the check, to narrow the race window
    creds_values = core["creds"]
    header = hmap(creds_values)
    by_col = header.get("LOCKED_BY")
    at_col = header.get("LOCKED_AT")
    excel_row = ACCOUNT_ROW + 1

    if by_col is None or at_col is None:
        return True   # no lock columns configured -> treat as always acquired

    locked_by     = cell(creds_values, excel_row, by_col)
    locked_at_raw = cell(creds_values, excel_row, at_col)

    stale = True
    if locked_at_raw:
        try:
            locked_at = time.mktime(time.strptime(locked_at_raw, "%Y-%m-%dT%H:%M:%SZ"))
            stale = (time.time() - locked_at) > LOCK_TTL_MINUTES * 60
        except ValueError:
            stale = True

    if locked_by and locked_by != CURRENT_REPO and not stale:
        print(f"Row {ACCOUNT_ROW} is currently locked by '{locked_by}' "
              f"(last heartbeat {locked_at_raw} UTC, TTL {LOCK_TTL_MINUTES}m). Skipping this run.")
        return False

    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    sheets_batch_update_values([
        {"range": qrange(CREDS_TAB, f"{col_letter(by_col)}{excel_row}"), "values": [[CURRENT_REPO]]},
        {"range": qrange(CREDS_TAB, f"{col_letter(at_col)}{excel_row}"), "values": [[now]]},
    ])
    if _account_config:
        _account_config["locked_by"] = CURRENT_REPO
        _account_config["locked_at"] = now
    return True


def _write_account_status(status):
    global _account_config

    core = load_core_data(force=True)
    header = hmap(core["creds"])
    status_col = header.get("ACCOUNT_STATUS")
    if status_col is None:
        print("Note: no ACCOUNT_STATUS column in Credentials — add one to track per-row status.")
        return
    at_col = header.get("ACCOUNT_STATUS_AT")
    excel_row = ACCOUNT_ROW + 1

    data = [{"range": qrange(CREDS_TAB, f"{col_letter(status_col)}{excel_row}"), "values": [[status]]}]
    if at_col is not None:
        data.append({"range": qrange(CREDS_TAB, f"{col_letter(at_col)}{excel_row}"), "values": [[_now_str()]]})

    try:
        sheets_batch_update_values(data)
        if _account_config:
            _account_config["account_status"] = status
        print(f"Credentials ACCOUNT_STATUS set to '{status}' for row {ACCOUNT_ROW}.")
    except Exception as exc:
        print(f"Warning: could not update ACCOUNT_STATUS: {exc}")


# ═══════════════════════════════════════════════════════════════════════════
#  PROXIES (per-account assignment, persisted; live-checked every cycle)
# ═══════════════════════════════════════════════════════════════════════════

class NoProxyAvailableError(Exception):
    """No live, unused proxy could be found this cycle (and PROXY_REQUIRED
    is true), or the previously-assigned proxy died and nothing else was
    available to replace it with."""


def _http_proxy_url(ip, port):
    return f"http://{ip}:{port}"


def check_proxy_alive(ip, port, timeout=10):
    """A proxy counts as 'alive' if it can actually reach Bluesky's PDS
    through it (not just any TCP connect) — that's the only thing that
    matters for this workflow."""
    proxy_url = _http_proxy_url(ip, port)
    try:
        resp = requests.get(
            PROXY_CHECK_URL,
            proxies={"http": proxy_url, "https": proxy_url},
            timeout=timeout,
        )
        return resp.status_code < 500
    except Exception:
        return False


def _load_proxies_values():
    res = sheets_get(qrange(PROXIES_TAB, "A:ZZ"))
    return res.get("values", [])


def _proxy_cols(values):
    header = hmap(values)
    def ci(*names):
        for n in names:
            if n.upper() in header:
                return header[n.upper()]
        return None
    return {
        "ip":           ci("IP"),
        "port":         ci("PORT"),
        "status":       ci("STATUS"),
        "rt":           ci("RESPONSETIME(S)", "RESPONSE TIME(S)", "RESPONSETIME"),
        "assigned_to":  ci("ASSIGNED_TO"),
        "assigned_at":  ci("ASSIGNED_AT"),
        "last_checked": ci("LAST_CHECKED"),
        "last_ok":      ci("LAST_CHECK_OK"),
    }


def _write_proxy_row(excel_row, cols, updates):
    """updates: dict of {col-key -> value}; unspecified keys are left
    untouched. Always goes out as ONE batchUpdate call regardless of how
    many of the cells changed."""
    data = []
    for key, val in updates.items():
        col = cols.get(key)
        if col is None:
            continue
        data.append({"range": qrange(PROXIES_TAB, f"{col_letter(col)}{excel_row}"), "values": [[val]]})
    if data:
        sheets_batch_update_values(data)


def _find_proxy_row(ip, port):
    """Returns (excel_row, cols, values) for the Proxies row matching
    ip/port, or (None, None, None) if not found. Returning the already-
    loaded `values` array lets callers read any field (e.g. ASSIGNED_TO)
    without an extra Sheets round trip."""
    values = _load_proxies_values()
    cols = _proxy_cols(values)
    if cols["ip"] is None:
        return None, None, None
    for excel_row in range(2, len(values) + 1):
        if cell(values, excel_row, cols["ip"]) == ip and cell(values, excel_row, cols["port"]) == port:
            return excel_row, cols, values
    return None, None, None


def _proxy_owned_by(ip, port, handle):
    """True only if the Proxies tab currently shows THIS handle as the
    ASSIGNED_TO owner of ip:port. Used to catch the case where an account's
    Credentials row still has a PROXY_IP/PROXY_PORT on file that either
    never got recorded as "assigned" (stale data from before ASSIGNED_TO
    existed) or has since been claimed by a different account — in either
    case this account must NOT keep reusing it, since that would mean two
    accounts sharing one proxy."""
    excel_row, cols, values = _find_proxy_row(ip, port)
    if excel_row is None or cols["assigned_to"] is None:
        return False
    assigned_to = cell(values, excel_row, cols["assigned_to"])
    if not assigned_to:
        return False
    return assigned_to.strip().lower() == handle.strip().lower()


def touch_proxy_alive(ip, port):
    """Update LAST_CHECKED/LAST_CHECK_OK for an already-assigned proxy
    that just passed its liveness check, without touching its assignment."""
    excel_row, cols, _values = _find_proxy_row(ip, port)
    if excel_row is None:
        return
    _write_proxy_row(excel_row, cols, {"last_checked": _now_str(), "last_ok": "alive"})


def release_or_kill_proxy(ip, port, alive):
    """alive=False: mark the proxy 'dead' so it's never handed out again.
    alive=True: release it back to the free pool (e.g. its account got
    banned and no longer needs it) so another account can claim it."""
    excel_row, cols, _values = _find_proxy_row(ip, port)
    if excel_row is None:
        return
    now = _now_str()
    if alive:
        _write_proxy_row(excel_row, cols, {
            "status": "", "assigned_to": "", "assigned_at": "",
            "last_checked": now, "last_ok": "alive",
        })
    else:
        _write_proxy_row(excel_row, cols, {
            "status": "dead", "last_checked": now, "last_ok": "dead",
        })


def _claim_proxy_row(cols, excel_row, handle):
    """Optimistic claim: re-read the Status cell (and ASSIGNED_TO, as a
    fallback if there's no Status column) fresh right before writing, to
    narrow (not fully eliminate) the race between two account jobs
    claiming the same proxy row at once — same pattern as claim_url_row()
    for LinkPlan rows elsewhere in this file."""
    if cols["status"] is not None:
        rng = qrange(PROXIES_TAB, f"{col_letter(cols['status'])}{excel_row}")
        fresh = sheets_get(rng)
        rows = fresh.get("values", [])
        current = str(rows[0][0]).strip().lower() if rows and rows[0] else ""
        if current in ("assigned", "dead"):
            return False
    elif cols["assigned_to"] is not None:
        rng = qrange(PROXIES_TAB, f"{col_letter(cols['assigned_to'])}{excel_row}")
        fresh = sheets_get(rng)
        rows = fresh.get("values", [])
        current = str(rows[0][0]).strip() if rows and rows[0] else ""
        if current:
            return False

    now = _now_str()
    _write_proxy_row(excel_row, cols, {
        "status": "assigned", "assigned_to": handle, "assigned_at": now,
        "last_checked": now, "last_ok": "alive",
    })
    return True


def find_and_claim_free_proxy(handle, timeout):
    """Scan the Proxies tab for a free (not assigned, not dead) proxy,
    fastest ResponseTime(s) first, live-check each candidate, and claim
    the first one that's actually alive. Dead candidates encountered along
    the way are marked 'dead' so future cycles skip them immediately."""
    values = _load_proxies_values()
    if not values or len(values) < 2:
        return None
    cols = _proxy_cols(values)
    if cols["ip"] is None or cols["port"] is None:
        print(f"Warning: '{PROXIES_TAB}' needs at least IP and Port columns.")
        return None

    candidates = []
    for excel_row in range(2, len(values) + 1):
        ip = cell(values, excel_row, cols["ip"])
        port = cell(values, excel_row, cols["port"])
        if not ip or not port:
            continue
        status = cell(values, excel_row, cols["status"]).lower() if cols["status"] else ""
        last_ok = cell(values, excel_row, cols["last_ok"]).lower() if cols["last_ok"] else ""
        assigned_to = cell(values, excel_row, cols["assigned_to"]).strip() if cols["assigned_to"] else ""
        # Belt-and-suspenders: honor Status if present, but ALSO skip a row
        # if LAST_CHECK_OK says "dead" or ASSIGNED_TO is already filled in —
        # this way a missing/deleted Status column can't cause a dead or
        # already-claimed proxy to look free.
        if status in ("assigned", "dead"):
            continue
        if last_ok == "dead":
            continue
        if assigned_to:
            continue
        rt = 999.0
        if cols["rt"]:
            raw_rt = cell(values, excel_row, cols["rt"])
            try:
                rt = float(raw_rt) if raw_rt else 999.0
            except ValueError:
                rt = 999.0
        candidates.append((rt, excel_row, ip, port))

    candidates.sort(key=lambda t: t[0])

    for _, excel_row, ip, port in candidates:
        if check_proxy_alive(ip, port, timeout):
            if _claim_proxy_row(cols, excel_row, handle):
                print(f"Claimed proxy {ip}:{port} for {handle}.")
                return ip, port
            print(f"Lost claim race on proxy {ip}:{port} — trying next candidate.")
            continue
        _write_proxy_row(excel_row, cols, {"status": "dead", "last_checked": _now_str(), "last_ok": "dead"})
        print(f"Proxy {ip}:{port} failed liveness check — marked dead, trying next candidate.")

    return None


def _write_account_proxy(ip, port):
    global _account_config
    core = load_core_data(force=True)
    header = hmap(core["creds"])
    ip_col   = header.get("PROXY_IP")
    port_col = header.get("PROXY_PORT")
    at_col   = header.get("PROXY_ASSIGNED_AT")
    if ip_col is None or port_col is None:
        print(f"Warning: '{CREDS_TAB}' needs PROXY_IP and PROXY_PORT columns to "
              f"persist the proxy assignment across cycles.")
        return
    excel_row = ACCOUNT_ROW + 1
    data = [
        {"range": qrange(CREDS_TAB, f"{col_letter(ip_col)}{excel_row}"), "values": [[ip]]},
        {"range": qrange(CREDS_TAB, f"{col_letter(port_col)}{excel_row}"), "values": [[port]]},
    ]
    if at_col is not None:
        data.append({"range": qrange(CREDS_TAB, f"{col_letter(at_col)}{excel_row}"), "values": [[_now_str()]]})
    sheets_batch_update_values(data)
    if _account_config:
        _account_config["proxy_ip"] = ip
        _account_config["proxy_port"] = port


def _clear_account_proxy():
    global _account_config
    core = load_core_data(force=True)
    header = hmap(core["creds"])
    ip_col   = header.get("PROXY_IP")
    port_col = header.get("PROXY_PORT")
    if ip_col is None or port_col is None:
        return
    excel_row = ACCOUNT_ROW + 1
    sheets_batch_update_values([
        {"range": qrange(CREDS_TAB, f"{col_letter(ip_col)}{excel_row}"), "values": [[""]]},
        {"range": qrange(CREDS_TAB, f"{col_letter(port_col)}{excel_row}"), "values": [[""]]},
    ])
    if _account_config:
        _account_config["proxy_ip"] = ""
        _account_config["proxy_port"] = ""


def ensure_account_proxy(cfg):
    """Called once per cycle, right before login. Keeps the account on its
    previously-assigned proxy as long as it's alive; otherwise kills it and
    claims a fresh, not-yet-used one. Returns an 'http://ip:port' proxy URL,
    or None if proxies are disabled (USE_PROXY=false)."""
    if not cfg.get("use_proxy", True):
        return None

    handle  = cfg["handle"]
    timeout = cfg.get("proxy_check_timeout", 10)

    ip, port = cfg.get("proxy_ip", ""), cfg.get("proxy_port", "")
    if ip and port:
        if not _proxy_owned_by(ip, port, handle):
            # Either stale data (this account's Credentials row still
            # points at a proxy that was never actually recorded as
            # "assigned" to it) or the proxy has since been claimed by a
            # different account. Either way: this account must NOT keep
            # using it — that would mean two accounts sharing one proxy.
            print(f"Proxy {ip}:{port} on file for {handle} is not exclusively assigned to "
                  f"this account in '{PROXIES_TAB}' — clearing the stale record and "
                  f"claiming a fresh, unshared proxy instead.")
            _clear_account_proxy()
        elif check_proxy_alive(ip, port, timeout):
            touch_proxy_alive(ip, port)
            print(f"Reusing existing proxy {ip}:{port} for {handle} (still alive).")
            return _http_proxy_url(ip, port)
        else:
            print(f"Assigned proxy {ip}:{port} for {handle} is dead — releasing it and picking a new one.")
            release_or_kill_proxy(ip, port, alive=False)
            _clear_account_proxy()

    claimed = find_and_claim_free_proxy(handle, timeout)
    if claimed is None:
        if cfg.get("proxy_required", True):
            raise NoProxyAvailableError(
                f"No live, unused proxy available in '{PROXIES_TAB}' for {handle}."
            )
        print("No proxy available and PROXY_REQUIRED is false — continuing without a proxy.")
        return None

    ip, port = claimed
    _write_account_proxy(ip, port)
    return _http_proxy_url(ip, port)


class ProxyClientBuildError(Exception):
    """Raised when a proxy was assigned/required but could not actually be
    attached to the atproto Client. Callers must treat this as a hard stop
    for the cycle (no login, no post) — never as a reason to fall back to
    posting without a proxy."""


def build_bsky_client(proxy_url):
    """Create the atproto Client, routed through `proxy_url` if given.

    IMPORTANT: atproto's Client(base_url=None, *args, **kwargs) looks like
    it forwards kwargs to httpx, but it doesn't — ClientBase.__init__ only
    accepts (base_url, request). Passing Client(proxy=...) or
    Client(proxies=...) directly raises TypeError (silently swallowed by a
    naive try/except, which is how a previous version of this function
    ended up building an UNPROXIED client and posting through it anyway).

    The correct way is to build the underlying Request object yourself —
    Request(**kwargs) forwards straight into httpx.Client(**kwargs) — and
    hand that pre-built Request to Client(request=...).
    """
    if not proxy_url:
        return Client()

    last_exc = None
    for kwargs in (
        {"proxy": proxy_url},                                          # httpx >= 0.26
        {"proxies": {"http://": proxy_url, "https://": proxy_url}},     # older httpx
    ):
        try:
            req = AtprotoRequest(**kwargs)
            return Client(request=req)
        except TypeError as exc:
            last_exc = exc
            continue

    raise ProxyClientBuildError(
        f"Could not configure HTTP proxy {proxy_url!r} on the atproto Client "
        f"with the installed httpx/atproto version ({last_exc})."
    )


def _looks_like_proxy_error(exc):
    """Heuristic: does this exception look like it came from the proxy
    itself being unreachable/broken, rather than a real Bluesky API error
    (bad credentials, account takedown, etc.)?"""
    text = f"{type(exc).__name__}: {exc}"
    markers = (
        "ProxyError", "ConnectError", "ConnectTimeout", "ReadTimeout",
        "Failed to establish a new connection", "Connection refused",
        "Cannot connect to proxy", "Tunnel connection failed",
        "Max retries exceeded", "RemoteProtocolError", "ConnectionResetError",
    )
    return any(m in text for m in markers)


# ═══════════════════════════════════════════════════════════════════════════
#  TEXT HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def _posting_handle():
    h = _cfg()["handle"]
    return h if h.startswith("@") else f"@{h}"

def replace_mentions(text):
    return _MENTION_RE.sub(_posting_handle(), text) if text else text

def replace_urls(text):
    return _URL_RE.sub(_cfg()["link_url"], text) if text else text


# ═══════════════════════════════════════════════════════════════════════════
#  CONFIG SUMMARY
# ═══════════════════════════════════════════════════════════════════════════

def print_config_summary():
    cfg = _cfg()
    print("── Run config (live from Settings + Credentials in Google Sheets) ──")
    print(f"  Account row:              {cfg['row_num']}  ({_posting_handle()})")
    print(f"  Account status:           {cfg.get('account_status') or '(not set)'}")
    print(f"  Mega upload folder:       {cfg['mega_upload_folder'] or '(not set!)'}")
    print(f"  Mega processed folder:    {cfg['mega_processed_folder'] or '(not set!)'}")
    print(f"  Post link (in-caption):   {cfg['link_display_text']} -> {cfg['link_url']}")
    print(f"  Post-type mix:            image {cfg['image_ratio']:.0%} / "
          f"video {cfg['video_ratio']:.0%} / previewLink {cfg['link_ratio']:.0%}")
    print(f"  Caption enabled:          {cfg['caption_enabled']}")
    print(f"  Hashtags on image posts:  {cfg['hashtags_enabled_image']}")
    print(f"  Hashtags on video posts:  {cfg['hashtags_enabled_video']}")
    print(f"  Hashtags on previewLink:  {cfg['hashtags_enabled_link']}")
    print(f"  Link on image posts:      {cfg['link_enabled_image']}")
    print(f"  Link on video posts:      {cfg['link_enabled_video']}")
    print(f"  Link inclusion rate:      {cfg['link_percentage']:.0%} of eligible image/video posts")
    print(f"  Max image size:           {cfg['max_image_bytes']/(1024*1024):.2f} MB")
    print(f"  previewLink auto-caption: {cfg['auto_caption_enabled_link']} (title+description when sheet has none)")
    print(f"  previewLink fetch timeout:{cfg['preview_timeout']}s")
    print(f"  previewLink max thumb:    {cfg['max_thumb_bytes']/(1024*1024):.2f} MB")
    print(f"  Loop interval:            {cfg['loop_interval_seconds']}s ({cfg['loop_interval_seconds']/60:.1f} min)")
    print(f"  Generate report:          {cfg['enable_report']}")
    if cfg["enable_report"]:
        print(f"  Report frequency:         {cfg['report_times_per_day']}x per 24h")
        print(f"  Top posts combined:       {cfg['top_posts_count']}")
        print(f"  Scan last N posts:        {cfg['top_posts_within']}")
    print(f"  Post-plan sheet (media):  {cfg['post_plan_sheet_name']}")
    print(f"  LinkPlan sheet:           {cfg['link_plan_sheet_name']}")
    print(f"  Config source:            Google Sheet ({GOOGLE_SHEET_ID})")
    if cfg.get("has_lock_columns"):
        print(f"  Cross-repo lock:          enabled (owner={cfg.get('locked_by') or '—'}, "
              f"last heartbeat={cfg.get('locked_at') or '—'})")
    else:
        print("  Cross-repo lock:          disabled (add LOCKED_BY / LOCKED_AT columns to enable)")
    print(f"  Proxy usage:              {'enabled' if cfg.get('use_proxy', True) else 'disabled'}"
          f"{' (required)' if cfg.get('use_proxy', True) and cfg.get('proxy_required', True) else ''}")
    if cfg.get("use_proxy", True):
        proxy_display = f"{cfg.get('proxy_ip') or '—'}:{cfg.get('proxy_port') or '—'}"
        print(f"  Current proxy:            {proxy_display}")
        print(f"  Proxy check timeout:      {cfg.get('proxy_check_timeout', 10)}s")
    print(f"  Sheets retry budget:      {SHEETS_RETRY_BUDGET_SECONDS}s per call "
          f"(max backoff {SHEETS_MAX_BACKOFF_SECONDS}s)")
    print("─────────────────────────────────────────────────")


# ═══════════════════════════════════════════════════════════════════════════
#  REPORT TAB
# ═══════════════════════════════════════════════════════════════════════════

def _now_str():
    return time.strftime("%Y-%m-%d %H:%M", time.gmtime()) + " UTC"

def _parse_report_ts(s):
    try:
        return time.mktime(time.strptime(s.replace(" UTC", ""), "%Y-%m-%d %H:%M"))
    except Exception:
        return None


def _last_report_for_handle(handle):
    """Reads from the already-cached core data — no extra API call."""
    report_values = load_core_data()["report"]
    for r in range(len(report_values), 1, -1):
        h = cell(report_values, r, 2)
        if h == handle:
            ts = cell(report_values, r, 1)
            followers = None
            f_raw = cell(report_values, r, 3)
            if f_raw:
                try:
                    followers = int(float(f_raw))
                except ValueError:
                    followers = None
            return ts, followers
    return None, None


def _report_due(handle, times_per_day):
    times_per_day = max(1, times_per_day)
    last_ts, _ = _last_report_for_handle(handle)
    if last_ts is None:
        return True
    last_epoch = _parse_report_ts(last_ts)
    if last_epoch is None:
        return True
    interval_seconds = 86400.0 / times_per_day
    return (time.time() - last_epoch) >= interval_seconds


def _append_report(rows):
    sheets_append(qrange(REPORT_TAB, "A:G"), rows)
    load_core_data(force=True)   # refresh cache so subsequent checks this cycle see the new row


def _top_post_summary(client, handle, top_n, within):
    try:
        response = client.get_author_feed(actor=handle, limit=within)
    except Exception as exc:
        return f"(couldn't fetch posts: {exc})", 0

    posts = []
    for item in response.feed:
        if getattr(item, "reason", None) is not None:
            continue
        p       = item.post
        likes   = getattr(p, "like_count",   0) or 0
        reposts = getattr(p, "repost_count", 0) or 0
        replies = getattr(p, "reply_count",  0) or 0
        quotes  = getattr(p, "quote_count",  0) or 0
        try:
            text = p.record.text or ""
        except AttributeError:
            text = ""
        posts.append({
            "text": text, "likes": likes, "reposts": reposts,
            "replies": replies, "quotes": quotes,
            "engagement": likes + reposts + replies + quotes,
        })

    if not posts:
        return "(no posts found)", 0

    ranked = sorted(posts, key=lambda p: p["engagement"], reverse=True)[:max(1, top_n)]

    if len(ranked) == 1:
        p = ranked[0]
        preview = p["text"][:100] + ("…" if len(p["text"]) > 100 else "")
        return preview, p["engagement"]

    parts = []
    for i, p in enumerate(ranked, start=1):
        preview = p["text"][:60] + ("…" if len(p["text"]) > 60 else "")
        parts.append(f"{i}) {preview} ({p['engagement']})")
    return " | ".join(parts), ranked[0]["engagement"]


def generate_report(client, handle, cfg):
    """All Bluesky API calls happen with no Sheets calls in between; only
    the final _append_report touches the sheet, and only once."""
    if not _report_due(handle, cfg["report_times_per_day"]):
        print(f"Report for {handle} not due yet (limit: {cfg['report_times_per_day']}x/24h).")
        return
    try:
        profile = client.get_profile(actor=handle)
        total   = profile.followers_count or 0

        _, prev_followers = _last_report_for_handle(handle)
        prev   = prev_followers if prev_followers is not None else total
        gained = total - prev

        top_preview, top_engagement = _top_post_summary(
            client, handle, cfg["top_posts_count"], cfg["top_posts_within"]
        )

        row = [_now_str(), handle, total, gained, top_preview, top_engagement, "OK"]
        _append_report([row])
        print(f"Report logged for {handle}: {total} followers ({gained:+d} since last), "
              f"top post engagement={top_engagement}.")
    except Exception as exc:
        print(f"Warning: report generation failed: {exc}")


def run_report(client, handle, cfg):
    try:
        generate_report(client, handle, cfg)
    except Exception as exc:
        print(f"Warning: report generation failed: {exc}")


# ═══════════════════════════════════════════════════════════════════════════
#  ERROR TYPES
# ═══════════════════════════════════════════════════════════════════════════

class AccountTakenDownError(Exception):
    """Fatal — log to sheet, disable workflow."""

class NoMediaFoundError(Exception):
    """Clean exit (code 0) — nothing postable this cycle; keep schedule running."""

class NoPreviewError(Exception):
    """Link-preview metadata could not be fetched via cardyb or manual scrape."""


def log_account_problem(handle, status):
    try:
        _append_report([[_now_str(), handle, "", "", "", "", status]])
        print(f"Logged '{status}' for {handle}.")
    except Exception as exc:
        print(f"Warning: could not log account status: {exc}")


# ═══════════════════════════════════════════════════════════════════════════
#  ACCOUNT DISPLAY
# ═══════════════════════════════════════════════════════════════════════════

def print_target_account(handle):
    display = handle if handle.startswith("@") else f"@{handle}"
    print(f"Target Bluesky account: {display}")
    print(f"  (app password: {'loaded' if _cfg().get('app_pw') else 'MISSING!'})")


# ═══════════════════════════════════════════════════════════════════════════
#  HASHTAGS
# ═══════════════════════════════════════════════════════════════════════════

def get_account_hashtags():
    raw = _cfg().get("hashtags_raw", "")
    if raw:
        tags = [w.lstrip("#") for w in raw.split() if w.startswith("#")]
        if tags:
            return tags
    try:
        with open("hashtags.txt", "r", encoding="utf-8") as f:
            sets = [l.strip() for l in f if l.strip()]
        return [w.lstrip("#") for w in random.choice(sets).split() if w.startswith("#")] if sets else []
    except FileNotFoundError:
        return []


# ═══════════════════════════════════════════════════════════════════════════
#  AUTO ADULT / LIVE-CAM CAPTIONS (unique every post, with emojis)
# ═══════════════════════════════════════════════════════════════════════════
# Post layout (every media post):
#
#   <main body caption with emoji>
#
#   💖 <action words> 😘 <rich "Live Models"  OR  plain actual URL>
#
#   #hashtag1 #hashtag2 ...
#
# Link is ONLY one style per post — never rich + plain together.
# Main caption is always unique adult/body-focused text.

_CAPTION_EMOJIS = [
    "🔥", "😈", "💋", "🥵", "💦", "🍑", "🍆", "❤️‍🔥", "😉", "👅",
    "💃", "🎥", "🔴", "💕", "✨", "👀", "🥰", "😏", "💖", "🌙", "😘",
]

# Main body captions (unique adult / body / cam style)
_BODY_CAPTION_TEMPLATES = [
    "I'm owning my body without a single thread in the way. These nipples are hard and ready for attention. {e}",
    "No clothes, no rules — just me, bare and waiting. Come closer. {e}",
    "Soft skin, hard nipples, and a very dirty mind tonight. {e}",
    "Fully naked and online. My body is yours to look at right now. {e}",
    "These curves aren't hiding anymore. Want a closer look? {e}",
    "Bare, wet, and ready for 1-on-1 attention. {e}",
    "I took everything off just for you. Don't be shy. {e}",
    "Nipples out, legs open, live and unfiltered. {e}",
    "No bra, no panties — just me and this cam. {e}",
    "Feeling extra naughty. My body is on full display. {e}",
    "Every inch of me is online right now. Come watch. {e}",
    "Hard nipples and a soft bed. Guess where I am. {e}",
    "Stripped down and live. Private shows start when you join. {e}",
    "I'm not wearing a single thing. Your move. {e}",
    "Body on show, mind in the gutter. Join me live. {e}",
    "These tits are out and ready for attention. {e}",
    "Ass up, cam on, waiting for someone fun. {e}",
    "Naked and bored until you show up. {e}",
    "Skin only. No filters. Real body, real time. {e}",
    "I undressed just so you could stare. Enjoy. {e}",
    "Live, nude, and in the mood. 1-on-1 is open. {e}",
    "My nipples got hard the second the cam went on. {e}",
    "Nothing left to the imagination — come see. {e}",
    "Bare skin and dirty thoughts. Private chat is open. {e}",
    "Fully exposed and loving every second of it. {e}",
]

# Action line that sits right before the link: "💖 action words 😘"
_ACTION_LINE_TEMPLATES = [
    "Join my private show",
    "Watch me live now",
    "Start 1-on-1 chat",
    "Come play with me",
    "Enter private cam",
    "Unlock full access",
    "See me completely nude",
    "Join exclusive live",
    "Open private room",
    "Chat with me live",
    "Get the full show",
    "Watch every inch",
    "Private nude live",
    "Come inside now",
    "Live cam is open",
    "Tap for more of me",
    "See what I'm hiding",
    "Join my OnlyFans",
    "Full nude content here",
    "Don't miss this show",
]

_RICH_LINK_DISPLAY_OPTIONS = [
    "Live Models",
    "Live Cam",
    "1-on-1 Live",
    "Private Show",
    "Live Chat",
    "Join Live",
    "Watch Live",
    "Private Cam",
    "Adult Live",
    "Live Now",
    "OnlyFans",
    "Full Video",
]


def _pick_emoji():
    return random.choice(_CAPTION_EMOJIS)


def generate_body_caption():
    """Unique main body caption (adult / body focused) with trailing emoji."""
    tmpl = random.choice(_BODY_CAPTION_TEMPLATES)
    return tmpl.format(e=_pick_emoji())


def generate_action_line():
    """Action words sandwiched in emojis, e.g. '💖 Join my private show 😘'."""
    words = random.choice(_ACTION_LINE_TEMPLATES)
    return f"💖 {words} 😘"


def choose_caption_and_link_style(sheet_caption, add_link):
    """Build the post pieces matching the desired layout.

    Returns:
      body_caption: str   — main adult body text
      action_line:  str   — "💖 action words 😘" (empty if no link)
      link_mode: None | "rich" | "plain"
      rich_display: str or None
    """
    cfg = _cfg()
    body_caption = generate_body_caption()

    if not add_link:
        return body_caption, "", None, None

    action_line = generate_action_line()
    # Exactly one link style: rich display text OR plain actual URL — never both.
    link_mode = random.choice(["rich", "plain"])
    rich_display = None
    if link_mode == "rich":
        account_display = (cfg.get("link_display_text") or "").strip()
        bare_url = (cfg.get("link_url") or "").replace("https://", "").replace("http://", "").lower()
        if account_display and account_display.lower() not in (bare_url, ""):
            rich_display = account_display if random.random() < 0.55 else random.choice(_RICH_LINK_DISPLAY_OPTIONS)
        else:
            rich_display = random.choice(_RICH_LINK_DISPLAY_OPTIONS)

    return body_caption, action_line, link_mode, rich_display


# ═══════════════════════════════════════════════════════════════════════════
#  LINK-IN-POST DECISION
# ═══════════════════════════════════════════════════════════════════════════

def should_add_link(kind):
    cfg     = _cfg()
    enabled = cfg["link_enabled_image"] if kind == "image" else cfg["link_enabled_video"]
    if not enabled:
        return False
    return random.random() < cfg["link_percentage"]


# ═══════════════════════════════════════════════════════════════════════════
#  POST-TYPE SELECTION
# ═══════════════════════════════════════════════════════════════════════════

def choose_media_kind():
    cfg = _cfg()
    return random.choices(
        ["image", "video", "previewLink"],
        weights=[cfg["image_ratio"], cfg["video_ratio"], cfg["link_ratio"]],
        k=1,
    )[0]


# ═══════════════════════════════════════════════════════════════════════════
#  POST-PLAN SHEET (File Name + Caption + Status)
# ═══════════════════════════════════════════════════════════════════════════

_post_plan_cache          = None
_post_plan_status_col_idx = None   # 1-based col index


def get_post_plan_tab_name():
    return _cfg()["post_plan_sheet_name"]


def load_post_plan(force_refresh=False):
    global _post_plan_cache, _post_plan_status_col_idx
    if _post_plan_cache is not None and not force_refresh:
        return _post_plan_cache

    tab_name = get_post_plan_tab_name()
    link_tab = get_link_plan_tab_name()
    plans = load_plan_data(tab_name, link_tab, force=force_refresh)
    values = plans["by_tab"].get(tab_name, [])

    if not values:
        print(f"Warning: post-plan sheet '{tab_name}' not found (or empty) in the spreadsheet — "
              f"add a tab with that name (File Name / Caption / Status columns).")
        _post_plan_cache = {}
        return _post_plan_cache

    header = hmap(values)

    def ci(*names):
        for n in names:
            if n.upper() in header:
                return header[n.upper()]
        return None

    file_idx    = ci("file name", "filename", "file")
    caption_idx = ci("caption", "captions")
    status_idx  = ci("status")
    _post_plan_status_col_idx = status_idx

    if file_idx is None or caption_idx is None:
        print(f"Warning: post-plan needs 'File Name' and 'Caption' columns. Found: {list(header)}")
        _post_plan_cache = {}
        return _post_plan_cache
    if status_idx is None:
        print("Warning: no 'Status' column — posted files won't be tracked.")

    plan_exact = {}
    plan_lower = {}
    already    = 0
    for excel_row in range(2, len(values) + 1):
        fname   = cell(values, excel_row, file_idx)
        caption = cell(values, excel_row, caption_idx)
        status  = cell(values, excel_row, status_idx) if status_idx else ""
        if not fname:
            continue
        entry = {"caption": caption, "row": excel_row, "status": status}
        plan_exact[fname]         = entry
        plan_lower[fname.lower()] = entry
        if status.lower() == POSTED_STATUS_VALUE:
            already += 1

    print(f"Loaded {len(plan_exact)} post-plan rows ({already} already posted).")
    _post_plan_cache = {"exact": plan_exact, "lower": plan_lower}
    return _post_plan_cache


def find_plan_entry(plan, filename):
    exact = plan.get("exact", {})
    lower = plan.get("lower", {})
    return (
        exact.get(filename)
        or lower.get(filename.lower())
        or lower.get(os.path.splitext(filename.lower())[0])
    )


def mark_posted(filename, row_number, retries=3):
    global _post_plan_cache
    if _post_plan_status_col_idx is None:
        print(f"Warning: no 'Status' column — cannot mark '{filename}' as posted.")
        return

    tab_name = get_post_plan_tab_name()
    rng = qrange(tab_name, f"{col_letter(_post_plan_status_col_idx)}{row_number}")

    for attempt in range(1, retries + 1):
        try:
            sheets_update(rng, [[POSTED_STATUS_VALUE]])
            if _post_plan_cache:
                for d in (_post_plan_cache.get("exact", {}), _post_plan_cache.get("lower", {})):
                    for entry in d.values():
                        if entry["row"] == row_number:
                            entry["status"] = POSTED_STATUS_VALUE
            print(f"Marked '{filename}' row {row_number} as posted.")
            return
        except Exception as exc:
            if attempt < retries:
                wait = 2 ** attempt
                print(f"  mark_posted attempt {attempt}/{retries} failed ({exc}); retrying in {wait}s…")
                time.sleep(wait)
            else:
                print(f"ERROR: could not mark '{filename}' as posted after {retries} attempts: {exc}")
                print("  Post was successful — file will be moved. Row may need manual update.")


# ═══════════════════════════════════════════════════════════════════════════
#  LINKPLAN SHEET (URL + Caption + Status)
# ═══════════════════════════════════════════════════════════════════════════

def get_link_plan_tab_name():
    return _cfg()["link_plan_sheet_name"]


def load_link_plan(force_refresh=False):
    tab_name  = get_link_plan_tab_name()
    post_tab  = get_post_plan_tab_name()
    plans = load_plan_data(post_tab, tab_name, force=force_refresh)
    values = plans["by_tab"].get(tab_name, [])
    if not values:
        return []

    header = hmap(values)

    def ci(*names):
        for n in names:
            if n.upper() in header:
                return header[n.upper()]
        return None

    url_idx    = ci("url")
    cap_idx    = ci("caption")
    status_idx = ci("status")
    if url_idx is None:
        raise RuntimeError(f"'{tab_name}' needs a 'URL' column.")

    out = []
    for excel_row in range(2, len(values) + 1):
        url = cell(values, excel_row, url_idx)
        if not url:
            continue
        caption = cell(values, excel_row, cap_idx) if cap_idx else ""
        status  = cell(values, excel_row, status_idx) if status_idx else ""
        out.append({
            "url": url, "caption": caption, "status": status,
            "row": excel_row, "status_col": status_idx,
        })
    return out


def pick_next_url():
    plan = load_link_plan()
    for entry in plan:
        s = entry["status"].lower()
        if s == POSTED_STATUS_VALUE or s.startswith(CLAIM_PREFIX.lower()):
            continue
        return entry
    return None


def claim_url_row(entry):
    """Read-fresh-then-write on just this one cell to narrow (not fully
    eliminate) the race between two runners claiming the same LinkPlan row.
    This mirrors how LinkPlan claiming already worked before the Mega
    rewrite — it's an optimistic claim, not a hard lock."""
    if entry["status_col"] is None:
        return True
    tab_name  = get_link_plan_tab_name()
    rng       = qrange(tab_name, f"{col_letter(entry['status_col'])}{entry['row']}")
    claim_val = f"{CLAIM_PREFIX}{RUN_TAG}"

    fresh = sheets_get(rng)
    current_rows = fresh.get("values", [])
    current = str(current_rows[0][0]).strip() if current_rows and current_rows[0] else ""
    if current.lower() == POSTED_STATUS_VALUE or current.lower().startswith(CLAIM_PREFIX.lower()):
        return False   # someone else got there first

    sheets_update(rng, [[claim_val]])
    return True


def mark_url_posted(entry):
    if entry["status_col"] is None:
        return
    tab_name = get_link_plan_tab_name()
    rng = qrange(tab_name, f"{col_letter(entry['status_col'])}{entry['row']}")
    sheets_update(rng, [[POSTED_STATUS_VALUE]])


def release_url_claim(entry):
    if entry["status_col"] is None:
        return
    tab_name = get_link_plan_tab_name()
    rng = qrange(tab_name, f"{col_letter(entry['status_col'])}{entry['row']}")
    try:
        sheets_update(rng, [[""]])
    except Exception as exc:
        print(f"Warning: could not release claim on LinkPlan row {entry['row']}: {exc}")


# ═══════════════════════════════════════════════════════════════════════════
#  LINK PREVIEW
# ═══════════════════════════════════════════════════════════════════════════

def fetch_link_metadata(url, timeout=20):
    last_exc = None
    for attempt in range(1, LINK_PREVIEW_MAX_RETRIES + 1):
        try:
            resp = requests.get(
                CARDYB_EXTRACT_URL,
                params={"url": url},
                headers=REQUEST_HEADERS,
                timeout=timeout,
            )
            resp.raise_for_status()
            data = resp.json()
            return {
                "title": (data.get("title") or url)[:300],
                "description": (data.get("description") or "")[:1000],
                "image": data.get("image") or None,
                "final_url": url,
            }
        except (requests.exceptions.RequestException, ValueError) as exc:
            last_exc = exc
            print(f"Attempt {attempt}/{LINK_PREVIEW_MAX_RETRIES} to fetch via cardyb failed: {exc}")
            if attempt < LINK_PREVIEW_MAX_RETRIES:
                delay = LINK_PREVIEW_RETRY_DELAY * (2 ** (attempt - 1))
                print(f"Retrying in {delay}s…")
                time.sleep(delay)

    print(f"cardyb extraction failed after {LINK_PREVIEW_MAX_RETRIES} attempts ({last_exc}); "
          f"falling back to manual scrape.")
    return _fetch_link_metadata_manual(url, timeout)


def _fetch_link_metadata_manual(url, timeout=20):
    last_exc = None
    for attempt in range(1, LINK_PREVIEW_MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=REQUEST_HEADERS, timeout=timeout, allow_redirects=True)
            resp.raise_for_status()
            return _parse_link_metadata(resp)
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            print(f"Attempt {attempt}/{LINK_PREVIEW_MAX_RETRIES} to manually fetch {url} failed: {exc}")
            if attempt < LINK_PREVIEW_MAX_RETRIES:
                delay = LINK_PREVIEW_RETRY_DELAY * (2 ** (attempt - 1))
                print(f"Retrying in {delay}s…")
                time.sleep(delay)

    raise NoPreviewError(
        f"Failed to fetch {url} after {LINK_PREVIEW_MAX_RETRIES} attempts (cardyb + manual)"
    ) from last_exc


def _parse_link_metadata(resp):
    soup = BeautifulSoup(resp.text, "html.parser")
    final_url = resp.url

    def meta(*props):
        for prop in props:
            tag = soup.find("meta", property=prop) or soup.find("meta", attrs={"name": prop})
            if tag and tag.get("content"):
                return tag["content"].strip()
        return None

    title = meta("og:title", "twitter:title") or (
        soup.title.string.strip() if soup.title and soup.title.string else resp.url
    )
    description = meta("og:description", "twitter:description", "description") or ""
    raw_image = meta("og:image", "og:image:url", "twitter:image")
    image = urljoin(final_url, raw_image) if raw_image else None

    return {"title": title[:300], "description": description[:1000], "image": image, "final_url": final_url}


def upload_link_thumbnail(client, image_url, referer, max_bytes, timeout=20):
    if not image_url:
        print("No preview image found — posting without a thumbnail.")
        return None

    headers = {**REQUEST_HEADERS, "Referer": referer}
    last_exc = None
    for attempt in range(1, LINK_PREVIEW_MAX_RETRIES + 1):
        try:
            img_resp = requests.get(image_url, headers=headers, timeout=timeout)
            img_resp.raise_for_status()
            content_type = img_resp.headers.get("Content-Type", "")
            if "image" not in content_type:
                print(f"Warning: fetched image URL did not return an image (Content-Type: {content_type!r})")
                return None

            data = img_resp.content
            if len(data) > max_bytes:
                data = _compress_link_thumb(data, max_bytes)
                if data is None:
                    return None

            upload = client.upload_blob(data)
            return upload.blob
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            print(f"Attempt {attempt}/{LINK_PREVIEW_MAX_RETRIES} to fetch/upload thumbnail failed: {exc}")
            if attempt < LINK_PREVIEW_MAX_RETRIES:
                delay = LINK_PREVIEW_RETRY_DELAY * (2 ** (attempt - 1))
                print(f"Retrying in {delay}s…")
                time.sleep(delay)

    print(f"Warning: thumbnail could not be fetched/uploaded after all retries ({last_exc}); posting without one.")
    return None


def _compress_link_thumb(data, max_bytes):
    try:
        from PIL import Image
        img = Image.open(io.BytesIO(data))
        if img.mode in ("RGBA", "P", "LA"):
            img = img.convert("RGB")
        for q in range(85, 20, -10):
            buf = io.BytesIO()
            img.save(buf, format="JPEG", quality=q, optimize=True)
            if buf.tell() <= max_bytes:
                return buf.getvalue()
        return buf.getvalue()
    except Exception as exc:
        print(f"Warning: could not compress thumbnail: {exc}")
        return None


MAX_POST_GRAPHEMES = 300

def build_link_caption_text(caption, tags, fallback_url=None):
    text = _URL_RE.sub("", caption or "").strip(" \t\r")
    if fallback_url:
        text = f"{text}\n{fallback_url}".strip(" \t\r") if text else fallback_url

    tb = TextBuilder()
    if text:
        tb.text(text)
    if tags:
        if text:
            tb.text("\n\n")
        for i, tag in enumerate(tags):
            tb.tag(f"#{tag}", tag)
            if i < len(tags) - 1:
                tb.text(" ")

    plain = tb.build_text()
    if len(plain) > MAX_POST_GRAPHEMES:
        hashtag_block = ("\n\n" + " ".join(f"#{t}" for t in tags)) if tags else ""
        budget = MAX_POST_GRAPHEMES - len(hashtag_block)
        trimmed = (text[:max(0, budget - 1)].rstrip() + "…") if budget > 0 else ""
        tb = TextBuilder()
        if trimmed:
            tb.text(trimmed)
        if tags:
            if trimmed:
                tb.text("\n\n")
            for i, tag in enumerate(tags):
                tb.tag(f"#{tag}", tag)
                if i < len(tags) - 1:
                    tb.text(" ")
    return tb


def build_external_embed(client, preview, max_thumb_bytes, timeout):
    thumb_blob = upload_link_thumbnail(
        client, preview["image"], referer=preview["final_url"],
        max_bytes=max_thumb_bytes, timeout=timeout,
    )
    return models.AppBskyEmbedExternal.Main(
        external=models.AppBskyEmbedExternal.External(
            uri=preview["final_url"],
            title=preview["title"],
            description=preview["description"],
            thumb=thumb_blob,
        )
    )


def compose_fallback_caption(preview):
    if not preview:
        return ""
    title = (preview.get("title") or "").strip()
    description = (preview.get("description") or "").strip()
    parts = [p for p in (title, description) if p]
    if not parts:
        return ""
    return "\n" + "\n".join(parts)


def post_link_card(client, url, caption, tags, timeout, max_thumb_bytes, auto_caption_enabled=True):
    print(f"[previewLink] Fetching preview for: {url}")
    preview = None
    embed = None
    try:
        preview = fetch_link_metadata(url, timeout)
        print(f"  title: {preview['title']!r}")
        embed = build_external_embed(client, preview, max_thumb_bytes, timeout)
    except Exception as exc:
        print(f"Warning: preview fetch failed ({exc}); posting as plain link instead.")

    used_auto_caption = False
    effective_caption = (caption or "").strip()
    if effective_caption:
        effective_caption = _URL_RE.sub("", effective_caption).strip()
    if not effective_caption and auto_caption_enabled:
        # Prefer unique adult/live-cam auto caption; fall back to preview title+desc only if disabled.
        effective_caption = generate_body_caption()
        used_auto_caption = True
        print(f"No Caption in sheet — using auto adult caption: {effective_caption!r}")
    elif not effective_caption and not auto_caption_enabled:
        print("No Caption in sheet and previewLink auto-caption is off — posting without a caption.")

    # For previewLink posts the embed already carries the destination URL, so we
    # do not also put a rich/plain link in the text (avoids double links).
    tb = build_link_caption_text(effective_caption, tags, fallback_url=(url if preview is None else None))
    client.send_post(text=tb, embed=embed)

    posted_url = preview["final_url"] if preview else url
    caption_source = "auto (adult/live-cam)" if used_auto_caption else ("sheet" if caption else "no")
    print(f"✓ Posted {'link card' if embed else 'plain link'} for {posted_url} "
          f"(caption={caption_source}, tags={len(tags)})")


# ═══════════════════════════════════════════════════════════════════════════
#  MEGA.NZ HELPERS (via rclone) — image/video media, unchanged
# ═══════════════════════════════════════════════════════════════════════════

_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".tiff", ".avif", ".heic"}
_VIDEO_EXTS = {".mp4", ".mov", ".avi", ".mkv", ".webm", ".m4v", ".flv", ".wmv", ".3gp", ".ts"}


def _kind_from_filename(filename):
    ext = os.path.splitext(filename.lower())[1]
    if ext in _IMAGE_EXTS:
        return "image"
    if ext in _VIDEO_EXTS:
        return "video"
    return None


def fetch_media_matching_plan(preferred_kind, plan):
    cfg           = _cfg()
    upload_folder = cfg["mega_upload_folder"]
    if not upload_folder:
        raise RuntimeError("MEGA_UPLOAD_FOLDER is empty in credentials sheet.")

    remote_folder = f"{RCLONE_REMOTE_NAME}:{upload_folder}"
    files = rclone_list_files(remote_folder)
    candidates = [f for f in files if not f.startswith(CLAIM_PREFIX)
                  and _kind_from_filename(f) == preferred_kind]

    counters = {"claim": 0, "plan": 0, "posted": 0}

    for name in candidates:
        entry = find_plan_entry(plan, name)
        if entry is None:
            counters["plan"] += 1
            continue
        if entry["status"].lower() == POSTED_STATUS_VALUE:
            counters["posted"] += 1
            continue

        print(f"Found {preferred_kind}: '{name}'")
        claimed_name = rclone_claim(remote_folder, name)
        if claimed_name is None:
            counters["claim"] += 1
            continue
        print(f"Claimed as '{claimed_name}'.")

        local_path = f"/tmp/{name}"
        if not rclone_download(remote_folder, claimed_name, local_path):
            print(f"Warning: download failed for claimed file '{claimed_name}' — releasing claim.")
            rclone_move(f"{remote_folder}/{claimed_name}", f"{remote_folder}/{name}")
            continue

        file_info = {"original_name": name, "claimed_name": claimed_name}
        return file_info, local_path, preferred_kind, entry["caption"], entry["row"]

    print(f"No {preferred_kind} match: "
          f"{counters['plan']} not in plan, {counters['posted']} already posted, "
          f"{counters['claim']} claimed by another run.")
    return None, None, None, None, None


def compress_image_under_limit(local_path):
    from PIL import Image
    max_bytes = _cfg()["max_image_bytes"]
    orig = os.path.getsize(local_path)
    if orig <= max_bytes:
        print(f"Image {orig/1024:.0f} KB — no compression needed.")
        return local_path
    img = Image.open(local_path)
    if img.mode in ("RGBA", "P", "LA"):
        img = img.convert("RGB")
    for q in range(90, 20, -10):
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=q, optimize=True)
        if buf.tell() <= max_bytes:
            with open(local_path, "wb") as f:
                f.write(buf.getvalue())
            print(f"Compressed {orig/1024:.0f} KB → {buf.tell()/1024:.0f} KB (q={q}).")
            return local_path
    w, h = img.size
    scale = 0.9
    while scale > 0.3:
        r = img.resize((max(1, int(w * scale)), max(1, int(h * scale))), Image.LANCZOS)
        buf = io.BytesIO()
        r.save(buf, format="JPEG", quality=70, optimize=True)
        if buf.tell() <= max_bytes:
            with open(local_path, "wb") as f:
                f.write(buf.getvalue())
            print(f"Resized+compressed → {buf.tell()/1024:.0f} KB.")
            return local_path
        scale -= 0.1
    with open(local_path, "wb") as f:
        f.write(buf.getvalue())
    print(f"Warning: best-effort compression = {buf.tell()/1024:.0f} KB.")
    return local_path


def move_file_to_processed(claimed_name, original_name):
    cfg = _cfg()
    remote_upload    = f"{RCLONE_REMOTE_NAME}:{cfg['mega_upload_folder']}"
    remote_processed = f"{RCLONE_REMOTE_NAME}:{cfg['mega_processed_folder']}"
    ok = rclone_move(f"{remote_upload}/{claimed_name}", f"{remote_processed}/{original_name}")
    print("Moved to processed folder on Mega." if ok else
          "Warning: failed to move file to processed folder on Mega — check manually.")
    return ok


def release_claim(claimed_name, original_name):
    cfg = _cfg()
    remote_upload = f"{RCLONE_REMOTE_NAME}:{cfg['mega_upload_folder']}"
    ok = rclone_move(f"{remote_upload}/{claimed_name}", f"{remote_upload}/{original_name}")
    print(f"Released claim on '{original_name}'." if ok else
          f"Warning: could not release claim for '{original_name}'.")
    return ok


# ═══════════════════════════════════════════════════════════════════════════
#  IMAGE/VIDEO POST BUILDING
# ═══════════════════════════════════════════════════════════════════════════

def build_post_from_caption(body_caption, action_line, tags, link_mode=None, rich_display=None):
    """Build rich-text post in the exact layout:

        <body caption>

        💖 action words 😘 <rich link OR plain URL>

        #hashtags...
    """
    cfg = _cfg()
    text = replace_mentions(body_caption) if body_caption else ""
    text = _URL_RE.sub("", text).strip() if text else ""

    def _assemble(caption_text):
        tb = TextBuilder()
        if caption_text:
            tb.text(caption_text)

        # Action line + single CLICKABLE link (never plain non-clickable text).
        # Both modes use app.bsky.richtext.facet#link via TextBuilder.link():
        #   "rich"  → custom anchor text (e.g. "Live Models") → URL
        #   "plain" → URL text itself is the clickable link     → URL
        if link_mode in ("rich", "plain"):
            tb.text("\n\n")
            if action_line:
                tb.text(action_line + " ")
            url = cfg["link_url"]
            if link_mode == "rich":
                display = (rich_display or cfg.get("link_display_text") or "Live Models").strip()
                tb.link(display, url)
            else:
                # URL string is both the visible text and the destination — real facet link
                tb.link(url, url)

        if tags:
            tb.text("\n\n")
            for i, tag in enumerate(tags):
                tb.tag(f"#{tag}", tag)
                if i < len(tags) - 1:
                    tb.text(" ")
        return tb

    tb = _assemble(text)
    plain = tb.build_text()

    if len(plain) > MAX_POST_GRAPHEMES:
        lo, hi, best_text = 0, len(text), ""
        while lo <= hi:
            mid = (lo + hi) // 2
            trial = text[:mid].rstrip()
            if mid < len(text):
                trial += "…"
            if len(_assemble(trial).build_text()) <= MAX_POST_GRAPHEMES:
                best_text = trial
                lo = mid + 1
            else:
                hi = mid - 1
        print(f"Caption too long for post limit ({len(plain)} > {MAX_POST_GRAPHEMES}); "
              f"trimmed caption to fit.")
        tb = _assemble(best_text)

    return tb


def post_to_bluesky(client, media_name, local_path, kind, caption, tags, add_link):
    body_caption, action_line, link_mode, rich_display = choose_caption_and_link_style(caption, add_link)
    tb = build_post_from_caption(
        body_caption, action_line, tags,
        link_mode=link_mode, rich_display=rich_display,
    )
    if kind == "video":
        with open(local_path, "rb") as f:
            client.send_video(text=tb, video=f.read(), video_alt=media_name)
    else:
        with open(local_path, "rb") as f:
            client.send_image(text=tb, image=f.read(), image_alt=media_name)

    link_info = "no"
    if link_mode == "rich":
        link_info = f"rich[{rich_display}]"
    elif link_mode == "plain":
        link_info = "plain-url"
    print(f"✓ Posted {kind}: {body_caption!r}")
    if action_line:
        print(f"  Action: {action_line} → {link_info}")
    if tags:
        print(f"  Tags: {' '.join('#'+t for t in tags)}")


# ═══════════════════════════════════════════════════════════════════════════
#  DISCOVER MODE
# ═══════════════════════════════════════════════════════════════════════════

def run_discover():
    ensure_required_tabs()

    core = load_core_data(force=True)
    creds_values = core["creds"]
    if not creds_values:
        print(f"::error::'{CREDS_TAB}' tab not found or empty in the spreadsheet.")
        sys.exit(1)
    header = hmap(creds_values)

    def hidx(*names):
        for n in names:
            if n.upper() in header:
                return header[n.upper()]
        return None

    handle_col = hidx("BSKY_HANDLE")
    status_col = hidx("ACCOUNT_STATUS")

    if handle_col is None:
        print(f"::error::'{CREDS_TAB}' needs a 'BSKY_HANDLE' column.")
        sys.exit(1)

    force_row = get_env("FORCE_ACCOUNT_ROW", required=False)
    if force_row:
        try:
            eligible = [max(1, int(force_row))]
        except ValueError:
            print(f"::error::FORCE_ACCOUNT_ROW={force_row!r} is not a valid row number.")
            sys.exit(1)
        print(f"FORCE_ACCOUNT_ROW set — running only row {eligible[0]} "
              f"(eligibility filter and MAX_ACCOUNTS_PER_RUN cap skipped).")
    else:
        eligible = []
        for excel_row in range(2, len(creds_values) + 1):
            handle = cell(creds_values, excel_row, handle_col)
            if not handle:
                continue
            status = cell(creds_values, excel_row, status_col).lower() if status_col else ""
            if any(marker in status for marker in SKIP_STATUS_MARKERS):
                print(f"Skipping row {excel_row - 1} ({handle}) — status: {status!r}")
                continue
            eligible.append(excel_row - 1)

        if not eligible:
            print(f"::error::No eligible account rows in '{CREDS_TAB}' "
                  f"(all rows are empty or flagged banned/suspended).")
            sys.exit(1)

        limit_raw = get_env("MAX_ACCOUNTS_PER_RUN", required=False)
        if not limit_raw:
            limit_raw = core["settings"].get("MAX_ACCOUNTS_PER_RUN", "")

        if limit_raw:
            try:
                limit = max(1, int(limit_raw))
                if limit < len(eligible):
                    print(f"Capping this run to the first {limit} of "
                          f"{len(eligible)} eligible accounts (MAX_ACCOUNTS_PER_RUN={limit}).")
                eligible = eligible[:limit]
            except ValueError:
                print(f"Warning: MAX_ACCOUNTS_PER_RUN={limit_raw!r} is not a valid "
                      f"number — running all eligible rows.")
        else:
            print("MAX_ACCOUNTS_PER_RUN not set — running all eligible accounts.")

    print(f"Account rows for this workflow run: {eligible}")
    rows_csv = ",".join(str(r) for r in eligible)

    gh_output = os.environ.get("GITHUB_OUTPUT")
    if not gh_output:
        raise RuntimeError("GITHUB_OUTPUT is not set — must be run inside a GitHub Actions step.")
    with open(gh_output, "a") as f:
        f.write(f"rows={rows_csv}\n")
        f.write(f"count={len(eligible)}\n")


# ═══════════════════════════════════════════════════════════════════════════
#  MAIN CYCLE
# ═══════════════════════════════════════════════════════════════════════════

def run_once():
    cfg = refresh_account_config()

    if not try_acquire_account_lock():
        raise AccountLockedElsewhereError(
            f"Account row {ACCOUNT_ROW} is locked by another repo right now."
        )

    handle = cfg["handle"]

    # Get (or keep) a live proxy for this account before doing anything else.
    proxy_url = ensure_account_proxy(cfg)
    cfg = _cfg()   # pick up any proxy_ip/proxy_port just written to the cache

    print_target_account(handle)
    try:
        client = build_bsky_client(proxy_url)
    except ProxyClientBuildError as exc:
        # A proxy was required/assigned but couldn't actually be attached
        # to the HTTP client — do NOT fall back to an unproxied client.
        # Skip this cycle entirely; nothing gets logged in or posted.
        raise NoProxyAvailableError(str(exc)) from exc

    try:
        client.login(handle, cfg["app_pw"])
    except Exception as exc:
        err = str(exc)
        if proxy_url and _looks_like_proxy_error(exc):
            ip, port = cfg.get("proxy_ip", ""), cfg.get("proxy_port", "")
            if ip and port:
                release_or_kill_proxy(ip, port, alive=False)
                _clear_account_proxy()
            print(f"Login failed through proxy {proxy_url} — marked dead; "
                  f"a new proxy will be picked next cycle: {exc}")
            raise
        if "AccountTakedown" in err or "AccountSuspended" in err:
            raise AccountTakenDownError(f"Account {handle} taken down/suspended.") from exc
        if "AuthenticationRequired" in err or "Invalid identifier or password" in err:
            raise AccountTakenDownError(
                f"Auth failed for {handle} — check BSKY_HANDLE / BSKY_APP_PW in Credentials row {ACCOUNT_ROW}."
            ) from exc
        raise

    _write_account_status("Active")

    if cfg["enable_report"]:
        run_report(client, handle, cfg)

    preferred = choose_media_kind()
    print(f"Post-type chosen for this cycle: {preferred}")

    # ── previewLink path ────────────────────────────────────────────────
    if preferred == "previewLink":
        entry = pick_next_url()
        if entry is None:
            print("No unposted rows left in LinkPlan — falling back to image/video this cycle.")
            preferred = random.choice(["image", "video"])
        else:
            if not claim_url_row(entry):
                print("Lost claim race on this LinkPlan row; will try again next cycle.")
                return

            tags = get_account_hashtags() if cfg["hashtags_enabled_link"] else []
            try:
                post_link_card(
                    client, entry["url"], entry["caption"], tags,
                    cfg["preview_timeout"], cfg["max_thumb_bytes"],
                    auto_caption_enabled=cfg["auto_caption_enabled_link"],
                )
            except Exception as exc:
                err = str(exc)
                release_url_claim(entry)
                if "AccountTakedown" in err or "AccountSuspended" in err:
                    raise AccountTakenDownError(f"Account {handle} taken down mid-cycle.") from exc
                print(f"previewLink post failed for {entry['url']} — claim released: {exc}")
                raise

            mark_url_posted(entry)
            return

    # ── image/video path ────────────────────────────────────────────────
    plan = load_post_plan()
    if not plan:
        raise NoMediaFoundError("Post-plan sheet has no usable rows.")

    fallback = "video" if preferred == "image" else "image"

    file, path, kind, caption, row_num = fetch_media_matching_plan(preferred, plan)
    if not file:
        print(f"No {preferred} matched; trying {fallback}.")
        file, path, kind, caption, row_num = fetch_media_matching_plan(fallback, plan)

    if not file:
        raise NoMediaFoundError("No unposted Mega file matching the post-plan sheet.")

    original_name = file["original_name"]
    claimed_name  = file["claimed_name"]

    try:
        if kind == "image":
            path = compress_image_under_limit(path)

        cfg = _cfg()
        hashtags_on = cfg["hashtags_enabled_image"] if kind == "image" else cfg["hashtags_enabled_video"]
        tags = get_account_hashtags() if hashtags_on else []
        add_link = should_add_link(kind)
        caption_to_use = caption if cfg.get("caption_enabled", True) else ""

        post_to_bluesky(client, original_name, path, kind, caption_to_use, tags, add_link)

    except Exception as exc:
        err = str(exc)
        if "AccountTakedown" in err or "AccountSuspended" in err:
            release_claim(claimed_name, original_name)
            raise AccountTakenDownError(f"Account {handle} taken down mid-cycle.") from exc
        release_claim(claimed_name, original_name)
        print(f"Post failed — claim released, file stays in upload folder.")
        raise

    mark_posted(original_name, row_num)
    move_file_to_processed(claimed_name, original_name)
    try:
        os.remove(path)
    except OSError:
        pass


def main():
    global ACCOUNT_ROW

    if START_JITTER_MAX_SECONDS > 0:
        delay = random.uniform(0, START_JITTER_MAX_SECONDS)
        print(f"Startup jitter: sleeping {delay:.1f}s to spread out Sheets API bursts "
              f"across parallel account jobs…")
        time.sleep(delay)

    try:
        ensure_required_tabs()
        ensure_extra_columns(CREDS_TAB, ["PROXY_IP", "PROXY_PORT", "PROXY_ASSIGNED_AT"])
        ensure_extra_columns(PROXIES_TAB, PROXY_TRACKING_COLUMNS)
        ensure_settings_defaults()
        ACCOUNT_ROW = resolve_account_row()
        load_account_config()
    except Exception as exc:
        print(f"\n{'='*60}\nFATAL: {exc}\n{'='*60}\n")
        sys.exit(1)

    print_config_summary()
    print(f"Starting loop. Loop interval and post-type mix are read from the "
          f"Settings tab of the Google Sheet and re-checked at the start of "
          f"every cycle — edit them there any time, no redeploy needed.")

    while True:
        cycle_start = time.time()
        try:
            run_once()
        except AccountLockedElsewhereError as exc:
            print(f"\n{'='*60}\n{exc}\nSkipping — schedule keeps running.\n{'='*60}\n")
            sys.exit(0)
        except NoProxyAvailableError as exc:
            print(f"\n{'='*60}\n{exc}\nSkipping this cycle — schedule keeps running.\n{'='*60}\n")
            sys.exit(0)
        except NoMediaFoundError as exc:
            print(f"\n{'='*60}\nNO MEDIA: {exc}\nStopping — schedule keeps running.\n{'='*60}\n")
            sys.exit(0)
        except AccountTakenDownError as exc:
            handle  = (_account_config or {}).get("handle", "unknown")
            err_str = str(exc)
            reason  = ("🔑 AUTH FAILED — check handle/app-password in sheet"
                       if "Auth failed" in err_str or "app password" in err_str
                       else "⛔ ACCOUNT TAKEN DOWN / BANNED")
            print(f"\n{'='*60}\n{err_str}\n→ {reason}\n{'='*60}\n")
            _write_account_status(reason)
            log_account_problem(handle, status=reason)
            # This account is done for good — free up its proxy for others.
            ip, port = (_account_config or {}).get("proxy_ip", ""), (_account_config or {}).get("proxy_port", "")
            if ip and port:
                release_or_kill_proxy(ip, port, alive=True)
                _clear_account_proxy()
            with open("ACCOUNT_BANNED", "w") as f:
                f.write(f"{handle}: {reason}\n")
            sys.exit(1)
        except Exception as exc:
            # Includes exhausted-quota-retry-budget errors from sheets_call().
            # We deliberately do NOT exit here — the whole point of the retry
            # budget + this catch-all is that a bad cycle (quota, transient
            # network blip, whatever) costs you one cycle, not the account.
            print(f"Error during cycle: {exc}")

        loop_interval = (_account_config or {}).get("loop_interval_seconds", DEFAULT_LOOP_INTERVAL_SECONDS)
        # Small extra jitter on top of the configured interval so many
        # parallel account jobs don't all re-hit the Sheets API in the same
        # second forever, even if they all started in sync.
        loop_interval = loop_interval + random.uniform(0, min(30, loop_interval * 0.05))
        elapsed   = time.time() - cycle_start
        sleep_for = max(0, loop_interval - elapsed)
        print(f"Cycle done in {elapsed:.1f}s. Sleeping {sleep_for:.1f}s "
              f"(interval={loop_interval:.0f}s from Settings tab, plus jitter)…")
        time.sleep(sleep_for)


if __name__ == "__main__":
    if "--discover" in sys.argv:
        run_discover()
    else:
        main()
