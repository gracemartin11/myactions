import io
import json
import os
import random
import re
import socket
import subprocess
import sys
import time
import uuid
import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from atproto import Client, models
from atproto_client.utils import TextBuilder

RUN_TAG      = os.getenv("GITHUB_RUN_ID") or f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
CLAIM_PREFIX = "CLAIMED_"

# Identity of the repo/runner executing this job right now — used for:
#   (a) the soft cross-repo posting lock (LOCKED_BY / LOCKED_AT columns)
#   (b) the *permanent* account-row assignment (ASSIGNED_REPO / ASSIGNED_STATUS
#       columns) — see resolve_account_row() below.
CURRENT_REPO     = os.getenv("GITHUB_REPOSITORY") or f"local-{socket.gethostname()}"
LOCK_TTL_MINUTES = 45  # longer than the internal post loop, so an actively
                        # running job keeps refreshing its own lock


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
    """Parses values meant as a share/percentage, e.g. '60' -> 0.60, '0.6' -> 0.6."""
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

ACCOUNT_ROW = get_int_env("ACCOUNT_ROW", 1)   # 1-based data row (header is row 0)

# ── rclone / Mega ────────────────────────────────────────────────────────────
# rclone.conf itself is now written by the workflow from the MEGA_RCLONE_CONF
# secret — nothing Mega-related is hardcoded or fetched from a public URL here.
RCLONE_CONFIG_PATH = get_env("RCLONE_CONFIG_PATH", required=False) or "rclone.conf"
RCLONE_REMOTE_NAME = get_env("RCLONE_REMOTE_NAME", required=False) or "mega"

SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


# ═══════════════════════════════════════════════════════════════════════════
#  SPREADSHEETS
# ═══════════════════════════════════════════════════════════════════════════
#
#  All spreadsheet IDs now come from env vars (set by the workflow from
#  repo/org secrets) instead of being hardcoded in source — nothing here
#  reveals which Google Sheet this points at.
#
#  Required:  GOOGLE_SHEET_ID          (master sheet: Credentials/Settings/Report)
#  Optional:  POST_PLAN_SHEET_ID       (defaults to GOOGLE_SHEET_ID)
#             LINK_PLAN_SHEET_ID       (defaults to POST_PLAN_SHEET_ID)

# Master sheet: Sheet1 = per-account credentials, Settings = shared live
# knobs (image/video/previewLink ratio, hashtags, link, caption toggle,
# report freq, etc.), Report = simplified one-row-per-check-in report.
MASTER_SHEET_ID = get_env("GOOGLE_SHEET_ID")
CREDS_TAB       = "Credentials"
SETTINGS_TAB    = "Settings"
REPORT_TAB      = "Report"

# Any ACCOUNT_STATUS value containing one of these (case-insensitive) is
# excluded from discovery — matches the status strings this same script
# writes on a fatal account error (see _write_account_status()).
SKIP_STATUS_MARKERS = ("banned", "suspended", "taken down", "auth failed")

# Simplified 7-column report header — one row per report check-in, not one
# row per followers-count plus N more rows per top post.
REPORT_HEADER = ["Timestamp (UTC)", "Handle", "Followers", "Gained", "Top Post", "Engagement", "Status"]

# Post-plan sheet (separate spreadsheet) — File Name + Caption + Status,
# used for the "image" and "video" post types (Mega-hosted media). Falls
# back to the master sheet ID if a dedicated one isn't set, so a single
# GOOGLE_SHEET_ID secret is enough for a one-spreadsheet setup.
POST_PLAN_SHEET_ID  = get_env("POST_PLAN_SHEET_ID", required=False) or MASTER_SHEET_ID
POSTED_STATUS_VALUE = "posted"

# LinkPlan sheet — URL + Caption + Status, used for the "previewLink" post
# type (social-card / link-preview posts, no media file needed). Lives as
# a tab in the SAME spreadsheet as the post-plan by default — set
# LINK_PLAN_SHEET_ID if you'd rather keep it separate.
LINK_PLAN_SHEET_ID = get_env("LINK_PLAN_SHEET_ID", required=False) or POST_PLAN_SHEET_ID

ASSIGN_STATUS_IN_USE = "In Use"

_URL_RE     = re.compile(r"https?://\S+")
_MENTION_RE = re.compile(r"@\S+")

# Shared, browser-like headers used for the manual link-preview scrape
# fallback and the thumbnail download.
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
LINK_PREVIEW_RETRY_DELAY = 2  # seconds; doubles each retry (2, 4, 8...)


# ═══════════════════════════════════════════════════════════════════════════
#  GOOGLE CREDENTIALS (service account — same one used by the Bluesky
#  scraper and image scraper scripts. Needs Editor access on BOTH the
#  master sheet and the post-plan/link-plan sheet.)
#
#  The credentials JSON itself is written to disk by the workflow from the
#  GOOGLE_SERVICE_ACCOUNT_JSON secret before this script runs — this file
#  never fetches it from anywhere.
# ═══════════════════════════════════════════════════════════════════════════

def get_sheets_service():
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "google_creds.json")
    if not os.path.exists(creds_path):
        raise RuntimeError(f"Google credentials file not found at {creds_path}")
    creds = Credentials.from_service_account_file(creds_path, scopes=SHEETS_SCOPES)
    return build("sheets", "v4", credentials=creds)


# ═══════════════════════════════════════════════════════════════════════════
#  SHEET CELL HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def _col_letter(idx0):
    idx, letters = idx0 + 1, ""
    while idx > 0:
        idx, rem = divmod(idx - 1, 26)
        letters  = chr(65 + rem) + letters
    return letters


# ═══════════════════════════════════════════════════════════════════════════
#  AUTO ACCOUNT-ROW ASSIGNMENT (unchanged)
# ═══════════════════════════════════════════════════════════════════════════

CREDS_RANGE = f"{CREDS_TAB}!A:Z"

def resolve_account_row():
    explicit = get_env("ACCOUNT_ROW", required=False)
    if explicit:
        row = _parse_int(explicit, 1)
        print(f"ACCOUNT_ROW={row} was explicitly set — using it as a manual override "
              f"(auto-assignment skipped).")
        return row

    service = get_sheets_service()
    values  = service.spreadsheets().values().get(
        spreadsheetId=MASTER_SHEET_ID, range=CREDS_RANGE
    ).execute().get("values", [])

    if len(values) < 2:
        raise RuntimeError(f"'{CREDS_TAB}' has no data rows to auto-assign.")

    header = [h.strip().upper() for h in values[0]]

    def hidx(*names):
        for n in names:
            if n in header:
                return header.index(n)
        return None

    handle_idx = hidx("BSKY_HANDLE")
    repo_idx   = hidx("ASSIGNED_REPO")
    status_idx = hidx("ASSIGNED_STATUS")
    at_idx     = hidx("ASSIGNED_AT")

    if handle_idx is None or repo_idx is None or status_idx is None:
        raise RuntimeError(
            f"Auto row-assignment needs 'BSKY_HANDLE', 'ASSIGNED_REPO' and "
            f"'ASSIGNED_STATUS' columns in '{CREDS_TAB}' (optionally "
            f"'ASSIGNED_AT' too). Add any missing ones to the header row, or "
            f"set ACCOUNT_ROW manually for this run."
        )

    def cell(row, idx):
        return row[idx].strip() if idx is not None and len(row) > idx else ""

    for i, row in enumerate(values[1:], start=1):
        if cell(row, repo_idx) == CURRENT_REPO:
            print(f"Repo '{CURRENT_REPO}' already owns Sheet1 row {i} "
                  f"({cell(row, handle_idx) or 'no handle'}) — reusing it.")
            return i

    for i, row in enumerate(values[1:], start=1):
        handle_val = cell(row, handle_idx)
        status_val = cell(row, status_idx)
        if not handle_val:
            continue
        if status_val.lower() == ASSIGN_STATUS_IN_USE.lower():
            continue
        _claim_account_row(service, i, repo_idx, status_idx, at_idx)
        print(f"Claimed Sheet1 row {i} ({handle_val}) for repo '{CURRENT_REPO}'.")
        return i

    raise RuntimeError(
        f"No available account rows left in '{CREDS_TAB}' — every configured "
        f"row is already marked '{ASSIGN_STATUS_IN_USE}'. Add a new account "
        f"row, or clear ASSIGNED_REPO/ASSIGNED_STATUS on one you want to free up."
    )


def _claim_account_row(service, data_idx, repo_idx, status_idx, at_idx):
    sheet_row = data_idx + 1
    now       = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    data = [
        {"range": f"{CREDS_TAB}!{_col_letter(repo_idx)}{sheet_row}",   "values": [[CURRENT_REPO]]},
        {"range": f"{CREDS_TAB}!{_col_letter(status_idx)}{sheet_row}", "values": [[ASSIGN_STATUS_IN_USE]]},
    ]
    if at_idx is not None:
        data.append({"range": f"{CREDS_TAB}!{_col_letter(at_idx)}{sheet_row}", "values": [[now]]})

    service.spreadsheets().values().batchUpdate(
        spreadsheetId=MASTER_SHEET_ID,
        body={"valueInputOption": "RAW", "data": data},
    ).execute()


# ═══════════════════════════════════════════════════════════════════════════
#  ACCOUNT CONFIG + LIVE SETTINGS — from Sheet1, row ACCOUNT_ROW.
# ═══════════════════════════════════════════════════════════════════════════

_account_config         = None
_creds_lock_col_by      = None
_creds_lock_col_at      = None
_creds_status_col       = None
_creds_status_at_col    = None
_global_settings_cache  = None

DEFAULT_LOOP_INTERVAL_SECONDS = 1800


def load_global_settings(force_refresh=False):
    global _global_settings_cache
    if _global_settings_cache is not None and not force_refresh:
        return _global_settings_cache

    settings = {}
    try:
        service = get_sheets_service()
        result  = service.spreadsheets().values().get(
            spreadsheetId=MASTER_SHEET_ID, range=f"{SETTINGS_TAB}!A:B"
        ).execute()
        values = result.get("values", [])
        for row in values[1:]:
            if len(row) >= 1 and row[0].strip():
                key = row[0].strip().upper()
                val = row[1].strip() if len(row) > 1 else ""
                settings[key] = val
    except Exception as exc:
        print(f"Note: '{SETTINGS_TAB}' tab not found or unreadable — using built-in "
              f"defaults for shared settings ({exc}).")

    _global_settings_cache = settings
    return _global_settings_cache


def load_account_config(force_refresh=False):
    global _account_config, _creds_lock_col_by, _creds_lock_col_at, _creds_status_col, _creds_status_at_col

    if _account_config is not None and not force_refresh:
        return _account_config

    service = get_sheets_service()
    result  = service.spreadsheets().values().get(
        spreadsheetId=MASTER_SHEET_ID, range=CREDS_RANGE
    ).execute()
    values = result.get("values", [])

    if len(values) < 2:
        raise RuntimeError(
            f"'{CREDS_TAB}' in the master sheet is empty or has only a header. "
            "Add at least one account data row."
        )

    data_idx = ACCOUNT_ROW
    if data_idx >= len(values):
        raise RuntimeError(
            f"ACCOUNT_ROW={ACCOUNT_ROW} but '{CREDS_TAB}' only has "
            f"{len(values)-1} data row(s)."
        )

    header = [h.strip().upper() for h in values[0]]
    row    = values[data_idx]

    def col(*names):
        for n in names:
            try:
                idx = header.index(n.upper())
                return row[idx].strip() if idx < len(row) else ""
            except ValueError:
                continue
        return ""

    _creds_lock_col_by = header.index("LOCKED_BY") if "LOCKED_BY" in header else None
    _creds_lock_col_at = header.index("LOCKED_AT") if "LOCKED_AT" in header else None
    _creds_status_col    = header.index("ACCOUNT_STATUS") if "ACCOUNT_STATUS" in header else None
    _creds_status_at_col = header.index("ACCOUNT_STATUS_AT") if "ACCOUNT_STATUS_AT" in header else None

    shared = load_global_settings(force_refresh)
    def setting(key):
        return col(key) or shared.get(key, "")

    raw_link     = col("LINK_URL") or "https://foodiesposts.com"
    link_url     = raw_link if raw_link.startswith("http") else f"https://{raw_link}"
    link_display = col("LINK_DISPLAY_TEXT") or link_url.replace("https://","").replace("http://","")

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
        "video_ratio":               video_ratio,
        "link_ratio":                link_ratio,
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
        "post_plan_sheet_name":     setting("POST_PLAN_SHEET_NAME") or "Sheet1",
        "link_plan_sheet_name":     setting("LINK_PLAN_SHEET_NAME") or "LinkPlan",
        "loop_interval_seconds":    _parse_int(setting("LOOP_INTERVAL_SECONDS"),
                                                DEFAULT_LOOP_INTERVAL_SECONDS),

        "locked_by": col("LOCKED_BY"),
        "locked_at": col("LOCKED_AT"),
        "account_status": col("ACCOUNT_STATUS"),
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
#  CROSS-REPO SOFT LOCK (unchanged)
# ═══════════════════════════════════════════════════════════════════════════

class AccountLockedElsewhereError(Exception):
    """Non-fatal — another repo currently owns this account row."""


def _write_lock_heartbeat(owner, ts):
    try:
        service = get_sheets_service()
        by_col  = _col_letter(_creds_lock_col_by)
        at_col  = _col_letter(_creds_lock_col_at)
        sheet_row = ACCOUNT_ROW + 1
        service.spreadsheets().values().update(
            spreadsheetId=MASTER_SHEET_ID,
            range=f"{CREDS_TAB}!{by_col}{sheet_row}:{at_col}{sheet_row}",
            valueInputOption="RAW",
            body={"values": [[owner, ts]]},
        ).execute()
        if _account_config:
            _account_config["locked_by"] = owner
            _account_config["locked_at"] = ts
    except Exception as exc:
        print(f"Warning: could not write account lock heartbeat: {exc}")


def try_acquire_account_lock():
    cfg = refresh_account_config()

    if _creds_lock_col_by is None or _creds_lock_col_at is None:
        return True

    locked_by     = cfg.get("locked_by", "")
    locked_at_raw = cfg.get("locked_at", "")

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

    _write_lock_heartbeat(CURRENT_REPO, time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    return True


def _write_account_status(status):
    """Writes a human-readable status directly onto this account's own
    Sheet1 row (e.g. 'Active', 'Banned', 'Auth Failed')."""
    global _account_config
    if _creds_status_col is None:
        print("Note: no ACCOUNT_STATUS column in Sheet1 — add one to track per-row status.")
        return
    try:
        service   = get_sheets_service()
        sheet_row = ACCOUNT_ROW + 1
        status_col = _col_letter(_creds_status_col)
        data = [{"range": f"{CREDS_TAB}!{status_col}{sheet_row}", "values": [[status]]}]
        if _creds_status_at_col is not None:
            at_col = _col_letter(_creds_status_at_col)
            data.append({"range": f"{CREDS_TAB}!{at_col}{sheet_row}", "values": [[_now_str()]]})
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=MASTER_SHEET_ID,
            body={"valueInputOption": "RAW", "data": data},
        ).execute()
        if _account_config:
            _account_config["account_status"] = status
        print(f"Sheet1 ACCOUNT_STATUS set to '{status}' for row {ACCOUNT_ROW}.")
    except Exception as exc:
        print(f"Warning: could not update ACCOUNT_STATUS: {exc}")


# ═══════════════════════════════════════════════════════════════════════════
#  TEXT HELPERS (unchanged)
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
    print("── Run config (live from 'Settings' tab + Sheet1, re-checked every cycle) ──")
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
    print(f"  Post-plan tab (media):    {cfg['post_plan_sheet_name']}")
    print(f"  LinkPlan tab (previewLink): {cfg['link_plan_sheet_name']}")
    print(f"  Google auth:              service account (GOOGLE_APPLICATION_CREDENTIALS)")
    if _creds_lock_col_by is not None:
        print(f"  Cross-repo lock:          enabled (owner={cfg.get('locked_by') or '—'}, "
              f"last heartbeat={cfg.get('locked_at') or '—'})")
    else:
        print("  Cross-repo lock:          disabled (add LOCKED_BY / LOCKED_AT columns to enable)")
    print("─────────────────────────────────────────────────")


# ═══════════════════════════════════════════════════════════════════════════
#  REPORT TAB (unchanged)
# ═══════════════════════════════════════════════════════════════════════════

def _now_str():
    return time.strftime("%Y-%m-%d %H:%M", time.gmtime()) + " UTC"

def _parse_report_ts(s):
    try:
        return time.mktime(time.strptime(s.replace(" UTC", ""), "%Y-%m-%d %H:%M"))
    except Exception:
        return None


def _ensure_report_tab(service):
    try:
        meta     = service.spreadsheets().get(spreadsheetId=MASTER_SHEET_ID).execute()
        existing = {s["properties"]["title"].strip().lower()
                    for s in meta.get("sheets", [])}
        if REPORT_TAB.lower() not in existing:
            service.spreadsheets().batchUpdate(
                spreadsheetId=MASTER_SHEET_ID,
                body={"requests": [{"addSheet": {"properties": {"title": REPORT_TAB}}}]},
            ).execute()
            print(f"Created '{REPORT_TAB}' tab.")
    except Exception as exc:
        if "already exists" not in str(exc).lower():
            print(f"Warning: could not verify/create Report tab: {exc}")

    last_col = _col_letter(len(REPORT_HEADER) - 1)
    try:
        r = service.spreadsheets().values().get(
            spreadsheetId=MASTER_SHEET_ID, range=f"{REPORT_TAB}!A1:{last_col}1"
        ).execute()
        existing_header = r.get("values", [[]])[0] if r.get("values") else []
        if existing_header != REPORT_HEADER:
            service.spreadsheets().values().update(
                spreadsheetId=MASTER_SHEET_ID,
                range=f"{REPORT_TAB}!A1:{last_col}1",
                valueInputOption="RAW",
                body={"values": [REPORT_HEADER]},
            ).execute()
            print(f"Set '{REPORT_TAB}' header to: {REPORT_HEADER}")
    except Exception as exc:
        print(f"Warning: could not check/update report header: {exc}")


def _last_report_for_handle(service, handle):
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=MASTER_SHEET_ID, range=f"{REPORT_TAB}!A:G"
        ).execute()
        rows = result.get("values", [])[1:]
        for row in reversed(rows):
            if len(row) >= 2 and row[1] == handle:
                ts = row[0] if len(row) > 0 else None
                followers = None
                if len(row) > 2 and row[2].strip():
                    try:
                        followers = int(row[2])
                    except ValueError:
                        followers = None
                return ts, followers
    except Exception:
        pass
    return None, None


def _report_due(service, handle, times_per_day):
    times_per_day = max(1, times_per_day)
    last_ts, _ = _last_report_for_handle(service, handle)
    if last_ts is None:
        return True
    last_epoch = _parse_report_ts(last_ts)
    if last_epoch is None:
        return True
    interval_seconds = 86400.0 / times_per_day
    return (time.time() - last_epoch) >= interval_seconds


def _append_report(service, rows):
    service.spreadsheets().values().append(
        spreadsheetId=MASTER_SHEET_ID,
        range=f"{REPORT_TAB}!A:G",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    ).execute()


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


def generate_report(client, handle, service, cfg):
    if not _report_due(service, handle, cfg["report_times_per_day"]):
        print(f"Report for {handle} not due yet (limit: {cfg['report_times_per_day']}x/24h).")
        return
    try:
        profile = client.get_profile(actor=handle)
        total   = profile.followers_count or 0

        _, prev_followers = _last_report_for_handle(service, handle)
        prev   = prev_followers if prev_followers is not None else total
        gained = total - prev

        top_preview, top_engagement = _top_post_summary(
            client, handle, cfg["top_posts_count"], cfg["top_posts_within"]
        )

        row = [_now_str(), handle, total, gained, top_preview, top_engagement, "OK"]
        _append_report(service, [row])
        print(f"Report logged for {handle}: {total} followers ({gained:+d} since last), "
              f"top post engagement={top_engagement}.")
    except Exception as exc:
        print(f"Warning: report generation failed: {exc}")


def run_report(client, handle, cfg):
    try:
        service = get_sheets_service()
        _ensure_report_tab(service)
        generate_report(client, handle, service, cfg)
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
        service = get_sheets_service()
        _ensure_report_tab(service)
        _append_report(service, [[_now_str(), handle, "", "", "", "", status]])
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
#  LINK-IN-POST DECISION (unrelated to the previewLink post type — this is
#  the "append a CTA link" toggle inside image/video captions)
# ═══════════════════════════════════════════════════════════════════════════

def should_add_link(kind):
    cfg     = _cfg()
    enabled = cfg["link_enabled_image"] if kind == "image" else cfg["link_enabled_video"]
    if not enabled:
        return False
    return random.random() < cfg["link_percentage"]


# ═══════════════════════════════════════════════════════════════════════════
#  POST-TYPE SELECTION — image / video / previewLink, weighted by the
#  normalized IMAGE_RATIO / VIDEO_RATIO / LINK_RATIO settings.
# ═══════════════════════════════════════════════════════════════════════════

def choose_media_kind():
    cfg = _cfg()
    return random.choices(
        ["image", "video", "previewLink"],
        weights=[cfg["image_ratio"], cfg["video_ratio"], cfg["link_ratio"]],
        k=1,
    )[0]


# ═══════════════════════════════════════════════════════════════════════════
#  POST-PLAN SHEET (File Name + Caption + Status) — image/video media
# ═══════════════════════════════════════════════════════════════════════════

_post_plan_cache          = None
_post_plan_status_col_idx = None


def get_post_plan_tab_name():
    return _cfg()["post_plan_sheet_name"]


def load_post_plan(force_refresh=False):
    global _post_plan_cache, _post_plan_status_col_idx
    if _post_plan_cache is not None and not force_refresh:
        return _post_plan_cache

    tab     = get_post_plan_tab_name()
    service = get_sheets_service()
    result  = service.spreadsheets().values().get(
        spreadsheetId=POST_PLAN_SHEET_ID, range=f"{tab}!A:Z"
    ).execute()
    values  = result.get("values", [])
    if not values:
        print(f"Warning: post-plan tab '{tab}' is empty.")
        _post_plan_cache = {}
        return _post_plan_cache

    header = [h.strip().lower() for h in values[0]]
    def ci(*names):
        for n in names:
            if n in header: return header.index(n)
        return None

    file_idx    = ci("file name", "filename", "file")
    caption_idx = ci("caption", "captions")
    status_idx  = ci("status")
    _post_plan_status_col_idx = status_idx

    if file_idx is None or caption_idx is None:
        print(f"Warning: post-plan needs 'File Name' and 'Caption' columns. Found: {header}")
        _post_plan_cache = {}
        return _post_plan_cache
    if status_idx is None:
        print("Warning: no 'Status' column — posted files won't be tracked.")

    plan_exact = {}
    plan_lower = {}
    already    = 0
    for i, row in enumerate(values[1:], start=2):
        fname   = row[file_idx].strip()    if len(row) > file_idx    else ""
        caption = row[caption_idx].strip() if len(row) > caption_idx else ""
        status  = row[status_idx].strip()  if status_idx is not None and len(row) > status_idx else ""
        if not fname: continue
        entry = {"caption": caption, "row": i, "status": status}
        plan_exact[fname]         = entry
        plan_lower[fname.lower()] = entry
        if status.lower() == POSTED_STATUS_VALUE: already += 1

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
    for attempt in range(1, retries + 1):
        try:
            tab     = get_post_plan_tab_name()
            col_l   = _col_letter(_post_plan_status_col_idx)
            service = get_sheets_service()
            service.spreadsheets().values().update(
                spreadsheetId=POST_PLAN_SHEET_ID,
                range=f"{tab}!{col_l}{row_number}",
                valueInputOption="RAW",
                body={"values": [[POSTED_STATUS_VALUE]]},
            ).execute()
            if _post_plan_cache:
                for d in (_post_plan_cache.get("exact",{}), _post_plan_cache.get("lower",{})):
                    if filename in d: d[filename]["status"] = POSTED_STATUS_VALUE
                    if filename.lower() in d: d[filename.lower()]["status"] = POSTED_STATUS_VALUE
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
#  LINKPLAN SHEET (URL + Caption + Status) — previewLink posts. Same
#  claim/post/mark-posted pattern as the post-plan sheet above, just keyed
#  on URL instead of a media filename (there's no file to download).
# ═══════════════════════════════════════════════════════════════════════════

def get_link_plan_tab_name():
    return _cfg()["link_plan_sheet_name"]


def load_link_plan(service):
    tab = get_link_plan_tab_name()
    values = service.spreadsheets().values().get(
        spreadsheetId=LINK_PLAN_SHEET_ID, range=f"{tab}!A:C"
    ).execute().get("values", [])
    if len(values) < 2:
        return []
    header = [h.strip().lower() for h in values[0]]
    def ci(*names):
        for n in names:
            if n in header:
                return header.index(n)
        return None
    url_idx, cap_idx, status_idx = ci("url"), ci("caption"), ci("status")
    if url_idx is None:
        raise RuntimeError(f"'{tab}' needs a 'URL' column.")

    rows = []
    for i, row in enumerate(values[1:], start=2):
        url = row[url_idx].strip() if len(row) > url_idx else ""
        if not url:
            continue
        caption = row[cap_idx].strip() if cap_idx is not None and len(row) > cap_idx else ""
        status  = row[status_idx].strip() if status_idx is not None and len(row) > status_idx else ""
        rows.append({"url": url, "caption": caption, "status": status, "row": i, "status_col": status_idx})
    return rows


def pick_next_url(service):
    plan = load_link_plan(service)
    for entry in plan:
        s = entry["status"].lower()
        if s == POSTED_STATUS_VALUE or s.startswith(CLAIM_PREFIX.lower()):
            continue
        return entry
    return None


def claim_url_row(service, entry):
    """Soft-claims a row by writing CLAIMED_<runtag> into Status, so two
    concurrent runners don't grab the same URL. Returns True if the claim
    stuck (nobody else claimed it first)."""
    if entry["status_col"] is None:
        return True
    tab = get_link_plan_tab_name()
    col_l = _col_letter(entry["status_col"])
    claim_val = f"{CLAIM_PREFIX}{RUN_TAG}"
    service.spreadsheets().values().update(
        spreadsheetId=LINK_PLAN_SHEET_ID, range=f"{tab}!{col_l}{entry['row']}",
        valueInputOption="RAW", body={"values": [[claim_val]]},
    ).execute()
    check = service.spreadsheets().values().get(
        spreadsheetId=LINK_PLAN_SHEET_ID, range=f"{tab}!{col_l}{entry['row']}"
    ).execute().get("values", [[""]])
    return check[0][0].strip() == claim_val if check else False


def mark_url_posted(service, entry):
    if entry["status_col"] is None:
        return
    tab = get_link_plan_tab_name()
    col_l = _col_letter(entry["status_col"])
    service.spreadsheets().values().update(
        spreadsheetId=LINK_PLAN_SHEET_ID, range=f"{tab}!{col_l}{entry['row']}",
        valueInputOption="RAW", body={"values": [[POSTED_STATUS_VALUE]]},
    ).execute()


def release_url_claim(service, entry):
    if entry["status_col"] is None:
        return
    try:
        tab = get_link_plan_tab_name()
        col_l = _col_letter(entry["status_col"])
        service.spreadsheets().values().update(
            spreadsheetId=LINK_PLAN_SHEET_ID, range=f"{tab}!{col_l}{entry['row']}",
            valueInputOption="RAW", body={"values": [[""]]},
        ).execute()
    except Exception as exc:
        print(f"Warning: could not release claim on LinkPlan row {entry['row']}: {exc}")


# ═══════════════════════════════════════════════════════════════════════════
#  LINK PREVIEW (og:title / og:description / og:image) for previewLink posts
#
#  PRIMARY PATH: Bluesky's own card-generation service, cardyb
#  (https://cardyb.bsky.app/v1/extract) — the same backend the official
#  Bluesky app/web client hits when you paste a link into the composer.
#  FALLBACK PATH: manually scrape the page's own <meta> tags if cardyb is
#  unreachable or returns nothing usable after retries.
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

    raise NoPreviewError(f"Failed to fetch {url} after {LINK_PREVIEW_MAX_RETRIES} attempts (cardyb + manual)") from last_exc


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
    """Download the preview image (if any) and upload it as a blob for the
    card, retrying transient failures and shrinking it if it's over size."""
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
    # NOTE: strip only spaces/tabs here, not newlines — compose_fallback_caption
    # intentionally prepends a leading "\n" before the title, and a plain
    # .strip() would silently remove it.
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
    """When the sheet has no Caption for a row, build one from the fetched
    preview instead: a blank line, then og:title, then description
    directly underneath. Hashtags still get appended after this block."""
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
        # Don't let a bad preview fetch kill the whole cycle — fall back
        # to a plain post that still includes the link as text.
        print(f"Warning: preview fetch failed ({exc}); posting as plain link instead.")

    used_auto_caption = False
    effective_caption = caption
    if not effective_caption and preview and auto_caption_enabled:
        effective_caption = compose_fallback_caption(preview)
        used_auto_caption = bool(effective_caption)
        if used_auto_caption:
            print("No Caption in sheet — using title + description from the preview instead.")
    elif not effective_caption and preview and not auto_caption_enabled:
        print("No Caption in sheet and previewLink auto-caption is off — posting without a caption.")

    tb = build_link_caption_text(effective_caption, tags, fallback_url=(url if preview is None else None))
    client.send_post(text=tb, embed=embed)

    posted_url = preview["final_url"] if preview else url
    caption_source = "auto (title+description)" if used_auto_caption else ("sheet" if caption else "no")
    print(f"✓ Posted {'link card' if embed else 'plain link'} for {posted_url} "
          f"(caption={caption_source}, tags={len(tags)})")


# ═══════════════════════════════════════════════════════════════════════════
#  MEGA.NZ HELPERS (via rclone) — image/video media
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


def _rclone_run(args):
    return subprocess.run(["rclone", "--config", RCLONE_CONFIG_PATH] + args,
                           capture_output=True, text=True)


def rclone_list_files(remote_folder):
    result = _rclone_run(["lsf", remote_folder, "--files-only"])
    if result.returncode != 0:
        print(f"Warning: rclone lsf failed for '{remote_folder}': {result.stderr.strip()[-300:]}")
        return []
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def rclone_claim(remote_folder, name):
    """Server-side rename to claim a file. If another runner already
    claimed/moved it, moveto fails harmlessly and we just try the next
    candidate — this is the Mega equivalent of the old Drive claim-rename."""
    claimed_name = f"{CLAIM_PREFIX}{RUN_TAG}__{name}"
    result = _rclone_run(["moveto", f"{remote_folder}/{name}", f"{remote_folder}/{claimed_name}"])
    return claimed_name if result.returncode == 0 else None


def rclone_download(remote_folder, filename, local_path):
    result = _rclone_run(["copyto", f"{remote_folder}/{filename}", local_path])
    return result.returncode == 0


def rclone_move(src, dst):
    result = _rclone_run(["moveto", src, dst])
    return result.returncode == 0


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
            with open(local_path, "wb") as f: f.write(buf.getvalue())
            print(f"Compressed {orig/1024:.0f} KB → {buf.tell()/1024:.0f} KB (q={q}).")
            return local_path
    w, h = img.size
    scale = 0.9
    while scale > 0.3:
        r = img.resize((max(1,int(w*scale)), max(1,int(h*scale))), Image.LANCZOS)
        buf = io.BytesIO()
        r.save(buf, format="JPEG", quality=70, optimize=True)
        if buf.tell() <= max_bytes:
            with open(local_path, "wb") as f: f.write(buf.getvalue())
            print(f"Resized+compressed → {buf.tell()/1024:.0f} KB.")
            return local_path
        scale -= 0.1
    with open(local_path, "wb") as f: f.write(buf.getvalue())
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

def build_post_from_caption(caption, tags, add_link):
    cfg  = _cfg()
    text = replace_mentions(caption) if caption else ""

    def _assemble(caption_text):
        tb = TextBuilder()
        if add_link:
            m = _URL_RE.search(caption_text)
            if m:
                before = caption_text[:m.start()].rstrip()
                after  = _URL_RE.sub("", caption_text[m.end():]).strip()
                if before:
                    tb.text(before + " ")
                tb.link(cfg["link_display_text"], cfg["link_url"])
                if after:
                    tb.text(" " + after)
            else:
                if caption_text:
                    tb.text(caption_text)
                    tb.text("\n\n")
                tb.link(cfg["link_display_text"], cfg["link_url"])
        else:
            text_no_url = _URL_RE.sub("", caption_text).strip()
            if text_no_url:
                tb.text(text_no_url)

        if tags:
            tb.text("\n\n")
            for i, tag in enumerate(tags):
                tb.tag(f"#{tag}", tag)
                if i < len(tags) - 1:
                    tb.text(" ")
        return tb

    tb    = _assemble(text)
    plain = tb.build_text()

    if len(plain) > MAX_POST_GRAPHEMES:
        lo, hi, best_text = 0, len(text), ""
        while lo <= hi:
            mid   = (lo + hi) // 2
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
    tb = build_post_from_caption(caption, tags, add_link)
    if kind == "video":
        with open(local_path, "rb") as f:
            client.send_video(text=tb, video=f.read(), video_alt=media_name)
    else:
        with open(local_path, "rb") as f:
            client.send_image(text=tb, image=f.read(), image_alt=media_name)

    preview = replace_mentions(caption or "")
    if add_link:
        m = _URL_RE.search(preview)
        if m:
            preview = (preview[:m.start()].rstrip()
                       + f" [{_cfg()['link_display_text']}]"
                       + _URL_RE.sub("", preview[m.end():]).strip())
        else:
            preview = (preview + f" [{_cfg()['link_display_text']}]").strip()
    else:
        preview = _URL_RE.sub("", preview).strip()
    print(f"✓ Posted {kind}: {preview!r} (link={'yes' if add_link else 'no'})")
    if tags:
        print(f"  Tags: {' '.join('#'+t for t in tags)}")


# ═══════════════════════════════════════════════════════════════════════════
#  DISCOVER MODE (`python postnow_status_post.py --discover`)
#
#  Runs once, in the workflow's 'discover-accounts' job — merged in from
#  the old standalone discover_accounts.py so there's only ONE script in
#  the repo. Reads the Credentials tab, drops empty/banned/suspended rows,
#  applies the optional MAX_ACCOUNTS_PER_RUN cap (or FORCE_ACCOUNT_ROW
#  override), and writes a plain comma-separated row list + count to
#  $GITHUB_OUTPUT so the 'post' job can fan out one job per eligible
#  account row.
#
#  IMPORTANT: we write a plain CSV string ("1,2,3,4"), NOT a JSON-shaped
#  string ("{\"account_row\": [1, 2, 3, 4]}"). GitHub Actions runs an
#  automatic secret-scanner on every step output and will silently blank
#  the ENTIRE output ("Skip output 'matrix' since it may contain secret")
#  if the text happens to overlap a registered secret value — which can
#  trigger by pure coincidence on a JSON string full of braces/quotes/
#  digits. The workflow yaml rebuilds the JSON array from this CSV string
#  via fromJson(format('[{0}]', ...)) instead.
# ═══════════════════════════════════════════════════════════════════════════

def run_discover():
    service = get_sheets_service()
    values  = service.spreadsheets().values().get(
        spreadsheetId=MASTER_SHEET_ID, range=CREDS_RANGE
    ).execute().get("values", [])

    if len(values) < 2:
        print(f"::error::'{CREDS_TAB}' has no data rows — add at least one account.")
        sys.exit(1)

    header = [h.strip().upper() for h in values[0]]

    def hidx(*names):
        for n in names:
            if n in header:
                return header.index(n)
        return None

    handle_idx = hidx("BSKY_HANDLE")
    status_idx = hidx("ACCOUNT_STATUS")

    if handle_idx is None:
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
        for i, row in enumerate(values[1:], start=1):
            handle = row[handle_idx].strip() if len(row) > handle_idx else ""
            if not handle:
                continue
            status = (row[status_idx].strip().lower()
                      if status_idx is not None and len(row) > status_idx else "")
            if any(marker in status for marker in SKIP_STATUS_MARKERS):
                print(f"Skipping row {i} ({handle}) — status: {status!r}")
                continue
            eligible.append(i)

        if not eligible:
            print(f"::error::No eligible account rows in '{CREDS_TAB}' "
                  f"(all rows are empty or flagged banned/suspended).")
            sys.exit(1)

        # Cap: workflow_dispatch input takes priority; otherwise fall back
        # to the Settings tab's MAX_ACCOUNTS_PER_RUN; blank/unset -> run all.
        limit_raw = get_env("MAX_ACCOUNTS_PER_RUN", required=False)
        if not limit_raw:
            settings = load_global_settings()
            limit_raw = settings.get("MAX_ACCOUNTS_PER_RUN", "")

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
#  MAIN CYCLE — one account row, one post per cycle, picked as
#  image / video / previewLink by the ratio settings.
# ═══════════════════════════════════════════════════════════════════════════

def run_once():
    cfg = refresh_account_config()

    if not try_acquire_account_lock():
        raise AccountLockedElsewhereError(
            f"Account row {ACCOUNT_ROW} is locked by another repo right now."
        )

    handle = cfg["handle"]

    print_target_account(handle)
    client = Client()
    try:
        client.login(handle, cfg["app_pw"])
    except Exception as exc:
        err = str(exc)
        if "AccountTakedown" in err or "AccountSuspended" in err:
            raise AccountTakenDownError(f"Account {handle} taken down/suspended.") from exc
        if "AuthenticationRequired" in err or "Invalid identifier or password" in err:
            raise AccountTakenDownError(
                f"Auth failed for {handle} — check BSKY_HANDLE / BSKY_APP_PW in sheet row {ACCOUNT_ROW}."
            ) from exc
        raise

    _write_account_status("Active")

    if cfg["enable_report"]:
        run_report(client, handle, cfg)

    preferred = choose_media_kind()
    print(f"Post-type chosen for this cycle: {preferred}")

    # ── previewLink path ────────────────────────────────────────────────
    if preferred == "previewLink":
        sheets_service = get_sheets_service()
        entry = pick_next_url(sheets_service)
        if entry is None:
            print("No unposted rows left in LinkPlan — falling back to image/video this cycle.")
            preferred = random.choice(["image", "video"])
        else:
            if not claim_url_row(sheets_service, entry):
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
                release_url_claim(sheets_service, entry)
                if "AccountTakedown" in err or "AccountSuspended" in err:
                    raise AccountTakenDownError(f"Account {handle} taken down mid-cycle.") from exc
                print(f"previewLink post failed for {entry['url']} — claim released: {exc}")
                raise

            mark_url_posted(sheets_service, entry)
            return  # cycle complete

    # ── image/video path (existing Mega-backed flow) ────────────────────
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

        cfg = _cfg()  # re-fetch in case the sheet changed between fetch and post
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
    try:
        ACCOUNT_ROW = resolve_account_row()
        load_account_config()
    except Exception as exc:
        print(f"\n{'='*60}\nFATAL: {exc}\n{'='*60}\n")
        sys.exit(1)

    print_config_summary()
    print(f"Starting loop. Loop interval and post-type mix are read from the "
          f"Settings tab and re-checked at the start of every cycle — edit "
          f"them in Google Sheets any time, no redeploy needed.")

    while True:
        cycle_start = time.time()
        try:
            run_once()
        except AccountLockedElsewhereError as exc:
            print(f"\n{'='*60}\n{exc}\nSkipping — schedule keeps running.\n{'='*60}\n")
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
            # Marker file for local/manual debugging. NOTE: this account is
            # excluded from FUTURE runs via ACCOUNT_STATUS in the sheet
            # (run_discover() skips banned/suspended/auth-failed rows) —
            # the workflow no longer disables itself on a single account's
            # failure, since one workflow run now serves many accounts.
            with open("ACCOUNT_BANNED", "w") as f:
                f.write(f"{handle}: {reason}\n")
            sys.exit(1)
        except Exception as exc:
            print(f"Error during cycle: {exc}")

        loop_interval = (_account_config or {}).get("loop_interval_seconds", DEFAULT_LOOP_INTERVAL_SECONDS)
        elapsed   = time.time() - cycle_start
        sleep_for = max(0, loop_interval - elapsed)
        print(f"Cycle done in {elapsed:.1f}s. Sleeping {sleep_for:.1f}s "
              f"(interval={loop_interval}s from Settings tab)…")
        time.sleep(sleep_for)


if __name__ == "__main__":
    if "--discover" in sys.argv:
        run_discover()
    else:
        main()
