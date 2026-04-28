"""
Cold Email Analytics — Full Sync Script (v13)
===============================================

Full historical load from SmartLead + GHL -> Google Sheets.
READ-ONLY: only GET requests to external APIs.

V13 CHANGES:
- Multi-client support via --client flag
- CAMPAIGN_FILTER in .env controls which campaigns to sync
- Dynamic campaign discovery (no more hardcoded campaign list)

Usage: python sync.py --client rainmaker
"""

import os
import re
import sys
import time
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build

# --- Client selection ---
parser = argparse.ArgumentParser(description="Full sync for a specific client")
parser.add_argument("--client", required=True, help="Client folder name under clients/")
args = parser.parse_args()

client_dir = Path(__file__).parent / "clients" / args.client
if not client_dir.exists():
    print(f"ERROR: Client folder not found: {client_dir}")
    sys.exit(1)

load_dotenv(dotenv_path=client_dir / ".env")

SMARTLEAD_API_KEY = os.getenv("SMARTLEAD_API_KEY")
GHL_PRIVATE_TOKEN = os.getenv("GHL_PRIVATE_TOKEN")
GHL_LOCATION_ID = os.getenv("GHL_LOCATION_ID")
GHL_PIPELINE_ID = os.getenv("GHL_PIPELINE_ID")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
CLIENT_NAME = os.getenv("CLIENT_NAME", args.client)
CAMPAIGN_FILTER = os.getenv("CAMPAIGN_FILTER", "")

# Service account JSON: check client folder first, fall back to .env path
_creds_in_client = client_dir / "service-account.json"
if _creds_in_client.exists():
    GOOGLE_CREDS_PATH = str(_creds_in_client)
else:
    GOOGLE_CREDS_PATH = os.getenv("GOOGLE_CREDS_PATH", "./service-account.json")

# TRACKED_CAMPAIGNS is built dynamically — see discover_filtered_campaigns()

GHL_CF_CAMPAIGN_ID = "IyS6bhX7hdUcg81AfRda"
GHL_CF_CALL_PROPOSED_AT = "SjdCvQ9cTILmG8MKqIyd"
GHL_CF_DAY1_FOLLOWUP_AT = "syJfDi9KwrqolBasfOwC"
GHL_CF_DAY2_FOLLOWUP_AT = "Z8iIXI5ZYaCa2C9GeDpT"
GHL_CF_BOOKED_CALL_AT = "sQUdyW4BzV6U5OLHRpeX"

SL_BASE = "https://server.smartlead.ai/api/v1"
GHL_BASE = "https://services.leadconnectorhq.com"
SHEETS_SCOPE = ["https://www.googleapis.com/auth/spreadsheets"]

def get_ghl_headers():
    return {
        "Authorization": f"Bearer {GHL_PRIVATE_TOKEN}",
        "Version": "2021-07-28",
        "Accept": "application/json",
    }


class ReadOnlyClient:
    @staticmethod
    def get(url, **kwargs):
        return requests.get(url, **kwargs)
    def __getattr__(self, name):
        if name in ("post", "put", "patch", "delete"):
            raise RuntimeError(f"BLOCKED: {name.upper()} is not permitted.")
        raise AttributeError(name)

http = ReadOnlyClient()


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def strip_html(raw):
    if not raw: return ""
    text = re.sub(r"<[^>]+>", " ", raw)
    return re.sub(r"\s+", " ", text).strip()

def parse_iso(s):
    if not s: return None
    try:
        s = s.replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except Exception:
        return None


def discover_filtered_campaigns():
    """
    Fetch all campaigns from SmartLead, filter by CAMPAIGN_FILTER keywords.
    If CAMPAIGN_FILTER is empty, returns ALL campaigns.
    Returns list of (campaign_id, campaign_name) tuples.
    """
    log(f"Discovering campaigns (filter: '{CAMPAIGN_FILTER or 'none — all campaigns'}')...")

    try:
        data = sl_get("/campaigns")
        campaigns = data if isinstance(data, list) else data.get("data", [])
    except Exception as e:
        log(f"  ERROR Could not fetch campaign list: {e}")
        return []

    log(f"  Total campaigns in account: {len(campaigns)}")

    if not CAMPAIGN_FILTER:
        result = [(c.get("id"), c.get("name", "(unnamed)")) for c in campaigns if c.get("id")]
        log(f"  No filter applied — returning all {len(result)} campaigns")
        return result

    keywords = [k.strip().lower() for k in CAMPAIGN_FILTER.split(",")]
    filtered = []
    for c in campaigns:
        name = (c.get("name") or "").lower()
        if any(kw in name for kw in keywords):
            filtered.append((c.get("id"), c.get("name", "(unnamed)")))

    log(f"  Matched {len(filtered)} campaigns for filter '{CAMPAIGN_FILTER}':")
    for cid, cname in filtered:
        log(f"    - {cid}: {cname}")

    return filtered

def sl_get(path, params=None):
    p = {"api_key": SMARTLEAD_API_KEY}
    if params: p.update(params)
    for attempt in range(3):
        r = http.get(f"{SL_BASE}{path}", params=p, timeout=30)
        if r.status_code == 429:
            time.sleep(2 ** attempt); continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"SL request failed: {path}")

def ghl_get(path, params=None):
    for attempt in range(3):
        r = http.get(f"{GHL_BASE}{path}", headers=get_ghl_headers(), params=params, timeout=30)
        if r.status_code == 429:
            time.sleep(2 ** attempt); continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"GHL request failed: {path}")


def get_sheets_service():
    creds = service_account.Credentials.from_service_account_file(
        GOOGLE_CREDS_PATH, scopes=SHEETS_SCOPE
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)

def read_config(sheets):
    try:
        result = sheets.spreadsheets().values().get(
            spreadsheetId=GOOGLE_SHEET_ID, range="Config!A4:B11",
        ).execute()
        cfg = {}
        for row in result.get("values", []):
            if len(row) >= 2: cfg[row[0]] = row[1]
            elif len(row) == 1: cfg[row[0]] = ""
        return cfg
    except Exception as e:
        log(f"  WARN Could not read Config: {e}")
        return {}

def write_config(sheets, updates):
    rows = [
        ["last_synced_at",       updates.get("last_synced_at", "")],
        ["smartlead_last_sync",  updates.get("smartlead_last_sync", "")],
        ["ghl_last_sync",        updates.get("ghl_last_sync", "")],
        ["sync_status",          updates.get("sync_status", "")],
        ["next_scheduled_sync",  updates.get("next_scheduled_sync", "")],
    ]
    sheets.spreadsheets().values().update(
        spreadsheetId=GOOGLE_SHEET_ID, range="Config!A5:B9",
        valueInputOption="USER_ENTERED", body={"values": rows},
    ).execute()

def read_existing_reply_keys(sheets):
    """V11: Reads Message IDs (column H) from existing rows for dedupe."""
    try:
        result = sheets.spreadsheets().values().get(
            spreadsheetId=GOOGLE_SHEET_ID,
            range="Raw SmartLead Replies!H2:H",
        ).execute()
        keys = set()
        for row in result.get("values", []):
            if row and row[0]:
                keys.add(row[0])
        return keys
    except Exception as e:
        log(f"  WARN Could not read existing message IDs: {e}")
        return set()

def read_existing_ghl_keys(sheets):
    try:
        result = sheets.spreadsheets().values().get(
            spreadsheetId=GOOGLE_SHEET_ID, range="Raw GHL Data!B2:B",
        ).execute()
        return {row[0].lower() for row in result.get("values", []) if row and row[0]}
    except Exception as e:
        log(f"  WARN Could not read existing GHL: {e}")
        return set()

def _sheets_call_with_retry(callable_fn, label="Sheets API call", max_attempts=5):
    """
    V12: Retry wrapper for any Sheets API call.
    Handles transient network errors (10053, 10054, timeout, token refresh failures)
    with exponential backoff.
    """
    import random
    last_err = None
    for attempt in range(1, max_attempts + 1):
        try:
            return callable_fn()
        except Exception as e:
            last_err = e
            err_str = str(e).lower()
            transient = any(m in err_str for m in [
                "connection", "10053", "10054", "timeout", "timed out",
                "aborted", "reset", "refresh", "ssl", "broken pipe"
            ])
            if not transient or attempt == max_attempts:
                raise
            wait = (2 ** attempt) + random.uniform(0, 1)
            log(f"  WARN {label} failed (attempt {attempt}/{max_attempts}): {e}")
            log(f"    Retrying in {wait:.1f}s...")
            time.sleep(wait)
    raise last_err

def update_range(sheets, range_str, rows):
    if not rows: return
    _sheets_call_with_retry(
        lambda: sheets.spreadsheets().values().update(
            spreadsheetId=GOOGLE_SHEET_ID, range=range_str,
            valueInputOption="USER_ENTERED", body={"values": rows},
        ).execute(),
        label=f"update {range_str}",
    )

def clear_range(sheets, range_str):
    _sheets_call_with_retry(
        lambda: sheets.spreadsheets().values().clear(
            spreadsheetId=GOOGLE_SHEET_ID, range=range_str,
        ).execute(),
        label=f"clear {range_str}",
    )

def append_rows(sheets, tab_name, rows):
    if not rows: return
    _sheets_call_with_retry(
        lambda: sheets.spreadsheets().values().append(
            spreadsheetId=GOOGLE_SHEET_ID, range=f"{tab_name}!A:A",
            valueInputOption="USER_ENTERED", insertDataOption="INSERT_ROWS",
            body={"values": rows},
        ).execute(),
        label=f"append to {tab_name}",
    )


# === SmartLead pulls ===
def install_stats_formulas(sheets, max_row):
    """
    Installs formulas for columns E, G, H in Raw SmartLead Stats.
      E: Reply Rate            = D/C  (2 decimal %)
      G: Positive Reply Rate   = F/D  (positive / total replies, 2 decimal %)
      H: Positive Reply Rate   = F/C  (positive / total sent, 4 decimal %)
    """
    e_rows = []
    g_rows = []
    h_rows = []
    for row_num in range(2, max_row + 2):
        e_rows.append([f"=IF(OR(A{row_num}=\"\",C{row_num}=0),\"\",D{row_num}/C{row_num})"])
        g_rows.append([f"=IF(OR(A{row_num}=\"\",D{row_num}=0),\"\",F{row_num}/D{row_num})"])
        h_rows.append([f"=IF(OR(A{row_num}=\"\",C{row_num}=0),\"\",F{row_num}/C{row_num})"])

    update_range(sheets, f"Raw SmartLead Stats!E2:E{max_row + 1}", e_rows)
    update_range(sheets, f"Raw SmartLead Stats!G2:G{max_row + 1}", g_rows)
    update_range(sheets, f"Raw SmartLead Stats!H2:H{max_row + 1}", h_rows)

    # Format percentages
    try:
        spreadsheet = sheets.spreadsheets().get(spreadsheetId=GOOGLE_SHEET_ID).execute()
        stats_sheet_id = None
        for s in spreadsheet["sheets"]:
            if s["properties"]["title"] == "Raw SmartLead Stats":
                stats_sheet_id = s["properties"]["sheetId"]
                break
        if stats_sheet_id is not None:
            pct_2dp = {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}}
            pct_4dp = {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.0000%"}}}
            sheets.spreadsheets().batchUpdate(
                spreadsheetId=GOOGLE_SHEET_ID,
                body={"requests": [
                    {"repeatCell": {"range": {"sheetId": stats_sheet_id, "startRowIndex": 1,
                        "endRowIndex": max_row + 1, "startColumnIndex": 4, "endColumnIndex": 5},
                        "cell": pct_2dp, "fields": "userEnteredFormat.numberFormat"}},
                    {"repeatCell": {"range": {"sheetId": stats_sheet_id, "startRowIndex": 1,
                        "endRowIndex": max_row + 1, "startColumnIndex": 6, "endColumnIndex": 7},
                        "cell": pct_2dp, "fields": "userEnteredFormat.numberFormat"}},
                    {"repeatCell": {"range": {"sheetId": stats_sheet_id, "startRowIndex": 1,
                        "endRowIndex": max_row + 1, "startColumnIndex": 7, "endColumnIndex": 8},
                        "cell": pct_4dp, "fields": "userEnteredFormat.numberFormat"}},
                ]},
            ).execute()
    except Exception as e:
        log(f"  WARN Could not format Stats percentages: {e}")

    log("  Installed Stats formulas (E, G, H) with formatting")


def fetch_smartlead_stats():
    """
    Returns three parallel lists for split-writing to Raw SmartLead Stats:
      - left_data:     [Campaign ID, Campaign Name, Total Sent, Total Replied]  -> cols A:D
      - positive_data: [Total Positive Replies]                                  -> col F
      - right_data:    [Total Bounced, Start Date, Sync Date]                    -> cols I:K

    Columns E, G, H are FORMULAS (installed by setup_stats_formulas.py):
      E: Reply Rate            = D/C
      G: Positive Reply Rate   = F/C  (from total sent)
      H: Positive Reply Rate   = F/D  (from total replies)
    """
    log("Fetching SmartLead campaign stats...")
    left_data = []     # A:D
    positive_data = [] # F
    right_data = []    # I:K
    sync_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for cid, cname in TRACKED_CAMPAIGNS:
        try:
            data = sl_get(f"/campaigns/{cid}/analytics")
            sent = int(data.get("sent_count", 0) or 0)
            replies = int(data.get("reply_count", 0) or 0)
            bounces = int(data.get("bounce_count", 0) or 0)
            start_date = (data.get("created_at") or "")[:10]

            lead_stats = data.get("campaign_lead_stats", {}) or {}
            interested = int(lead_stats.get("interested", 0) or 0)

            left_data.append([cid, cname, sent, replies])
            positive_data.append([interested])
            right_data.append([bounces, start_date, sync_date])

            log(f"  {cid}: sent={sent} replies={replies} interested={interested} bounces={bounces} started={start_date}")
            time.sleep(1.1)
        except Exception as e:
            log(f"  WARN Stats error for {cid}: {e}")
            left_data.append([cid, cname, 0, 0])
            positive_data.append([0])
            right_data.append([0, "", sync_date])
    return left_data, positive_data, right_data


def fetch_smartlead_categories():
    log("Fetching SmartLead category names...")
    try:
        data = sl_get("/leads/fetch-categories")
        if isinstance(data, list):
            return {str(c.get("id")): c.get("name", "") for c in data}
        return {}
    except Exception as e:
        log(f"  WARN Category fetch error: {e}")
        return {}


def fetch_smartlead_replies(existing_keys, is_first_run):
    """
    V11: One row per MESSAGE (not per lead).
    Fetches full conversation history for every categorized lead.
    Each message — both SENT and REPLY — becomes a row.
    Dedupe key is Message ID (unique RFC-822 ID from SmartLead).

    Output columns (12):
      A: Campaign ID
      B: Campaign Name
      C: Lead Email
      D: Lead Name
      E: Message Text
      F: Message Date
      G: Message Type (SENT or REPLY)
      H: Message ID
      I: SL Category
      J: Sequence Step
      K: Conversation ID (= Lead ID)
      L: Sync Date
    """
    mode = "FIRST RUN" if is_first_run else "incremental"
    log(f"Fetching SmartLead messages ({mode})...")
    rows = []
    sync_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    total_new_msgs, total_skipped_msgs = 0, 0
    total_new_convos, total_skipped_no_reply = 0, 0

    for cid, cname in TRACKED_CAMPAIGNS:
        log(f"  Campaign {cid}: scanning leads...")
        offset = 0
        limit = 100
        campaign_new_msgs = 0
        campaign_skipped_msgs = 0
        campaign_new_convos = 0
        campaign_skipped_no_reply = 0

        while True:
            try:
                data = sl_get(
                    f"/campaigns/{cid}/leads",
                    params={"offset": offset, "limit": limit},
                )
                time.sleep(1.1)
            except Exception as e:
                log(f"    WARN Lead fetch error: {e}")
                break

            leads = data.get("data", []) if isinstance(data, dict) else []
            if not leads: break

            for lead_wrapper in leads:
                category_id = lead_wrapper.get("lead_category_id")
                if not category_id: continue

                lead_obj = lead_wrapper.get("lead", {})
                lead_id = lead_obj.get("id")
                email = (lead_obj.get("email") or "").lower()
                if not lead_id or not email: continue

                lead_name = f"{lead_obj.get('first_name', '') or ''} {lead_obj.get('last_name', '') or ''}".strip()

                # Fetch full conversation history
                try:
                    msgs = sl_get(f"/campaigns/{cid}/leads/{lead_id}/message-history")
                    time.sleep(1.1)
                    history = msgs.get("history", []) if isinstance(msgs, dict) else (msgs if isinstance(msgs, list) else [])
                except Exception as e:
                    log(f"    WARN Message history error for lead {lead_id}: {e}")
                    continue

                # Skip leads that are categorized but have no actual REPLY
                # (handles auto-categorization of bounces, manual tagging, etc.)
                has_reply = any((m.get("type") or "").upper() == "REPLY" for m in history)
                if not has_reply:
                    campaign_skipped_no_reply += 1
                    continue

                # Generate rows for every message in the conversation
                lead_messages_added = 0
                for m in history:
                    msg_id = m.get("message_id") or ""
                    if not msg_id:
                        # Skip messages without an ID (can't dedupe reliably)
                        continue

                    # Dedupe by message ID
                    if msg_id in existing_keys:
                        campaign_skipped_msgs += 1
                        continue

                    msg_type = (m.get("type") or "").upper()
                    msg_text = strip_html(m.get("email_body") or m.get("body") or "")[:2000]
                    msg_date = (m.get("time") or m.get("sent_time") or "")[:10]
                    seq_step = m.get("email_seq_number", "")

                    rows.append([
                        cid,                    # A: Campaign ID
                        cname,                  # B: Campaign Name
                        email,                  # C: Lead Email
                        lead_name,              # D: Lead Name
                        msg_text,               # E: Message Text
                        msg_date,               # F: Message Date
                        msg_type,               # G: Message Type
                        msg_id,                 # H: Message ID
                        str(category_id),       # I: SL Category (translated to name later)
                        seq_step,               # J: Sequence Step
                        str(lead_id),           # K: Conversation ID
                        sync_date,              # L: Sync Date
                    ])
                    existing_keys.add(msg_id)
                    campaign_new_msgs += 1
                    lead_messages_added += 1

                if lead_messages_added > 0:
                    campaign_new_convos += 1

            if len(leads) < limit: break
            offset += limit

        log(f"    -> {cid}: {campaign_new_convos} new convos ({campaign_new_msgs} new msgs), "
            f"{campaign_skipped_msgs} dupe msgs skipped, "
            f"{campaign_skipped_no_reply} categorized-but-no-reply skipped")
        total_new_msgs += campaign_new_msgs
        total_skipped_msgs += campaign_skipped_msgs
        total_new_convos += campaign_new_convos
        total_skipped_no_reply += campaign_skipped_no_reply

    log(f"  Total: {total_new_convos} conversations, {total_new_msgs} messages, "
        f"{total_skipped_msgs} duplicates skipped")
    return rows


def fetch_smartlead_sequences():
    log("Fetching SmartLead sequences...")
    rows = []
    sync_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for cid, cname in TRACKED_CAMPAIGNS:
        try:
            seq_data = sl_get(f"/campaigns/{cid}/sequences")
            steps = seq_data if isinstance(seq_data, list) else seq_data.get("sequences", [])

            for step in steps:
                seq_number = step.get("seq_number", step.get("step", ""))
                delay = 0
                if isinstance(step.get("seq_delay_details"), dict):
                    delay = (step["seq_delay_details"].get("delayInDays")
                             or step["seq_delay_details"].get("delay_in_days", 0))

                variants = step.get("sequence_variants", [])
                if not variants:
                    rows.append([
                        cid, cname, seq_number,
                        step.get("subject", ""),
                        strip_html(step.get("email_body", ""))[:2000],
                        delay, "", sync_date,
                    ])
                else:
                    for v in variants:
                        if v.get("is_deleted"): continue
                        rows.append([
                            cid, cname, seq_number,
                            v.get("subject", ""),
                            strip_html(v.get("email_body", ""))[:2000],
                            delay,
                            v.get("variant_label", ""),
                            sync_date,
                        ])
            time.sleep(1.1)
        except Exception as e:
            log(f"  WARN Sequence error for {cid}: {e}")

    log(f"  Total sequence rows: {len(rows)}")
    return rows


# === GHL pulls ===
def fetch_ghl_pipeline_stages():
    log("Fetching GHL pipeline stages...")
    try:
        data = ghl_get("/opportunities/pipelines",
                       params={"locationId": GHL_LOCATION_ID})
        pipelines = data.get("pipelines", [])
        target = next((p for p in pipelines if p.get("id") == GHL_PIPELINE_ID), None)
        if not target:
            log("  WARN Pipeline not found")
            return {}
        return {s["id"]: s["name"] for s in target.get("stages", [])}
    except Exception as e:
        log(f"  WARN Pipeline fetch error: {e}")
        return {}


def extract_cf_by_id(custom_fields_list, field_id):
    for f in custom_fields_list or []:
        if f.get("id") == field_id:
            return f.get("fieldValue") or f.get("value") or ""
    return ""


def fetch_contact_custom_fields(contact_id):
    """
    V10: Custom fields aren't returned by /opportunities/search — they live
    on the contact record. Fetch them directly via /contacts/{id}.
    Returns the customFields list (possibly empty).
    """
    if not contact_id:
        return []
    try:
        data = ghl_get(f"/contacts/{contact_id}")
        contact = data.get("contact", data) if isinstance(data, dict) else {}
        return contact.get("customFields", []) or []
    except Exception as e:
        log(f"    WARN Contact fetch error for {contact_id}: {e}")
        return []


def fetch_ghl_opportunities(stage_id_to_name, existing_emails, is_first_run):
    mode = "FIRST RUN" if is_first_run else "incremental"
    log(f"Fetching GHL opportunities ({mode})...")
    rows = []
    sync_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    new_count, skipped_count = 0, 0

    page = 1
    per_page = 100
    while True:
        try:
            data = ghl_get("/opportunities/search", params={
                "location_id": GHL_LOCATION_ID,
                "pipeline_id": GHL_PIPELINE_ID,
                "limit": per_page,
                "page": page,
            })
        except Exception as e:
            log(f"  WARN Opportunity fetch error: {e}")
            break

        opps = data.get("opportunities", [])
        if not opps: break

        for opp in opps:
            contact = opp.get("contact", {}) or {}
            email = (contact.get("email") or "").lower()

            if email and email in existing_emails:
                skipped_count += 1
                continue

            current_stage = stage_id_to_name.get(opp.get("pipelineStageId", ""), "")

            # V10: Fetch custom fields from the contact record (not the opp)
            contact_id = opp.get("contactId") or contact.get("id")
            custom_fields = fetch_contact_custom_fields(contact_id)
            time.sleep(0.2)  # gentle pacing on the per-contact calls

            rows.append([
                opp.get("name") or f"{contact.get('firstName','')} {contact.get('lastName','')}".strip(),
                email,
                extract_cf_by_id(custom_fields, GHL_CF_CAMPAIGN_ID),
                extract_cf_by_id(custom_fields, GHL_CF_CALL_PROPOSED_AT),
                extract_cf_by_id(custom_fields, GHL_CF_DAY1_FOLLOWUP_AT),
                extract_cf_by_id(custom_fields, GHL_CF_DAY2_FOLLOWUP_AT),
                extract_cf_by_id(custom_fields, GHL_CF_BOOKED_CALL_AT),
                current_stage,
                sync_date,
            ])
            if email: existing_emails.add(email)
            new_count += 1

        if len(opps) < per_page: break
        page += 1
        time.sleep(0.5)

    log(f"  Total: {new_count} new, {skipped_count} duplicates skipped")
    return rows


# === Auto-dedupe ================================================
def dedupe_replies_tab(sheets):
    """
    V11: Reads all rows from Raw SmartLead Replies (12 columns), dedupes by
    Message ID (column H, index 7), rewrites the tab keeping first occurrence.
    """
    log("Deduping Raw SmartLead Replies...")
    try:
        result = sheets.spreadsheets().values().get(
            spreadsheetId=GOOGLE_SHEET_ID,
            range="Raw SmartLead Replies!A2:L",
        ).execute()
        rows = result.get("values", [])
        if not rows:
            log("  No data to dedupe")
            return

        seen = set()
        unique_rows = []
        duplicates = 0
        for row in rows:
            # Pad row to 12 columns if shorter
            while len(row) < 12:
                row.append("")
            msg_id = str(row[7]).strip() if row[7] else ""
            if not msg_id:
                # Keep rows without message ID (can't dedupe)
                unique_rows.append(row)
                continue
            if msg_id in seen:
                duplicates += 1
                continue
            seen.add(msg_id)
            unique_rows.append(row)

        if duplicates == 0:
            log(f"  No duplicates found ({len(unique_rows)} rows clean)")
            return

        log(f"  Found {duplicates} duplicates, rewriting with {len(unique_rows)} unique rows")
        clear_range(sheets, "Raw SmartLead Replies!A2:ZZ")
        update_range(sheets, "Raw SmartLead Replies!A2", unique_rows)
        log(f"  Replies deduped: {len(rows)} -> {len(unique_rows)}")
    except Exception as e:
        log(f"  WARN dedupe failed: {e}")


def dedupe_ghl_tab(sheets):
    """
    Read all rows from Raw GHL Data, keep one row per email, rewrite the tab.
    Keeps the FIRST occurrence.
    """
    log("Deduping Raw GHL Data...")
    try:
        result = sheets.spreadsheets().values().get(
            spreadsheetId=GOOGLE_SHEET_ID,
            range="Raw GHL Data!A2:I",
        ).execute()
        rows = result.get("values", [])
        if not rows:
            log("  No data to dedupe")
            return

        seen = set()
        unique_rows = []
        duplicates = 0
        for row in rows:
            while len(row) < 9:
                row.append("")
            email = str(row[1]).strip().lower() if row[1] else ""
            if not email:
                # Keep rows without email (they can't be deduped reliably)
                unique_rows.append(row)
                continue
            if email in seen:
                duplicates += 1
                continue
            seen.add(email)
            unique_rows.append(row)

        if duplicates == 0:
            log(f"  No duplicates found ({len(unique_rows)} rows clean)")
            return

        log(f"  Found {duplicates} duplicates, rewriting with {len(unique_rows)} unique rows")
        clear_range(sheets, "Raw GHL Data!A2:ZZ")
        update_range(sheets, "Raw GHL Data!A2", unique_rows)
        log(f"  GHL deduped: {len(rows)} -> {len(unique_rows)}")
    except Exception as e:
        log(f"  WARN dedupe failed: {e}")


# === Campaign Overview auto-population ===========================
def update_campaign_overview(sheets, tracked_campaigns):
    """
    Auto-populates Campaign Overview tab columns A-D with any tracked campaigns
    not already present. Sorts by Launch Date (newest first). Updates status
    for existing campaigns.
    """
    log("=== STAGE 6: Campaign Overview ===")
    
    try:
        result = sheets.spreadsheets().values().get(
            spreadsheetId=GOOGLE_SHEET_ID,
            range="Campaign Overview!A3:D500",
        ).execute()
        existing_rows = result.get("values", [])
    except Exception:
        existing_rows = []
    
    existing_ids = set()
    for row in existing_rows:
        if row and row[0]:
            existing_ids.add(str(row[0]).strip())
    
    log(f"  Existing campaigns in Overview: {len(existing_ids)}")
    
    new_campaigns = []
    for cid, cname in tracked_campaigns:
        if str(cid) not in existing_ids:
            new_campaigns.append((cid, cname))
    
    if not new_campaigns:
        log("  All tracked campaigns already in Overview, nothing to add")
        _refresh_overview_metadata(sheets, existing_rows, tracked_campaigns)
        return
    
    log(f"  Adding {len(new_campaigns)} new campaign(s) to Overview")
    
    new_rows = []
    for cid, cname in new_campaigns:
        try:
            data = sl_get(f"/campaigns/{cid}/analytics")
            status = data.get("status", "")
            launch_date = (data.get("created_at") or "")[:10]
            time.sleep(0.5)
        except Exception as e:
            log(f"    WARN Could not fetch analytics for {cid}: {e}")
            status = ""
            launch_date = ""
        
        new_rows.append([str(cid), launch_date, status, cname])
        log(f"    + {cid}: {cname} ({status}, {launch_date})")
    
    all_rows = []
    for row in existing_rows:
        while len(row) < 4:
            row.append("")
        all_rows.append(row)
    all_rows.extend(new_rows)
    
    all_rows.sort(key=lambda r: r[1] if len(r) > 1 and r[1] else "", reverse=True)
    
    if all_rows:
        clear_range(sheets, "Campaign Overview!A3:D500")
        update_range(sheets, f"Campaign Overview!A3:D{3 + len(all_rows) - 1}", all_rows)
    
    log(f"  Overview now has {len(all_rows)} campaigns (sorted by launch date)")


def _refresh_overview_metadata(sheets, existing_rows, tracked_campaigns):
    tracked_map = {}
    for cid, cname in tracked_campaigns:
        try:
            data = sl_get(f"/campaigns/{cid}/analytics")
            tracked_map[str(cid)] = {
                "status": data.get("status", ""),
                "launch_date": (data.get("created_at") or "")[:10],
                "name": cname,
            }
            time.sleep(0.5)
        except Exception:
            pass
    
    if not tracked_map:
        return
    
    updated = False
    for row in existing_rows:
        if not row or not row[0]:
            continue
        cid = str(row[0]).strip()
        if cid in tracked_map:
            info = tracked_map[cid]
            while len(row) < 4:
                row.append("")
            if row[1] != info["launch_date"] or row[2] != info["status"]:
                row[1] = info["launch_date"]
                row[2] = info["status"]
                updated = True
    
    if updated:
        clear_range(sheets, "Campaign Overview!A3:D500")
        update_range(sheets, f"Campaign Overview!A3:D{3 + len(existing_rows) - 1}", existing_rows)
        log("  Updated status/dates for existing campaigns")


# === Main ===
def main():
    global TRACKED_CAMPAIGNS

    # SmartLead + Google Sheet are required; GHL is optional
    missing = [k for k, v in {
        "SMARTLEAD_API_KEY": SMARTLEAD_API_KEY,
        "GOOGLE_SHEET_ID": GOOGLE_SHEET_ID,
    }.items() if not v]
    if missing:
        log(f"ERROR Missing required env vars: {missing}")
        sys.exit(1)

    ghl_enabled = bool(GHL_PRIVATE_TOKEN and GHL_LOCATION_ID and GHL_PIPELINE_ID)
    if not ghl_enabled:
        log("NOTE: GHL credentials not provided — skipping GHL stages")

    if not Path(GOOGLE_CREDS_PATH).exists():
        log(f"ERROR Service account JSON not found at {GOOGLE_CREDS_PATH}")
        sys.exit(1)

    start = datetime.now(timezone.utc)
    log(f"=== SYNC started for client: {CLIENT_NAME} ===")
    log(f"=== Started at {start.isoformat()} ===")

    # Discover campaigns based on CAMPAIGN_FILTER
    TRACKED_CAMPAIGNS = discover_filtered_campaigns()
    if not TRACKED_CAMPAIGNS:
        log("No campaigns found matching filter. Nothing to sync.")
        return

    log(f"=== Processing {len(TRACKED_CAMPAIGNS)} campaign(s) ===")

    sheets = get_sheets_service()
    config = read_config(sheets)
    last_sync = parse_iso(config.get("last_synced_at", ""))
    is_first_run = last_sync is None
    log(f"Mode: {'FIRST RUN' if is_first_run else 'INCREMENTAL'}")

    try:
        existing_reply_keys = set() if is_first_run else read_existing_reply_keys(sheets)
        existing_ghl_emails = set()
        if ghl_enabled and not is_first_run:
            existing_ghl_emails = read_existing_ghl_keys(sheets)
        if not is_first_run:
            log(f"  Loaded {len(existing_reply_keys)} reply keys" +
                (f", {len(existing_ghl_emails)} GHL emails" if ghl_enabled else "") +
                " for dedup")

        categories = fetch_smartlead_categories()
        log(f"  Loaded {len(categories)} category names")

        # ─── STAGE 1: Stats (fast, write immediately) ─────────────
        log("=== STAGE 1: SmartLead Stats ===")
        stats_left, stats_positive, stats_right = fetch_smartlead_stats()
        n_stats = len(stats_left)
        if n_stats > 0:
            clear_range(sheets, f"Raw SmartLead Stats!A2:D{n_stats + 50}")
            clear_range(sheets, f"Raw SmartLead Stats!F2:F{n_stats + 50}")
            clear_range(sheets, f"Raw SmartLead Stats!I2:K{n_stats + 50}")
            update_range(sheets, f"Raw SmartLead Stats!A2:D{n_stats + 1}", stats_left)
            update_range(sheets, f"Raw SmartLead Stats!F2:F{n_stats + 1}", stats_positive)
            update_range(sheets, f"Raw SmartLead Stats!I2:K{n_stats + 1}", stats_right)
            # Install formulas for E, G, H (every run to ensure they're always present)
            install_stats_formulas(sheets, n_stats + 50)
        log(f"  Wrote {n_stats} stats rows")

        # ─── STAGE 2: Sequences (fast, write immediately) ─────────
        log("=== STAGE 2: SmartLead Sequences ===")
        sl_sequences = fetch_smartlead_sequences()
        clear_range(sheets, "Raw SmartLead Sequences!A2:ZZ")
        update_range(sheets, "Raw SmartLead Sequences!A2", sl_sequences)
        log(f"  Wrote {len(sl_sequences)} sequence rows")

        # ─── STAGE 3: Replies (slow, write as we go per campaign) ─
        log("=== STAGE 3: SmartLead Replies (this is the slow part) ===")
        sl_replies = fetch_smartlead_replies(existing_reply_keys, is_first_run)
        if categories:
            for row in sl_replies:
                cat_id = str(row[8])
                row[8] = categories.get(cat_id, cat_id)

        if is_first_run:
            clear_range(sheets, "Raw SmartLead Replies!A2:ZZ")
            update_range(sheets, "Raw SmartLead Replies!A2", sl_replies)
        else:
            append_rows(sheets, "Raw SmartLead Replies", sl_replies)
        log(f"  Wrote {len(sl_replies)} reply rows")

        # Save progress marker so if GHL section fails, replies are preserved
        partial_now = datetime.now(timezone.utc).isoformat()
        write_config(sheets, {
            "last_synced_at": partial_now,
            "smartlead_last_sync": partial_now,
            "ghl_last_sync": config.get("ghl_last_sync", ""),
            "sync_status": f"SmartLead done, fetching GHL...",
            "next_scheduled_sync": "",
        })

        # ─── STAGE 4: GHL (skip if no GHL credentials) ─────────────
        ghl_opps = []
        if ghl_enabled:
            log("=== STAGE 4: GHL Data ===")
            stages = fetch_ghl_pipeline_stages()
            ghl_opps = fetch_ghl_opportunities(stages, existing_ghl_emails, is_first_run)

            if is_first_run:
                clear_range(sheets, "Raw GHL Data!A2:ZZ")
                update_range(sheets, "Raw GHL Data!A2", ghl_opps)
            else:
                append_rows(sheets, "Raw GHL Data", ghl_opps)
            log(f"  Wrote {len(ghl_opps)} GHL rows")
        else:
            log("=== STAGE 4: GHL Data — SKIPPED (no credentials) ===")

        # ─── STAGE 5: Dedupe ──────────────────────────────────────
        log("=== STAGE 5: Dedupe ===")
        dedupe_replies_tab(sheets)
        if ghl_enabled:
            dedupe_ghl_tab(sheets)

        # ─── STAGE 6: Campaign Overview auto-populate ─────────────
        update_campaign_overview(sheets, TRACKED_CAMPAIGNS)

        now = datetime.now(timezone.utc).isoformat()
        write_config(sheets, {
            "last_synced_at": now,
            "smartlead_last_sync": now,
            "ghl_last_sync": now,
            "sync_status": f"Success - {len(sl_replies)} new replies, {len(ghl_opps)} new opps",
            "next_scheduled_sync": (datetime.now(timezone.utc) + timedelta(days=1)).isoformat(),
        })

        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        log(f"DONE Sync complete in {elapsed:.1f}s ({elapsed/60:.1f} min)")

    except Exception as e:
        log(f"ERROR Sync failed: {e}")
        try:
            write_config(sheets, {
                "sync_status": f"Failed: {str(e)[:200]}",
                "last_synced_at": config.get("last_synced_at", ""),
            })
        except Exception:
            pass
        raise


if __name__ == "__main__":
    main()
