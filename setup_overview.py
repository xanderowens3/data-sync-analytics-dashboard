"""
Setup Campaign Overview tab (v3)
=================================

Column layout (row 1 = title, row 2 = headers, data starts row 3):
    A: Campaign ID            <- script
    B: Campaign Launch Date   <- script
    C: Campaign Status        <- script
    D: Campaign Name          <- script
    E: Total Sent             <- FORMULA (VLOOKUP from Stats)
    F: Total Replies          <- FORMULA (VLOOKUP from Stats)
    G: Reply Rate             <- FORMULA (F/E)
    H: Total Positive Replies <- FORMULA (VLOOKUP from Stats)
    I: Positive Reply Rate    <- FORMULA (H/F) — % of replies that were positive
    J: Actual Positive Reply Rate <- FORMULA (H/E) — % of total sends that got positive reply
    K: Emails Sent per Positive   <- FORMULA (E/H) — how many emails per positive reply
    L: Calls Proposed         <- FORMULA (COUNTIFS from GHL Data)
    M: Day 1 Follow-Ups       <- FORMULA
    N: Day 2 Follow-Ups       <- FORMULA
    O: Calls Booked           <- FORMULA
    P: Booking Rate           <- FORMULA (O/H)

Usage: python setup_overview.py
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
import requests

load_dotenv()

SMARTLEAD_API_KEY = os.getenv("SMARTLEAD_API_KEY")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_CREDS_PATH = os.getenv("GOOGLE_CREDS_PATH", "./service-account.json")
SHEETS_SCOPE = ["https://www.googleapis.com/auth/spreadsheets"]
SL_BASE = "https://server.smartlead.ai/api/v1"

MAX_FORMULA_ROWS = 250
START_ROW = 3

if not SMARTLEAD_API_KEY:
    print("ERROR: SMARTLEAD_API_KEY missing"); sys.exit(1)
if not GOOGLE_SHEET_ID:
    print("ERROR: GOOGLE_SHEET_ID missing"); sys.exit(1)
if not Path(GOOGLE_CREDS_PATH).exists():
    print(f"ERROR: {GOOGLE_CREDS_PATH} not found"); sys.exit(1)

creds = service_account.Credentials.from_service_account_file(
    GOOGLE_CREDS_PATH, scopes=SHEETS_SCOPE
)
sheets = build("sheets", "v4", credentials=creds, cache_discovery=False)


# Step 1: Fetch campaigns
print("Fetching all campaigns from SmartLead...")
r = requests.get(f"{SL_BASE}/campaigns", params={"api_key": SMARTLEAD_API_KEY}, timeout=30)
r.raise_for_status()
data = r.json()
campaigns = data if isinstance(data, list) else data.get("data", [])
print(f"  Total campaigns: {len(campaigns)}")

campaigns_sorted = sorted(campaigns, key=lambda c: c.get("created_at", ""), reverse=True)
top_10 = campaigns_sorted[:10]

print(f"\nTop 10 most recent campaigns:")
for c in top_10:
    print(f"  {c.get('id')} | {c.get('status')} | {c.get('created_at', '')[:10]} | {c.get('name')}")


# Step 2: Build data rows (A-D)
data_rows = []
for c in top_10:
    data_rows.append([
        c.get("id", ""),
        (c.get("created_at") or "")[:10],
        c.get("status", ""),
        c.get("name", ""),
    ])


# Step 3: Clear and write data
print("\nClearing existing Campaign Overview data...")
sheets.spreadsheets().values().clear(
    spreadsheetId=GOOGLE_SHEET_ID,
    range="Campaign Overview!A3:P500",
).execute()

print("Writing campaign data...")
sheets.spreadsheets().values().update(
    spreadsheetId=GOOGLE_SHEET_ID,
    range=f"Campaign Overview!A3:D{START_ROW + len(data_rows) - 1}",
    valueInputOption="USER_ENTERED",
    body={"values": data_rows},
).execute()


# Step 4: Install formulas E-P
print(f"Installing formulas...")

# GHL Data column references (after v10 restructure):
#   C = Campaign ID
#   D = Entered Call Proposed At
#   E = Entered Day 1 Follow-Up At
#   F = Entered Day 2 Follow-Up At
#   G = Entered Call Booked At

formula_rows = []
for row_num in range(START_ROW, START_ROW + MAX_FORMULA_ROWS):
    formula_rows.append([
        # E: Total Sent
        f"=IF(A{row_num}=\"\",\"\",IFERROR(VLOOKUP(A{row_num},'Raw SmartLead Stats'!A:J,3,FALSE),0))",
        # F: Total Replies
        f"=IF(A{row_num}=\"\",\"\",IFERROR(VLOOKUP(A{row_num},'Raw SmartLead Stats'!A:J,4,FALSE),0))",
        # G: Reply Rate (F/E)
        f"=IF(OR(A{row_num}=\"\",E{row_num}=0),\"\",F{row_num}/E{row_num})",
        # H: Total Positive Replies
        f"=IF(A{row_num}=\"\",\"\",IFERROR(VLOOKUP(A{row_num},'Raw SmartLead Stats'!A:J,6,FALSE),0))",
        # I: Positive Reply Rate (H/F — what % of replies were positive)
        f"=IF(OR(A{row_num}=\"\",F{row_num}=0),\"\",H{row_num}/F{row_num})",
        # J: Actual Positive Reply Rate (H/E — what % of total sends got a positive reply)
        f"=IF(OR(A{row_num}=\"\",E{row_num}=0),\"\",H{row_num}/E{row_num})",
        # K: Emails Sent per Positive (E/H — how many emails until a positive reply)
        f"=IF(OR(A{row_num}=\"\",H{row_num}=0),\"\",E{row_num}/H{row_num})",
        # L: Calls Proposed
        f"=IF(A{row_num}=\"\",\"\",COUNTIFS('Raw GHL Data'!C:C,A{row_num},'Raw GHL Data'!D:D,\"<>\"))",
        # M: Day 1 Follow-Ups
        f"=IF(A{row_num}=\"\",\"\",COUNTIFS('Raw GHL Data'!C:C,A{row_num},'Raw GHL Data'!E:E,\"<>\"))",
        # N: Day 2 Follow-Ups
        f"=IF(A{row_num}=\"\",\"\",COUNTIFS('Raw GHL Data'!C:C,A{row_num},'Raw GHL Data'!F:F,\"<>\"))",
        # O: Calls Booked
        f"=IF(A{row_num}=\"\",\"\",COUNTIFS('Raw GHL Data'!C:C,A{row_num},'Raw GHL Data'!G:G,\"<>\"))",
        # P: Booking Rate (O/H — Calls Booked / Total Positive Replies)
        f"=IF(OR(A{row_num}=\"\",H{row_num}=0),\"\",O{row_num}/H{row_num})",
    ])

sheets.spreadsheets().values().update(
    spreadsheetId=GOOGLE_SHEET_ID,
    range=f"Campaign Overview!E{START_ROW}:P{START_ROW + MAX_FORMULA_ROWS - 1}",
    valueInputOption="USER_ENTERED",
    body={"values": formula_rows},
).execute()


# Step 5: Format
print("Applying formatting...")

spreadsheet = sheets.spreadsheets().get(spreadsheetId=GOOGLE_SHEET_ID).execute()
overview_sheet_id = None
for s in spreadsheet["sheets"]:
    if s["properties"]["title"] == "Campaign Overview":
        overview_sheet_id = s["properties"]["sheetId"]
        break

if overview_sheet_id is not None:
    pct_format = {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}}}
    int_format = {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}}}
    end_row = START_ROW + MAX_FORMULA_ROWS - 1

    border_style = {
        "style": "SOLID",
        "width": 1,
        "colorStyle": {"rgbColor": {"red": 0.75, "green": 0.75, "blue": 0.75}},
    }

    def cell_range(start_col, end_col):
        return {
            "sheetId": overview_sheet_id,
            "startRowIndex": START_ROW - 1,
            "endRowIndex": end_row,
            "startColumnIndex": start_col,
            "endColumnIndex": end_col,
        }

    batch_requests = [
        # Arial 10pt, centered for all data cells (A-P)
        {
            "repeatCell": {
                "range": cell_range(0, 16),
                "cell": {
                    "userEnteredFormat": {
                        "textFormat": {"fontFamily": "Arial", "fontSize": 10},
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                    }
                },
                "fields": "userEnteredFormat(textFormat,horizontalAlignment,verticalAlignment)",
            }
        },
        # Borders
        {
            "updateBorders": {
                "range": cell_range(0, 16),
                "top": border_style, "bottom": border_style,
                "left": border_style, "right": border_style,
                "innerHorizontal": border_style, "innerVertical": border_style,
            }
        },
        # Column D: wrap text
        {
            "repeatCell": {
                "range": cell_range(3, 4),
                "cell": {"userEnteredFormat": {"wrapStrategy": "CLIP", "horizontalAlignment": "LEFT"}},
                "fields": "userEnteredFormat(wrapStrategy,horizontalAlignment)",
            }
        },
        # G: Reply Rate (%)
        {"repeatCell": {"range": cell_range(6, 7), "cell": pct_format,
                        "fields": "userEnteredFormat.numberFormat"}},
        # H: Total Positive Replies (integer)
        {"repeatCell": {"range": cell_range(7, 8), "cell": int_format,
                        "fields": "userEnteredFormat.numberFormat"}},
        # I: Positive Reply Rate (%)
        {"repeatCell": {"range": cell_range(8, 9), "cell": pct_format,
                        "fields": "userEnteredFormat.numberFormat"}},
        # J: Actual Positive Reply Rate (%)
        {"repeatCell": {"range": cell_range(9, 10), "cell": pct_format,
                        "fields": "userEnteredFormat.numberFormat"}},
        # K: Emails Sent per Positive (integer)
        {"repeatCell": {"range": cell_range(10, 11), "cell": int_format,
                        "fields": "userEnteredFormat.numberFormat"}},
        # P: Booking Rate (%)
        {"repeatCell": {"range": cell_range(15, 16), "cell": pct_format,
                        "fields": "userEnteredFormat.numberFormat"}},
    ]

    sheets.spreadsheets().batchUpdate(
        spreadsheetId=GOOGLE_SHEET_ID,
        body={"requests": batch_requests},
    ).execute()
    print("  Formatting applied.")
else:
    print("  WARN: Could not find Campaign Overview sheet ID.")

print(f"\nDone. {len(data_rows)} campaigns loaded, formulas installed (E-P), formatting applied.")
