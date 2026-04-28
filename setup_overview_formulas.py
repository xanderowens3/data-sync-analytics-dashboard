"""
One-Time Formula Setup for Campaign Overview tab (v2)
======================================================

Installs all formulas in the Campaign Overview tab.

V2 FIXES:
- Formulas start at row 3 (row 1 is title, row 2 is column headers)
- Clears any leftover formulas/formatting from row 2
- Formats F as plain integer (was previously formatted as %)
- Only E, G, L get percentage formatting

Column layout (1-indexed):
    A: Campaign ID            <- you fill in manually
    B: Campaign Name          <- you fill in manually
    C: Total Sent             <- FORMULA (VLOOKUP from Stats)
    D: Total Replied          <- FORMULA (VLOOKUP from Stats — matches SL dashboard)
    E: Reply Rate             <- FORMULA (D/C, % format)
    F: Total Positive Replies <- FORMULA (VLOOKUP from Stats — matches SL dashboard, INTEGER format)
    G: Positive Reply Rate    <- FORMULA (F/D, % format)
    H: Call Proposed          <- FORMULA (count GHL contacts with Call Proposed timestamp)
    I: Day 1 Follow-Up        <- FORMULA
    J: Day 2 Follow-Up        <- FORMULA
    K: Calls Booked           <- FORMULA
    L: Book Rate              <- FORMULA (K/D, % format)

Run ONCE after column structure is set. Safe to re-run.

Usage: python setup_overview_formulas.py
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build

load_dotenv()

GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_CREDS_PATH = os.getenv("GOOGLE_CREDS_PATH", "./service-account.json")
SHEETS_SCOPE = ["https://www.googleapis.com/auth/spreadsheets"]

# Formulas start at row 3 (row 1 = title, row 2 = headers)
START_ROW = 3
END_ROW = 252  # 250 campaign rows total

if not GOOGLE_SHEET_ID:
    print("ERROR: GOOGLE_SHEET_ID missing from .env"); sys.exit(1)
if not Path(GOOGLE_CREDS_PATH).exists():
    print(f"ERROR: Service account JSON not found at {GOOGLE_CREDS_PATH}"); sys.exit(1)

creds = service_account.Credentials.from_service_account_file(
    GOOGLE_CREDS_PATH, scopes=SHEETS_SCOPE
)
sheets = build("sheets", "v4", credentials=creds, cache_discovery=False)

# Step 1: Clear any leftover bogus formulas from row 2 (between title and data)
print("Clearing any leftover content from row 2 columns C-L...")
sheets.spreadsheets().values().clear(
    spreadsheetId=GOOGLE_SHEET_ID,
    range="Campaign Overview!C2:L2",
).execute()

# Step 2: Build formula rows starting at START_ROW
print(f"Building formulas for rows {START_ROW} through {END_ROW}...")
formula_rows = []
for row_num in range(START_ROW, END_ROW + 1):
    formula_rows.append([
        # C: Total Sent
        f"=IF(A{row_num}=\"\",\"\",IFERROR(VLOOKUP(A{row_num},'Raw SmartLead Stats'!A:J,3,FALSE),0))",
        # D: Total Replied
        f"=IF(A{row_num}=\"\",\"\",IFERROR(VLOOKUP(A{row_num},'Raw SmartLead Stats'!A:J,4,FALSE),0))",
        # E: Reply Rate
        f"=IF(OR(A{row_num}=\"\",C{row_num}=0),\"\",D{row_num}/C{row_num})",
        # F: Total Positive Replies
        f"=IF(A{row_num}=\"\",\"\",IFERROR(VLOOKUP(A{row_num},'Raw SmartLead Stats'!A:J,6,FALSE),0))",
        # G: Positive Reply Rate
        f"=IF(OR(A{row_num}=\"\",D{row_num}=0),\"\",F{row_num}/D{row_num})",
        # H: Call Proposed
        f"=IF(A{row_num}=\"\",\"\",COUNTIFS('Raw GHL Data'!C:C,A{row_num},'Raw GHL Data'!D:D,\"<>\"))",
        # I: Day 1 Follow-Up
        f"=IF(A{row_num}=\"\",\"\",COUNTIFS('Raw GHL Data'!C:C,A{row_num},'Raw GHL Data'!E:E,\"<>\"))",
        # J: Day 2 Follow-Up
        f"=IF(A{row_num}=\"\",\"\",COUNTIFS('Raw GHL Data'!C:C,A{row_num},'Raw GHL Data'!F:F,\"<>\"))",
        # K: Calls Booked
        f"=IF(A{row_num}=\"\",\"\",COUNTIFS('Raw GHL Data'!C:C,A{row_num},'Raw GHL Data'!G:G,\"<>\"))",
        # L: Book Rate
        f"=IF(OR(A{row_num}=\"\",D{row_num}=0),\"\",K{row_num}/D{row_num})",
    ])

print(f"Writing formulas to C{START_ROW}:L{END_ROW}...")
sheets.spreadsheets().values().update(
    spreadsheetId=GOOGLE_SHEET_ID,
    range=f"Campaign Overview!C{START_ROW}:L{END_ROW}",
    valueInputOption="USER_ENTERED",
    body={"values": formula_rows},
).execute()

# Step 3: Apply formats
print("Setting cell formats...")

spreadsheet = sheets.spreadsheets().get(spreadsheetId=GOOGLE_SHEET_ID).execute()
overview_sheet_id = None
for s in spreadsheet["sheets"]:
    if s["properties"]["title"] == "Campaign Overview":
        overview_sheet_id = s["properties"]["sheetId"]
        break

if overview_sheet_id is not None:
    pct_format = {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}}}
    int_format = {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}}}

    def cell_range(start_col_idx, end_col_idx):
        return {
            "sheetId": overview_sheet_id,
            "startRowIndex": START_ROW - 1,  # 0-indexed for the API
            "endRowIndex": END_ROW,
            "startColumnIndex": start_col_idx,
            "endColumnIndex": end_col_idx,
        }

    requests_body = {
        "requests": [
            # E: Reply Rate (percent)
            {"repeatCell": {"range": cell_range(4, 5), "cell": pct_format,
                            "fields": "userEnteredFormat.numberFormat"}},
            # F: Total Positive Replies (integer — overrides any old % formatting)
            {"repeatCell": {"range": cell_range(5, 6), "cell": int_format,
                            "fields": "userEnteredFormat.numberFormat"}},
            # G: Positive Reply Rate (percent)
            {"repeatCell": {"range": cell_range(6, 7), "cell": pct_format,
                            "fields": "userEnteredFormat.numberFormat"}},
            # L: Book Rate (percent)
            {"repeatCell": {"range": cell_range(11, 12), "cell": pct_format,
                            "fields": "userEnteredFormat.numberFormat"}},
        ]
    }
    sheets.spreadsheets().batchUpdate(
        spreadsheetId=GOOGLE_SHEET_ID, body=requests_body
    ).execute()
    print("Done. Formulas installed and formats applied.")
else:
    print("WARN: Could not find 'Campaign Overview' sheet ID for formatting.")
    print("  Manually format E, G, L as %, and F as integer.")

print("\nNote: Fill in columns A (Campaign ID) and B (Campaign Name) for each campaign.")
