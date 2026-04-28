"""
One-Time Formula Setup for Raw SmartLead Stats
================================================

Installs formulas in columns E, G, H of the Raw SmartLead Stats tab.

Column layout (11 columns):
    A: Campaign ID                           <- script
    B: Campaign Name                         <- script
    C: Total Sent                            <- script
    D: Total Replied                         <- script
    E: Reply Rate                            <- FORMULA (D/C)
    F: Total Positive Replies                <- script
    G: Positive Reply Rate (from total sent) <- FORMULA (F/C)
    H: Positive Reply Rate (from total replies) <- FORMULA (F/D)
    I: Total Bounced                         <- script
    J: Start Date                            <- script
    K: Sync Date                             <- script

Usage: python setup_stats_formulas.py --client <client-name>
"""

import os
import sys
import argparse
from pathlib import Path
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build

parser = argparse.ArgumentParser()
parser.add_argument("--client", required=True, help="Client folder name under clients/")
args = parser.parse_args()

client_dir = Path(__file__).parent / "clients" / args.client
if not client_dir.exists():
    print(f"ERROR: Client folder not found: {client_dir}")
    sys.exit(1)

load_dotenv(dotenv_path=client_dir / ".env")

GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
creds_path = str(client_dir / "service-account.json")
SHEETS_SCOPE = ["https://www.googleapis.com/auth/spreadsheets"]

MAX_ROWS = 250

if not GOOGLE_SHEET_ID:
    print("ERROR: GOOGLE_SHEET_ID missing from .env"); sys.exit(1)
if not Path(creds_path).exists():
    print(f"ERROR: {creds_path} not found"); sys.exit(1)

creds = service_account.Credentials.from_service_account_file(
    creds_path, scopes=SHEETS_SCOPE
)
sheets = build("sheets", "v4", credentials=creds, cache_discovery=False)

print("Building formula rows for E, G, H...")

# E: Reply Rate
e_rows = []
for row_num in range(2, MAX_ROWS + 2):
    e_rows.append([
        f"=IF(OR(A{row_num}=\"\",C{row_num}=0),\"\",D{row_num}/C{row_num})",
    ])

# G: Positive Reply Rate from Total Sent
g_rows = []
for row_num in range(2, MAX_ROWS + 2):
    g_rows.append([
        f"=IF(OR(A{row_num}=\"\",C{row_num}=0),\"\",F{row_num}/C{row_num})",
    ])

# H: Positive Reply Rate from Total Replies
h_rows = []
for row_num in range(2, MAX_ROWS + 2):
    h_rows.append([
        f"=IF(OR(A{row_num}=\"\",D{row_num}=0),\"\",F{row_num}/D{row_num})",
    ])

print(f"Writing E formulas...")
sheets.spreadsheets().values().update(
    spreadsheetId=GOOGLE_SHEET_ID,
    range=f"Raw SmartLead Stats!E2:E{MAX_ROWS + 1}",
    valueInputOption="USER_ENTERED",
    body={"values": e_rows},
).execute()

print(f"Writing G formulas...")
sheets.spreadsheets().values().update(
    spreadsheetId=GOOGLE_SHEET_ID,
    range=f"Raw SmartLead Stats!G2:G{MAX_ROWS + 1}",
    valueInputOption="USER_ENTERED",
    body={"values": g_rows},
).execute()

print(f"Writing H formulas...")
sheets.spreadsheets().values().update(
    spreadsheetId=GOOGLE_SHEET_ID,
    range=f"Raw SmartLead Stats!H2:H{MAX_ROWS + 1}",
    valueInputOption="USER_ENTERED",
    body={"values": h_rows},
).execute()

print("Formatting E, G, H as percentages...")

spreadsheet = sheets.spreadsheets().get(spreadsheetId=GOOGLE_SHEET_ID).execute()
stats_sheet_id = None
for s in spreadsheet["sheets"]:
    if s["properties"]["title"] == "Raw SmartLead Stats":
        stats_sheet_id = s["properties"]["sheetId"]
        break

if stats_sheet_id is not None:
    pct_format = {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}}}
    sheets.spreadsheets().batchUpdate(
        spreadsheetId=GOOGLE_SHEET_ID,
        body={"requests": [
            {"repeatCell": {"range": {"sheetId": stats_sheet_id, "startRowIndex": 1,
                "endRowIndex": MAX_ROWS + 1, "startColumnIndex": 4, "endColumnIndex": 5},
                "cell": pct_format, "fields": "userEnteredFormat.numberFormat"}},
            {"repeatCell": {"range": {"sheetId": stats_sheet_id, "startRowIndex": 1,
                "endRowIndex": MAX_ROWS + 1, "startColumnIndex": 6, "endColumnIndex": 7},
                "cell": pct_format, "fields": "userEnteredFormat.numberFormat"}},
            {"repeatCell": {"range": {"sheetId": stats_sheet_id, "startRowIndex": 1,
                "endRowIndex": MAX_ROWS + 1, "startColumnIndex": 7, "endColumnIndex": 8},
                "cell": pct_format, "fields": "userEnteredFormat.numberFormat"}},
        ]},
    ).execute()
    print("Done. Formulas installed, E/G/H formatted as percentages.")
else:
    print("WARN: Could not find 'Raw SmartLead Stats' sheet ID. Format E, G, H manually.")

print("\nNext step: run sync.py or update.py")
