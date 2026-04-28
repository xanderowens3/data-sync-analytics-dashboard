"""
Inspects the message-history response for one replied lead so we can find
the correct field name for sequence step.

Pages through up to 5000 leads to find a replied one.

Usage: python debug_message_history.py
"""

import os
import json
import time
import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("SMARTLEAD_API_KEY")
BASE = "https://server.smartlead.ai/api/v1"
CAMPAIGN_ID = 3134980  # Bathroom

print(f"Step 1: Searching for a replied lead in campaign {CAMPAIGN_ID}…")
print("(Paging through leads 100 at a time — may take a minute)")

replied_lead = None
offset = 0
limit = 100
max_pages = 50  # Up to 5000 leads

for page in range(max_pages):
    r = requests.get(
        f"{BASE}/campaigns/{CAMPAIGN_ID}/leads",
        params={"api_key": API_KEY, "offset": offset, "limit": limit},
        timeout=30,
    )
    if r.status_code != 200:
        print(f"  Error: {r.status_code}")
        break

    data = r.json()
    leads = data.get("data", []) if isinstance(data, dict) else []
    if not leads:
        print(f"  No more leads at offset {offset}")
        break

    for lw in leads:
        if lw.get("lead_category_id"):
            replied_lead = lw
            break

    if replied_lead:
        print(f"  Found at offset {offset}!")
        break

    offset += limit
    print(f"  Scanned {offset} leads, none replied yet…")
    time.sleep(1.1)

if not replied_lead:
    print("\n❌ No replied lead found. Try a different campaign.")
    exit(0)

lead_id = replied_lead["lead"]["id"]
print(f"\n  Replied lead: {replied_lead['lead'].get('email')} (id: {lead_id})")
print(f"  Category ID: {replied_lead.get('lead_category_id')}")

print(f"\nStep 2: Fetching message history for lead {lead_id}…")
r = requests.get(
    f"{BASE}/campaigns/{CAMPAIGN_ID}/leads/{lead_id}/message-history",
    params={"api_key": API_KEY},
    timeout=30,
)
print(f"  Status: {r.status_code}")

if r.status_code == 200:
    data = r.json()
    print(f"\nTop-level type: {type(data).__name__}")
    if isinstance(data, dict):
        print(f"Top-level keys: {list(data.keys())}")
        history = data.get("history", [])
    else:
        history = data

    print(f"\nNumber of messages in history: {len(history)}")
    if history:
        print(f"\nKeys in first message: {list(history[0].keys())}")

        # Show all messages so we can see SENT vs REPLY structure
        for i, m in enumerate(history):
            mtype = m.get("type") or m.get("message_type") or ""
            print(f"\n--- Message {i}: type={mtype} ---")
            # Print compact version: just keys + values up to 200 chars
            for k, v in m.items():
                v_str = str(v)
                if len(v_str) > 200:
                    v_str = v_str[:200] + "…"
                print(f"  {k}: {v_str}")
            if i >= 4:
                print(f"\n  …({len(history) - 5} more messages, truncated)")
                break
else:
    print(f"  Response: {r.text[:500]}")

print("\n" + "=" * 70)
print("Copy this entire output and paste back to me.")
