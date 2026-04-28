"""
SmartLead Sequence Diagnostic
==============================
Inspects the /sequences endpoint to see the actual response shape,
so we can fix the subject line / body copy extraction in sync.py.

Usage: python debug_sequences.py
"""

import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("SMARTLEAD_API_KEY")
BASE = "https://server.smartlead.ai/api/v1"
CAMPAIGN_ID = 3134980  # Bathroom Renovations

if not API_KEY:
    print("❌ SMARTLEAD_API_KEY not found in .env")
    exit(1)

print(f"Fetching sequences for campaign {CAMPAIGN_ID}…")
print("=" * 70)

r = requests.get(
    f"{BASE}/campaigns/{CAMPAIGN_ID}/sequences",
    params={"api_key": API_KEY},
    timeout=30,
)
print(f"Status: {r.status_code}")

if r.status_code == 200:
    data = r.json()
    print(f"\nTop-level type: {type(data).__name__}")
    if isinstance(data, list):
        print(f"Number of sequence steps: {len(data)}")
        if data:
            print(f"\nKeys in first step: {list(data[0].keys())}")
            print(f"\nFull first step:")
            print(json.dumps(data[0], indent=2)[:3000])
    elif isinstance(data, dict):
        print(f"Keys: {list(data.keys())}")
        print(f"\nFull response (first 3000 chars):")
        print(json.dumps(data, indent=2)[:3000])
else:
    print(f"Response: {r.text[:500]}")

print("\n" + "=" * 70)
print("Copy this entire output and paste back to me.")
