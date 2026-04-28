"""
SmartLead API Diagnostic
========================
Runs a few test calls against SmartLead and prints the raw responses
so we can see what shape the data comes back in.

Usage: python debug_smartlead.py
"""

import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("SMARTLEAD_API_KEY")
BASE = "https://server.smartlead.ai/api/v1"

if not API_KEY:
    print("❌ SMARTLEAD_API_KEY not found in .env")
    exit(1)

print(f"Using API key: {API_KEY[:10]}...{API_KEY[-4:]}")
print("=" * 70)

# Test 1: Can we list campaigns at all?
print("\n[TEST 1] Listing all campaigns in your account…")
try:
    r = requests.get(f"{BASE}/campaigns", params={"api_key": API_KEY}, timeout=30)
    print(f"  Status: {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        campaigns = data if isinstance(data, list) else data.get("data", [])
        print(f"  Found {len(campaigns)} campaigns")
        if campaigns:
            print("  First 5 campaigns:")
            for c in campaigns[:5]:
                cid = c.get("id")
                name = c.get("name", "(no name)")
                status = c.get("status", "?")
                print(f"    - ID: {cid} | Status: {status} | Name: {name}")
    else:
        print(f"  Response body: {r.text[:500]}")
except Exception as e:
    print(f"  ❌ Error: {e}")

# Test 2: Try to fetch stats for one of your tracked campaigns
TRACKED = [3134980, 3134858, 3072412, 3043415, 2978732]

print(f"\n[TEST 2] Fetching stats for campaign {TRACKED[0]}…")
try:
    r = requests.get(
        f"{BASE}/campaigns/{TRACKED[0]}/statistics",
        params={"api_key": API_KEY},
        timeout=30,
    )
    print(f"  Status: {r.status_code}")
    if r.status_code == 200:
        print(f"  Raw response (first 2000 chars):")
        print(f"  {json.dumps(r.json(), indent=2)[:2000]}")
    else:
        print(f"  Response body: {r.text[:500]}")
except Exception as e:
    print(f"  ❌ Error: {e}")

# Test 3: Try the analytics endpoint instead
print(f"\n[TEST 3] Trying /campaigns/{TRACKED[0]}/analytics…")
try:
    r = requests.get(
        f"{BASE}/campaigns/{TRACKED[0]}/analytics",
        params={"api_key": API_KEY},
        timeout=30,
    )
    print(f"  Status: {r.status_code}")
    if r.status_code == 200:
        print(f"  Raw response (first 2000 chars):")
        print(f"  {json.dumps(r.json(), indent=2)[:2000]}")
    else:
        print(f"  Response body: {r.text[:300]}")
except Exception as e:
    print(f"  ❌ Error: {e}")

# Test 4: Try fetching leads for that campaign
print(f"\n[TEST 4] Fetching leads for campaign {TRACKED[0]} (first 5)…")
try:
    r = requests.get(
        f"{BASE}/campaigns/{TRACKED[0]}/leads",
        params={"api_key": API_KEY, "offset": 0, "limit": 5},
        timeout=30,
    )
    print(f"  Status: {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        print(f"  Top-level keys: {list(data.keys()) if isinstance(data, dict) else 'LIST'}")
        leads = data.get("data", data) if isinstance(data, dict) else data
        print(f"  Total leads returned: {len(leads) if isinstance(leads, list) else 'not a list'}")
        if isinstance(leads, list) and leads:
            print(f"  First lead keys: {list(leads[0].keys())}")
            print(f"  First lead (first 1500 chars):")
            print(f"  {json.dumps(leads[0], indent=2)[:1500]}")
    else:
        print(f"  Response body: {r.text[:500]}")
except Exception as e:
    print(f"  ❌ Error: {e}")

# Test 5: Try to find a campaign with replies using the lead-statistics endpoint
print(f"\n[TEST 5] Trying /campaigns/{TRACKED[0]}/lead-statistics…")
try:
    r = requests.get(
        f"{BASE}/campaigns/{TRACKED[0]}/lead-statistics",
        params={"api_key": API_KEY, "offset": 0, "limit": 5},
        timeout=30,
    )
    print(f"  Status: {r.status_code}")
    if r.status_code == 200:
        print(f"  Raw response (first 1500 chars):")
        print(f"  {json.dumps(r.json(), indent=2)[:1500]}")
    else:
        print(f"  Response body: {r.text[:300]}")
except Exception as e:
    print(f"  ❌ Error: {e}")

print("\n" + "=" * 70)
print("Done. Copy this entire output and paste back to me.")
