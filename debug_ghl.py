"""
Inspects the GHL /opportunities/search response so we can see exactly
how custom fields are returned and fix the field extraction.

Usage: python debug_ghl.py
"""

import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

GHL_TOKEN = os.getenv("GHL_PRIVATE_TOKEN")
GHL_LOCATION_ID = os.getenv("GHL_LOCATION_ID")
GHL_PIPELINE_ID = os.getenv("GHL_PIPELINE_ID")

BASE = "https://services.leadconnectorhq.com"
HEADERS = {
    "Authorization": f"Bearer {GHL_TOKEN}",
    "Version": "2021-07-28",
    "Accept": "application/json",
}

# Custom field IDs we expect to find
EXPECTED_FIELDS = {
    "IyS6bhX7hdUcg81AfRda": "SmartLead Campaign ID",
    "SjdCvQ9cTILmG8MKqIyd": "Call Proposed At",
    "syJfDi9KwrqolBasfOwC": "Day 1 Follow-Up At",
    "Z8iIXI5ZYaCa2C9GeDpT": "Day 2 Follow-Up At",
    "sQUdyW4BzV6U5OLHRpeX": "Booked Call At",
}

print("Fetching GHL opportunities (first page)...")
r = requests.get(
    f"{BASE}/opportunities/search",
    headers=HEADERS,
    params={
        "location_id": GHL_LOCATION_ID,
        "pipeline_id": GHL_PIPELINE_ID,
        "limit": 20,
        "page": 1,
    },
    timeout=30,
)
print(f"Status: {r.status_code}\n")

if r.status_code != 200:
    print(f"Response: {r.text[:500]}")
    exit(1)

data = r.json()
opps = data.get("opportunities", [])
print(f"Found {len(opps)} opportunities\n")

# Show structure of first opp
if opps:
    first = opps[0]
    print(f"Top-level keys of first opportunity:")
    print(f"  {list(first.keys())}\n")

    print(f"customFields field present: {'customFields' in first}")
    if "customFields" in first:
        cf = first.get("customFields")
        print(f"customFields type: {type(cf).__name__}")
        print(f"customFields value: {json.dumps(cf, indent=2)[:1000]}\n")

# Find an opp that should have custom fields filled
print("=" * 70)
print("Searching for an opp with non-empty customFields...\n")

for opp in opps:
    cf = opp.get("customFields", []) or []
    if cf:
        print(f"Found one: {opp.get('name')} (email: {opp.get('contact', {}).get('email')})")
        print(f"customFields structure (raw):")
        print(json.dumps(cf, indent=2)[:2000])
        print()
        # Check if any of our expected IDs are present
        for f in cf:
            field_id = f.get("id") or f.get("customFieldId") or f.get("fieldId")
            if field_id in EXPECTED_FIELDS:
                print(f"  MATCH: id={field_id} -> {EXPECTED_FIELDS[field_id]}")
                print(f"    Full field obj: {json.dumps(f, indent=4)}")
        break
else:
    print("No opps in first 20 had any customFields populated.")
    print("This is probably why the columns are blank!\n")
    print("Possible explanations:")
    print("  1. GHL API isn't returning custom fields by default for this endpoint")
    print("  2. None of the contacts in the first 20 have those custom fields set")
    print("  3. The Manus automation hasn't backfilled most contacts yet")

# Also try fetching one specific contact to see custom fields directly
if opps and opps[0].get("contact", {}).get("id"):
    contact_id = opps[0]["contact"]["id"]
    print(f"\n" + "=" * 70)
    print(f"Trying /contacts/{contact_id} to see if custom fields appear there...")
    r = requests.get(
        f"{BASE}/contacts/{contact_id}",
        headers=HEADERS,
        timeout=30,
    )
    print(f"Status: {r.status_code}")
    if r.status_code == 200:
        cdata = r.json().get("contact", r.json())
        print(f"Contact top-level keys: {list(cdata.keys())[:20]}")
        if "customFields" in cdata:
            print(f"customFields from /contacts: {json.dumps(cdata['customFields'], indent=2)[:1500]}")

print("\n" + "=" * 70)
print("Copy this entire output and paste back to me.")
