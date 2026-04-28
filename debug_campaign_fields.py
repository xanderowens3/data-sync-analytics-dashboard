"""
Checks SmartLead's "List all Campaigns" endpoint to find the exact
field name for last send date / last activity date.

Usage: python debug_campaign_fields.py
"""

import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("SMARTLEAD_API_KEY")
BASE = "https://server.smartlead.ai/api/v1"

print("Fetching campaign list...")
r = requests.get(f"{BASE}/campaigns", params={"api_key": API_KEY}, timeout=30)
print(f"Status: {r.status_code}\n")

if r.status_code == 200:
    data = r.json()
    campaigns = data if isinstance(data, list) else data.get("data", [])
    print(f"Total campaigns: {len(campaigns)}\n")

    if campaigns:
        # Show ALL keys from first campaign
        first = campaigns[0]
        print(f"All keys in first campaign:")
        for k, v in first.items():
            v_str = str(v)
            if len(v_str) > 100:
                v_str = v_str[:100] + "..."
            print(f"  {k}: {v_str}")

        # Find an ACTIVE campaign and show its fields
        print("\n" + "=" * 70)
        active = [c for c in campaigns if c.get("status") == "ACTIVE"]
        print(f"\nActive campaigns: {len(active)}")
        if active:
            print(f"\nFirst active campaign fields:")
            for k, v in active[0].items():
                v_str = str(v)
                if len(v_str) > 100:
                    v_str = v_str[:100] + "..."
                print(f"  {k}: {v_str}")

        # Also check /analytics for one campaign to see if last send date is there
        print("\n" + "=" * 70)
        test_id = campaigns[0].get("id")
        print(f"\nChecking /analytics for campaign {test_id}...")
        r2 = requests.get(f"{BASE}/campaigns/{test_id}/analytics",
                          params={"api_key": API_KEY}, timeout=30)
        if r2.status_code == 200:
            analytics = r2.json()
            print("All analytics keys:")
            for k, v in analytics.items():
                v_str = str(v)
                if len(v_str) > 100:
                    v_str = v_str[:100] + "..."
                print(f"  {k}: {v_str}")
else:
    print(f"Error: {r.text[:500]}")

print("\n" + "=" * 70)
print("Copy this output and paste back to me.")
