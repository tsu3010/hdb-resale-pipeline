"""Quick smoke test for the data.gov.sg API key."""

import time

import requests

from config import DATA_GOV_SG_API_KEY, HDB_API_INITIATE_URL, HDB_API_POLL_URL

MAX_POLLS = 5

headers = {"Content-Type": "application/json"}
if DATA_GOV_SG_API_KEY:
    headers["x-api-key"] = DATA_GOV_SG_API_KEY

print(f"API key loaded: {'YES' if DATA_GOV_SG_API_KEY else 'NO — set DATA_GOV_SG_API_KEY in .env'}")
print(f"Initiate URL:   {HDB_API_INITIATE_URL}\n")

s = requests.Session()

# Step 1: Initiate download
initiate_resp = s.get(HDB_API_INITIATE_URL, headers=headers, json={})
print(f"Initiate [{initiate_resp.status_code}]:", initiate_resp.json().get("data", {}).get("message", initiate_resp.text[:200]))

if initiate_resp.status_code not in (200, 201):
    print("\nFAILED — check API key or dataset ID.")
    raise SystemExit(1)

# Step 2: Poll for download URL
for i in range(MAX_POLLS):
    time.sleep(3)
    poll_resp = s.get(HDB_API_POLL_URL, headers=headers, json={})
    data = poll_resp.json().get("data", {})
    print(f"Poll {i+1}/{MAX_POLLS} [{poll_resp.status_code}]:", data)

    if "url" in data:
        print(f"\nDownload URL: {data['url']}")
        print("\nAPI key test PASSED.")
        break
    elif i == MAX_POLLS - 1:
        print("\nTimed out — no download URL returned.")
