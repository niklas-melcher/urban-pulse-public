# scripts/explore_luchtmeetnet.py
"""Throwaway script to figure out Luchtmeetnet's actual measurements
endpoint shape before writing real ingestion code against it.
Run with: uv run python scripts/explore_luchtmeetnet.py
"""

import httpx

STATION = "NL49017"  # swap for your real station number
BASE = "https://api.luchtmeetnet.nl/open_api"

candidates = [
    f"{BASE}/stations/{STATION}/measurements",
    f"{BASE}/measurements?station_number={STATION}",
    f"{BASE}/concentrations?station_number={STATION}",
]

for url in candidates:
    r = httpx.get(url, timeout=10.0)
    print(f"\n{url}\n  status: {r.status_code}")
    if r.status_code == 200:
        print(f"  body sample: {str(r.json())[:300]}")
