#!/usr/bin/env python3
"""
Backfill address fields (city/state/postcode/country [+ street/house]) using Nominatim reverse geocoding.
- Respects usage policy: 1 req/sec, descriptive User-Agent with contact email, retries with backoff.
- Caches results locally so re-runs are fast and gentle to the service.
- Can enforce a final state filter after enrichment (e.g., keep only MN).

Usage:
  python3 backfill_address.py --in coffee_mn.csv --out coffee_mn_enriched.csv \
      --email you@example.com --only-missing 1 --state-filter MN --sleep 1.1

Notes:
- Please use your own email in --email (Nominatim policy).
- Keep --sleep >= 1.0 to be a good API citizen.
- If you plan heavy volumes, consider self-hosting Nominatim.
"""

import csv, sys, time, argparse, json, os, math
from typing import Dict, Tuple
import requests

API_URL = "https://nominatim.openstreetmap.org/reverse"
DEFAULT_SLEEP = 1.1  # seconds between requests (>=1.0 recommended)
CACHE_FILE_DEFAULT = "nominatim_cache.json"

# Which CSV columns we will try to fill
ADDR_COLS = [
    "housenumber", "street", "city", "state", "postcode", "country"
]

CITY_KEYS = [
    "city", "town", "village", "hamlet", "municipality", "suburb", "neighbourhood"
]

SESSION = None


def load_cache(path: str) -> Dict[str, dict]:
    if path and os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_cache(path: str, data: Dict[str, dict]):
    if not path:
        return
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, path)


def norm_key(lat: str, lon: str, precision: int = 5) -> str:
    # Round to reduce duplicate calls due to sub-meter diffs
    try:
        latf = round(float(lat), precision)
        lonf = round(float(lon), precision)
    except Exception:
        return f"{lat},{lon}"
    return f"{latf:.{precision}f},{lonf:.{precision}f}"


def reverse_geocode(lat: str, lon: str, email: str, sleep: float, retries: int = 3) -> dict:
    global SESSION
    if SESSION is None:
        SESSION = requests.Session()

    params = {
        "lat": lat,
        "lon": lon,
        "format": "jsonv2",
        "addressdetails": 1,
        "zoom": 18,
    }
    headers = {
        "User-Agent": f"OSMBackfill/1.0 (+{email})"
    }
    last_err = None
    for attempt in range(retries):
        try:
            r = SESSION.get(API_URL, params=params, headers=headers, timeout=30)
            if r.status_code == 429:
                # too many requests: back off more
                wait = max(sleep, 1.0) * (attempt + 2)
                time.sleep(wait)
                continue
            r.raise_for_status()
            data = r.json()
            time.sleep(max(sleep, 1.0))  # be polite between calls
            return data
        except Exception as ex:
            last_err = ex
            time.sleep(max(sleep, 1.0) * (attempt + 1))
    raise last_err


def extract_fields(payload: dict) -> dict:
    """Map Nominatim response into our CSV fields."""
    addr = (payload or {}).get("address", {})
    # city fallback chain
    city = ""
    for k in CITY_KEYS:
        if addr.get(k):
            city = addr[k]
            break
    state = addr.get("state", "")
    postcode = addr.get("postcode", "")
    country = addr.get("country_code", "").upper() or addr.get("country", "")
    # street + housenumber if present
    street = addr.get("road", "") or addr.get("pedestrian", "") or addr.get("footway", "")
    housenumber = addr.get("house_number", "")
    return {
        "housenumber": housenumber,
        "street": street,
        "city": city,
        "state": state,
        "postcode": postcode,
        "country": country,
    }


def needs_backfill(row: dict, only_missing: bool) -> bool:
    if not only_missing:
        return True
    # Backfill only if at least one of target fields is missing
    return any(not (row.get(col) or "").strip() for col in ADDR_COLS)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="Input CSV from OSM scraper")
    ap.add_argument("--out", dest="out", required=True, help="Output enriched CSV")
    ap.add_argument("--email", required=True, help="Contact email for Nominatim User-Agent")
    ap.add_argument("--only-missing", type=int, default=1, help="1=only fill rows with missing address fields")
    ap.add_argument("--sleep", type=float, default=DEFAULT_SLEEP, help="Seconds between lookups (>=1.0)")
    ap.add_argument("--cache", default=CACHE_FILE_DEFAULT, help="Path to JSON cache file")
    ap.add_argument("--max", type=int, default=0, help="Stop after N rows (debug)")
    ap.add_argument("--state-filter", default="", help="Keep only this state code after backfill (e.g., MN)")
    args = ap.parse_args()

    cache = load_cache(args.cache)
    total = kept = looked = 0

    with open(args.inp, newline='', encoding='utf-8') as f_in, \
         open(args.out, 'w', newline='', encoding='utf-8') as f_out:
        reader = csv.DictReader(f_in)
        # Ensure output has all original fields plus our address cols
        fieldnames = list(reader.fieldnames or [])
        for c in ADDR_COLS:
            if c not in fieldnames:
                fieldnames.append(c)
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            total += 1
            lat = (row.get('latitude') or '').strip()
            lon = (row.get('longitude') or '').strip()
            if not lat or not lon:
                # Can't backfill without coordinates
                writer.writerow(row)
                continue

            if needs_backfill(row, bool(args.only_missing)):
                key = norm_key(lat, lon)
                data = cache.get(key)
                if not data:
                    try:
                        data = reverse_geocode(lat, lon, args.email, args.sleep)
                        cache[key] = data
                        looked += 1
                        if looked % 25 == 0:
                            print(f"🔎 Looked up {looked} locations so far (cached {len(cache)}).")
                    except Exception as ex:
                        print(f"⚠️  Reverse geocode failed for {lat},{lon}: {ex}")
                        data = None
                if data:
                    fields = extract_fields(data)
                    # Fill only if missing
                    for c in ADDR_COLS:
                        if not (row.get(c) or "").strip() and fields.get(c):
                            row[c] = fields[c]

            # Optional final filter by state
            if args.state_filter:
                st = (row.get('state') or '').strip().upper()
                if st and st != args.state_filter.upper():
                    continue  # drop

            writer.writerow(row)
            kept += 1
            if args.max and kept >= args.max:
                break

    save_cache(args.cache, cache)
    print("✅ Backfill complete")
    print(f"   Input rows:  {total}")
    print(f"   Output rows: {kept}")
    print(f"   Cache size:  {len(cache)} keys")
    print(f"   Output file: {args.out}")


if __name__ == "__main__":
    main()
