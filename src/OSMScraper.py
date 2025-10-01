#!/usr/bin/env python3
"""
OSM Leads Seed (USA + State mode) — relevant fields for selling leads, resumable, tile-based.
"""

import csv, argparse, sys, time, json, re, os
from urllib.parse import urlsplit, urlunsplit, parse_qsl
import requests

DASH_STATS_PATH = "dash_stats.json"

def write_stats(path, stats, bbox=None, center=None, examples=None, state=None):
    """Safely write stats JSON for dashboard.html to read.
       Can include bbox, center, examples, and state for the live dashboard.
    """
    payload = dict(stats)  # copy whatever you pass in

    if bbox:
        payload["bbox"] = {
            "s": bbox[0],
            "w": bbox[1],
            "n": bbox[2],
            "e": bbox[3],
        }
    if center:
        payload["center"] = {"lat": center[0], "lon": center[1]}
    if examples:
        payload["examples"] = examples[-10:]  # last 10 only
    if state:
        payload["state"] = state

    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    os.replace(tmp, path)


OVERPASS_URLS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.openstreetmap.ru/api/interpreter",
]
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; OSMLeadsSeedUSA/1.0)"}

PRESETS = {
    "coffee":   {"tags": ["amenity=cafe", "shop=coffee"]},
    # wider net to catch indie shops with inconsistent tagging
    "coffee_wide": {"tags": [
        "amenity=cafe","shop=coffee","craft=coffee_roaster",
        "amenity=restaurant","amenity=fast_food","shop=bakery","shop=tea"
    ]},
    "golf": {
        # Strict: real courses only
        "tags": [
            "leisure=golf_course"
        ]
    },
    "dental": {
        "tags": [
            "amenity=dentist",
            "healthcare=dentist",
            "healthcare:speciality=orthodontics",
            "healthcare:speciality=oral_surgery",
            "healthcare:speciality=endodontics",
            "healthcare:speciality=periodontics",
            "healthcare:speciality=prosthodontics",
            "healthcare:speciality=pediatric_dentistry"
        ]
    },
    "veterinary": {
        "tags": [
            "amenity=veterinary",
            "healthcare=veterinary",
            "healthcare:speciality=veterinary",
            "healthcare:speciality=animal_hospital",
            "healthcare:speciality=small_animals",
            "healthcare:speciality=large_animals",
            "healthcare:speciality=exotics",
            "healthcare:speciality=equine",
            "healthcare:speciality=wildlife"
        ]
    },
    "distillery": {
        "tags": ["craft=distillery", "amenity=distillery"]
    },
    "brewery":  {"tags": ["craft=brewery", "amenity=pub"]},
    "winery":   {"tags": ["craft=winery", "amenity=winery"]},
    "gym":      {"tags": ["leisure=fitness_centre", "leisure=sports_centre"]},
    "salon":    {"tags": ["shop=hairdresser", "shop=beauty"]},
    "dirtworld": {
    "tags": [
        "office=construction_company",   # proper construction contractors
        "craft=builder",                 # small contractors
        "industrial=construction",       # some excavation firms
        "craft=plumber",                 # septic often falls here
        "service=septic_tank"            # explicit septic work
    ],

}
}

SCHEMA = [
    "osm_id","name","category_tags","brand","operator",
    "housenumber","street","city","state","postcode","country",
    "latitude","longitude",
    "website","phone","email",
    "facebook","instagram","twitter","tiktok","linkedin",
    "opening_hours","source"
]

STATE_BBOXES = {
"AL": (30.137, -88.473, 35.008, -84.889),
"AK": (51.214, -179.148, 71.352, 179.778),
"AZ": (31.332, -114.817, 37.004, -109.045),
"AR": (33.004, -94.617, 36.500, -89.644),
"CA": (32.534, -124.409, 42.009, -114.131),
"CO": (36.993, -109.045, 41.003, -102.042),
"CT": (41.011, -73.727, 42.050, -71.787),
"DE": (38.451, -75.789, 39.839, -74.984),
"FL": (24.396, -87.634, 31.001, -80.031),
"GA": (30.356, -85.605, 35.000, -80.841),
"HI": (18.910, -178.334, 28.402, -154.806),
"ID": (41.988, -117.243, 49.001, -111.043),
"IL": (36.970, -91.513, 42.509, -87.019),
"IN": (37.771, -88.097, 41.761, -84.784),
"IA": (40.375, -96.639, 43.501, -90.140),
"KS": (36.993, -102.051, 40.004, -94.588),
"KY": (36.497, -89.571, 39.147, -81.964),
"LA": (28.927, -94.043, 33.019, -88.817),
"ME": (42.977, -71.083, 47.460, -66.950),
"MD": (37.888, -79.487, 39.723, -75.049),
"MA": (41.187, -73.508, 42.887, -69.861),
"MI": (41.696, -90.418, 48.305, -82.413),
"MN": (43.499, -97.239, 49.384, -89.491),
"MS": (30.147, -91.655, 35.007, -88.098),
"MO": (35.996, -95.774, 40.613, -89.100),
"MT": (44.358, -116.050, 49.001, -104.039),
"NE": (39.999, -104.057, 43.001, -95.309),
"NV": (35.001, -120.005, 42.002, -114.039),
"NH": (42.696, -72.557, 45.305, -70.704),
"NJ": (38.788, -75.563, 41.357, -73.885),
"NM": (31.332, -109.050, 37.000, -103.002),
"NY": (40.496, -79.762, 45.015, -71.852),
"NC": (33.751, -84.322, 36.588, -75.400),
"ND": (45.935, -104.050, 49.000, -96.554),
"OH": (38.403, -84.821, 41.978, -80.519),
"OK": (33.615, -103.002, 37.002, -94.431),
"OR": (41.991, -124.566, 46.292, -116.463),
"PA": (39.719, -80.520, 42.269, -74.689),
"RI": (41.146, -71.862, 42.018, -71.120),
"SC": (32.034, -83.354, 35.215, -78.542),
"SD": (42.479, -104.057, 45.945, -96.436),
"TN": (34.982, -90.311, 36.678, -81.646),
"TX": (25.837, -106.645, 36.500, -93.508),
"UT": (36.997, -114.053, 42.001, -109.041),
"VT": (42.726, -73.437, 45.017, -71.465),
"VA": (36.541, -83.675, 39.466, -75.242),
"WA": (45.543, -124.848, 49.002, -116.918),
"WV": (37.201, -82.644, 40.638, -77.719),
"WI": (42.491, -92.889, 47.309, -86.250),
"WY": (40.994, -111.056, 45.005, -104.052),
}

TRACK_QS = {"utm_source","utm_medium","utm_campaign","utm_term","utm_content","fbclid","gclid","mc_cid","mc_eid"}
PHONE_RE = re.compile(r"[\+\d][\d\s\-().]{5,}")
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")

CHAIN_PATTERNS = re.compile(
    r"(?i)\b("
    r"starbucks|dunkin|peet'?s|caribou|tim\\s*hortons|dutch\\s*bros|gloria\\s*jeans|biggby|coffee\\s*bean\\s*&\\s*tea|pj'?s\\s*coffee"
    r")\b"
)

def is_chain(tags, exclude_rx=None):
    """Return True if element looks like a big chain by name/brand/operator."""
    name = (tags.get("name") or tags.get("alt_name") or "").strip()
    brand = (tags.get("brand") or "").strip()
    operator = (tags.get("operator") or "").strip()
    if exclude_rx and (exclude_rx.search(name) or exclude_rx.search(brand) or exclude_rx.search(operator)):
        return True
    if CHAIN_PATTERNS.search(name) or CHAIN_PATTERNS.search(brand) or CHAIN_PATTERNS.search(operator):
        return True
    if CHAIN_PATTERNS.search(tags.get("brand:en","")): return True
    if CHAIN_PATTERNS.search(tags.get("brand:wikidata","")): return True
    if CHAIN_PATTERNS.search(tags.get("brand:wikipedia","")): return True
    return False

OVERPASS_TMPL = """
[out:json][timeout:30];
(
{selectors}
);
out center tags;
"""

def clean_url(u: str) -> str:
    if not u: return ''
    try:
        if not (u.startswith("http://") or u.startswith("https://")):
            u = "https://" + u.strip()
        p = urlsplit(u)
        qs = [(k,v) for k,v in parse_qsl(p.query, keep_blank_values=True) if k.lower() not in TRACK_QS]
        netloc = p.netloc.lower().replace("www.", "")
        return urlunsplit((p.scheme, netloc, p.path or "", "&".join([f"{k}={v}" for k,v in qs]), ""))
    except Exception:
        return u

def pick(t, keys):
    for k in keys:
        if t.get(k): return t.get(k)
    return ""

def norm_phone(s):
    if not s: return ""
    m = PHONE_RE.search(s)
    if not m: return s.strip()
    raw = re.sub(r"[\s().-]", "", m.group(0))
    if raw.startswith("+"): return raw
    if len(raw) == 10: return "+1"+raw
    return raw

def norm_email(s):
    if not s: return ""
    m = EMAIL_RE.search(s)
    return (m.group(0) if m else s).strip().lower()

def make_selectors(tags,s,w,n,e):
    lines=[]
    for t in tags:
        if "=" not in t: continue
        k,v=t.split("=",1)
        for obj in ("node","way","rel"):
            lines.append(f'  {obj}["{k}"="{v}"]({s},{w},{n},{e});')
    return "\n".join(lines)

def overpass_query(tags,s,w,n,e,sess,max_retries=4):
    q=OVERPASS_TMPL.format(selectors=make_selectors(tags,s,w,n,e))
    for attempt in range(max_retries):
        try:
            url=OVERPASS_URLS[attempt % len(OVERPASS_URLS)]
            r=sess.post(url,data={"data":q},headers=HEADERS,timeout=90)
            r.raise_for_status()
            return r.json().get("elements",[])
        except Exception:
            time.sleep(2**attempt)
    return []

def tile_bbox(s,w,n,e,step):
    lat=s
    while lat<n:
        lon=w
        lat2=min(lat+step,n)
        while lon<e:
            lon2=min(lon+step,e)
            yield (lat,lon,lat2,lon2)
            lon=lon2
        lat=lat2

def usa_tiles(step):
    for box in tile_bbox(24.5,-125.0,49.5,-66.9,step):
        yield box

def parse_bbox(text):
    s,w,n,e=[float(x) for x in text.split(",")]
    return s,w,n,e

def extract_row(el, tags_used):
    t = el.get("tags", {})  # define first; used by everything below

    # Helper: collapse any URL to scheme + host only (homepage)
    def rootify(u: str) -> str:
        u = clean_url(u)
        if not u:
            return ""
        p = urlsplit(u)
        return urlunsplit((p.scheme, p.netloc, "", "", ""))

    # Website (collapse website/contact:website → root homepage)
    site1 = t.get("website") or ""
    site2 = t.get("contact:website") or ""
    website = rootify(site1 or site2)

    # Phones (merge, normalize, dedupe; deterministic order)
    p1 = norm_phone(t.get("phone") or "")
    p2 = norm_phone(t.get("contact:phone") or "")
    phones = [p for p in {p1, p2} if p]
    phone = "; ".join(sorted(phones))

    # Emails (merge, normalize, dedupe; deterministic order)
    e1 = norm_email(t.get("email") or "")
    e2 = norm_email(t.get("contact:email") or "")
    emails = [e for e in {e1, e2} if e]
    email = "; ".join(sorted(emails))

    name = t.get("name") or t.get("alt_name") or ""
    housenumber = pick(t, ["addr:housenumber"])
    street = pick(t, ["addr:street"])
    city = pick(t, ["addr:city", "addr:suburb", "addr:district"])
    state = pick(t, ["addr:state", "addr:province", "addr:region"])
    postcode = pick(t, ["addr:postcode"])
    country = pick(t, ["addr:country"]) or "US"

    facebook  = clean_url(t.get("facebook")  or t.get("contact:facebook")  or "")
    instagram = clean_url(t.get("instagram") or t.get("contact:instagram") or "")
    twitter   = clean_url(t.get("twitter")   or t.get("contact:twitter")   or "")
    tiktok    = clean_url(t.get("tiktok")    or t.get("contact:tiktok")    or "")
    linkedin  = clean_url(t.get("linkedin")  or t.get("contact:linkedin")  or "")

    opening_hours = t.get("opening_hours", "")
    brand = t.get("brand", "")
    operator = t.get("operator", "")

    lat = el.get("lat") or (el.get("center") or {}).get("lat") or ""
    lon = el.get("lon") or (el.get("center") or {}).get("lon") or ""

    return {
        "osm_id": f"{el.get('type','node')}:{el.get('id')}",
        "name": name,
        "category_tags": ",".join(tags_used),
        "brand": brand,
        "operator": operator,
        "housenumber": housenumber,
        "street": street,
        "city": city,
        "state": state,
        "postcode": postcode,
        "country": country,
        "latitude": str(lat),
        "longitude": str(lon),
        "website": website,
        "phone": phone,
        "email": email,
        "facebook": facebook,
        "instagram": instagram,
        "twitter": twitter,
        "tiktok": tiktok,
        "linkedin": linkedin,
        "opening_hours": opening_hours,
        "source": "OSM Overpass",
    }


def write_headers_if_needed(path):
    if not os.path.exists(path) or os.path.getsize(path)==0:
        with open(path,'w',newline='',encoding='utf-8') as f:
            csv.DictWriter(f,fieldnames=SCHEMA).writeheader()

def load_existing_ids(path):
    ids=set()
    if not os.path.exists(path): return ids
    with open(path,newline='',encoding='utf-8') as f:
        for row in csv.DictReader(f):
            if row.get('osm_id'): ids.add(row['osm_id'])
    return ids

def append_row(path,row):
    with open(path,'a',newline='',encoding='utf-8') as f:
        csv.DictWriter(f,fieldnames=SCHEMA).writerow(row)

def main():
    ap=argparse.ArgumentParser()
    where=ap.add_mutually_exclusive_group(required=True)
    where.add_argument("--bbox",help="south,west,north,east")
    where.add_argument("--usa",type=int,help="1=scan continental USA")
    where.add_argument("--state",help="Two-letter state code (e.g., CA, TX, NY)")
    ap.add_argument("--preset",choices=sorted(PRESETS.keys()))
    ap.add_argument("--tags",help="Custom tags")
    ap.add_argument("--name-regex")
    ap.add_argument("--exclude-name-regex", help="Regex to exclude names (e.g., chains)")
    ap.add_argument("--exclude-chains", type=int, default=1, help="1=exclude known big chains via brand/operator/name")
    ap.add_argument("--require-any-contact",type=int,default=0)
    ap.add_argument("--tile",type=float,default=1.0)
    ap.add_argument("--sleep",type=float,default=1.5)
    ap.add_argument("--limit",type=int,default=0)
    ap.add_argument("--out",required=True)
    ap.add_argument("--resume",type=int,default=0)
    args=ap.parse_args()

    print(f"🚀 Starting scrape: preset={args.preset or 'custom'} "
      f"state={args.state or 'N/A'} usa={args.usa or 0} "
      f"tile={args.tile} sleep={args.sleep}s "
      f"require_contact={args.require_any_contact} resume={args.resume}")

    tags=[]
    if args.preset: tags=PRESETS[args.preset]["tags"][:]
    if args.tags: tags+=[t.strip() for t in args.tags.split(",") if t.strip()]
    if not tags: sys.exit("No tags specified")

    write_headers_if_needed(args.out)
    seen=load_existing_ids(args.out) if args.resume else set()
    name_rx=re.compile(args.name_regex,re.I) if args.name_regex else None
    exclude_rx=re.compile(args.exclude_name_regex,re.I) if args.exclude_name_regex else None

    if args.bbox:
        tiles=list(tile_bbox(*parse_bbox(args.bbox),args.tile))
    elif args.state:
        code=args.state.upper()
        if code not in STATE_BBOXES: sys.exit(f"Unknown state code {code}")
        tiles=list(tile_bbox(*STATE_BBOXES[code],args.tile))
        print(f"📍 Scanning state: {code} ({len(tiles)} tiles)", flush=True)

    else:
        tiles=list(usa_tiles(args.tile))

    kept=0
    start_ts = time.time()
    examples = []
    TOTAL_TILES = len(tiles)
    last_examples = []

    # initial write so dashboard.html can open immediately
    write_stats(DASH_STATS_PATH, {
        "started_at": start_ts,
        "tiles_done": 0,
        "tiles_total": TOTAL_TILES,
        "kept_total": 0,
        "rate_rows_per_sec": 0.0,
        "elapsed_sec": 0.0,
        "last_examples": []
    })

    with requests.Session() as sess:
        seen_uids = set()
        for i,(s,w,n,e) in enumerate(tiles,1):
            els=overpass_query(tags,s,w,n,e,sess)
            _uniq = {}
            for _el in els:
                _uid = f"{_el.get('type','node')}:{_el.get('id')}"
                _uniq[_uid] = _el
            els = list(_uniq.values())
            got=0
            for el in els:
                uid = f"{el.get('type','node')}:{el.get('id')}"
                if uid in seen_uids:
                    continue
                seen_uids.add(uid)
                t=el.get("tags",{})
                nm=(t.get("name") or t.get("alt_name") or "").strip()
                if name_rx and not name_rx.search(nm): continue
                if exclude_rx and exclude_rx.search(nm): continue
                if args.exclude_chains and is_chain(t, exclude_rx): continue
                if args.require_any_contact and not (t.get("website") or t.get("contact:website") or t.get("phone") or t.get("contact:phone") or t.get("email") or t.get("contact:email")):
                    continue
                row=extract_row(el,tags)
                if row['osm_id'] in seen:
                    continue
                seen.add(row['osm_id'])
                append_row(args.out,row)

                # NEW: track last examples for the dashboard table
                if row.get("name"):
                    last_examples.append({
                        "name": row.get("name",""),
                        "city": row.get("city",""),
                        "state": row.get("state",""),
                        "website": row.get("website","")
                    })
                    if len(last_examples) > 20:  # keep only the most recent 20
                        last_examples.pop(0)

                if kept % 50 == 0:
                    print(f"✨ Example [{kept}]: {row['name']} "
                        f"({row['city']}, {row['state']}) -> {row['website'] or 'no site'}")
                kept+=1; got+=1
                if args.limit and kept>=args.limit: return
            print(f"[{i}/{len(tiles)}] tile {s:.2f},{w:.2f}->{n:.2f},{e:.2f}: kept {got} total {kept}")
            elapsed = time.time() - start_ts
            rate = (kept / elapsed) if elapsed > 0 else 0.0

            stats = {
                "tiles_done": i,
                "tiles_total": len(tiles),
                "kept_total": kept,
                "elapsed_sec": int(elapsed),
                "rows_per_sec": rate,
            }
            center = ((s + n) / 2.0, (w + e) / 2.0)
            write_stats(
                "dash_stats.json",
                stats,
                bbox=(s, w, n, e),
                center=center,
                examples=last_examples,
                state=(args.state.upper() if args.state else "")
            )

            # add one sample row if we actually kept something in this tile
            if got > 0:
                examples.append({
                    "name": row.get("name",""),
                    "city": row.get("city",""),
                    "state": row.get("state",""),
                    "website": row.get("website",""),
                    "lat": row.get("latitude",""),
                    "lon": row.get("longitude","")
                })
                if len(examples) > 5:
                    examples = examples[-5:]

            write_stats(DASH_STATS_PATH, {
                "started_at": start_ts,
                "tiles_done": i,
                "tiles_total": TOTAL_TILES,
                "kept_total": kept,
                "rate_rows_per_sec": round(rate, 3),
                "elapsed_sec": round(elapsed, 1),
                "last_examples": examples
            })
            if i % 10 == 0:
                print(f"📊 Progress: {i}/{len(tiles)} tiles done, {kept} businesses kept so far")
            time.sleep(args.sleep)
    print("✅ Scrape finished")
    print(f"   Total businesses kept: {kept}")
    print(f"   Output file: {args.out}")
    elapsed = time.time() - start_ts
    rate = (kept / elapsed) if elapsed > 0 else 0.0
    write_stats(DASH_STATS_PATH, {
        "started_at": start_ts,
        "tiles_done": TOTAL_TILES,
        "tiles_total": TOTAL_TILES,
        "kept_total": kept,
        "rate_rows_per_sec": round(rate, 3),
        "elapsed_sec": round(elapsed, 1),
        "last_examples": examples
    })
  
if __name__=="__main__":
    main()
