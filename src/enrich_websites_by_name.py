#!/usr/bin/env python3
"""
Website Finder (Dental-optimized, safety-first)

Reads an input CSV with at least these columns:
  name, city, state, postcode, website, email
(Your file likely has more; we preserve column order.)

Behavior:
- Skip rows that already have a non-directory website (default).
- Optional --repair 1: treat directory/social hosts as "missing" and try to find a real site.
- Stage 1: If email has a custom domain (not free webmail), accept its apex as the website
           (Level 0: offline; Level 1+: confirm with HEAD).
- Stage 2: Deterministic guesses for dentists:
           name+dental(.com), name+dentistry(.com), tokens-with-dental-kept(.com),
           hyphen variant, (optional) city variant.
           Only try TLDs in --tlds (default: .com,.net,.org,.dental).
           DNS gate → public IP only → HEAD (or tiny GET at safety 2) → accept.
- No search engines by default. (You can add later if desired.)

Safety levels:
  --safety 0 : Offline only for email-derived domains. Guessing still does DNS only;
               no HTTP fetch (accepts on strong SLD-name similarity after DNS).
  --safety 1 : (DEFAULT) DNS + HEAD on candidates (no body). Fast & safe.
  --safety 2 : DNS + tiny GET (~100KB) for scoring (better precision).

Conservatism:
- Denylist directory/social/link-shortener hosts (fb/yelp/angi/etc).
- Allowlist TLDs only (default .com,.net,.org,.dental).
- Block IP literals, odd ports, punycode (xn--), non-global IPs.
- Stop early at first accepted candidate.
- Small per-row budgets.

Outputs:
- New CSV (same schema/order).
- Audit JSONL: one line per processed row with tried candidates & rationale.

Usage:
  python find_domains.py --in dental.csv --out dental.withsites.csv --audit audit.jsonl \
      --vertical dental --safety 1 --repair 0 --tlds .com,.net,.org,.dental --max-candidates 6
"""

import csv, argparse, sys, time, json, re, os, socket, ipaddress
from urllib.parse import urlsplit, urlunsplit
import requests
from threading import Lock

# ---------- Config defaults ----------
DEFAULT_TLDS = [".com", ".net", ".org", ".dental"]
FREE_EMAIL_DOMAINS = {
    "gmail.com","googlemail.com","yahoo.com","ymail.com","outlook.com","hotmail.com","live.com","msn.com",
    "icloud.com","me.com","mac.com","aol.com","proton.me","protonmail.com","zoho.com","yandex.com",
    "comcast.net","sbcglobal.net","att.net","verizon.net","mail.com","pm.me"
}
DIRECTORY_HOSTS = {
    "facebook.com","m.facebook.com","fb.com","instagram.com","x.com","twitter.com","linkedin.com",
    "yelp.com","angi.com","yellowpages.com","bbb.org","mapquest.com","foursquare.com",
    "tripadvisor.com","linktr.ee","business.site","godaddysites.com","wixsite.com","weebly.com","square.site"
}
PARKED_PHRASES = [
    "domain for sale", "buy this domain", "this domain has been registered",
    "future home of", "coming soon", "powered by godaddy", "sedo.com",
    "namecheap parking", "parkingcrew"
]
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; WebsiteFinder/1.0)"}

# Stopwords for dental tokenization
NAME_STOPWORDS = {
    "llc","inc","co","company","corp","corporation","the","and","&","llp","pllc","pc","p.c.","p.a.","pa","plc",
    "dental","dentistry","clinic","center","practice","group","associates","associate","family","of","at","for",
    "smile","smiles","studio","kids","children","pediatric","pediatrics","orthodontics","orthodontic",
    "endodontics","periodontics","oral","surgery","surgeons","dds","dmd","md","ms","dr","doctor"
}
# keep dental words in this variant
STOPWORDS_KEEP_DENTAL = NAME_STOPWORDS - {"dental","dentistry"}

VERTICALS = {
    "dental": ["dental","dentistry","dds","dmd","orthodontics","implant","hygienist","pediatric","periodontics","endodontics"]
}

# ---------- Helpers ----------
def rootify(u: str) -> str:
    if not u:
        return ""
    u = u.strip()
    if not (u.startswith("http://") or u.startswith("https://")):
        u = "https://" + u
    p = urlsplit(u)
    host = p.netloc.lower().replace("www.", "")
    if ":" in host:  # no odd ports
        host = host.split(":")[0]
    return urlunsplit((p.scheme, host, "", "", ""))

def host_of(u: str) -> str:
    if not u:
        return ""
    p = urlsplit(u if u.startswith("http") else "https://" + u)
    host = p.netloc.lower()
    return host[4:] if host.startswith("www.") else host

def is_directory_host(host: str) -> bool:
    h = (host or "").lower()
    return any(h == d or h.endswith("." + d) for d in DIRECTORY_HOSTS)

def looks_parked(text: str) -> bool:
    t = (text or "").lower()
    return any(ph in t for ph in PARKED_PHRASES)

def tokens(s: str, remove=set()):
    s = re.sub(r"[^a-z0-9]+", " ", (s or "").lower())
    return [w for w in s.split() if w and w not in remove]

def sld_from_host(host: str) -> str:
    parts = (host or "").split(".")
    return parts[-2] if len(parts) >= 2 else host

def clean_sld(sld: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (sld or "").lower())

def last4_digits(s: str) -> str:
    d = re.sub(r"\D", "", s or "")
    return d[-4:] if len(d) >= 4 else ""

def is_public_ip(addr: str) -> bool:
    try:
        ip = ipaddress.ip_address(addr)
        return ip.is_global
    except Exception:
        return False

def resolve_public(host: str) -> bool:
    try:
        infos = socket.getaddrinfo(host, 443, proto=socket.IPPROTO_TCP)
        for fam,_,_,_,sa in infos:
            ip = sa[0]
            if is_public_ip(ip):
                return True
        return False
    except Exception:
        return False

def read_small(url: str, sess: requests.Session, timeout_connect: float, timeout_read: float, max_bytes: int = 100_000):
    """HEAD then tiny GET. Returns (status_code, content_type, text<=max_bytes)."""
    try:
        r = sess.head(url, headers=HEADERS, allow_redirects=True, timeout=(timeout_connect, timeout_read))
        ct = (r.headers.get("content-type") or "").split(";")[0].strip().lower()
        sc = r.status_code
    except Exception:
        sc, ct = None, ""
    text = ""
    try:
        r = sess.get(url, headers=HEADERS, allow_redirects=True, stream=True, timeout=(timeout_connect, timeout_read))
        ct = (r.headers.get("content-type") or ct).split(";")[0].strip().lower()
        buf = b""
        for chunk in r.iter_content(chunk_size=4096):
            if chunk:
                buf += chunk
                if len(buf) >= max_bytes:
                    break
        try:
            text = buf.decode(r.encoding or "utf-8", errors="ignore")
        except Exception:
            text = buf.decode("utf-8", errors="ignore")
        if sc is None:
            sc = r.status_code
    except Exception:
        pass
    return sc, ct, text

def head_only(url: str, sess: requests.Session, timeout_connect: float, timeout_read: float):
    try:
        r = sess.head(url, headers=HEADERS, allow_redirects=True, timeout=(timeout_connect, timeout_read))
        ct = (r.headers.get("content-type") or "").split(";")[0].strip().lower()
        return r.status_code, ct
    except Exception:
        return None, ""

def name_overlap_ratio(name_tokens, sld_clean):
    if not name_tokens or not sld_clean:
        return 0.0
    present = sum(1 for w in name_tokens if w in sld_clean)
    return present / max(1, len(name_tokens))

# ---------- Candidate generation (dental-optimized) ----------
def generate_candidates(name: str, city: str, tlds, vertical_words):
    # Keep dental words; also a version without dental that we append dental/dentistry to
    toks_keep_dental = tokens(name, remove=STOPWORDS_KEEP_DENTAL)
    toks_no_dental = tokens(name, remove=NAME_STOPWORDS)

    joined_keep = "".join(toks_keep_dental)
    hyph_keep = "-".join(toks_keep_dental)
    core = "".join(toks_no_dental)

    bases = []
    if joined_keep:
        bases.append(joined_keep)
    if hyph_keep and hyph_keep != joined_keep:
        bases.append(hyph_keep)

    # append dental/dentistry to the core (no dental words)
    if core:
        bases.append(core + "dental")
        bases.append(core + "dentistry")

    # optional: include a short city suffix late in the list
    city_tok = re.sub(r"[^a-z0-9]+", "", (city or "").lower())
    if city_tok and len(city_tok) >= 4 and core:
        bases.append(core + city_tok)

    # build full domains by tld priority
    out = []
    seen = set()
    for b in bases:
        for tld in tlds:
            host = f"{b}{tld}"
            if host not in seen:
                seen.add(host)
                out.append(host)
    return out

# ---------- Scoring (only used at safety 2) ----------
def score_candidate(text: str, host: str, name: str, city: str, state: str, postcode: str, phone_last4: str, vertical_words: set) -> int:
    score = 0
    t = (text or "").lower()
    # name overlap
    ntoks = tokens(name, remove=NAME_STOPWORDS)
    if ntoks:
        ratio = name_overlap_ratio(ntoks, t)
        if ratio >= 0.8: score += 40
        elif ratio >= 0.5: score += 25
        elif ratio >= 0.3: score += 15
        elif ratio >= 0.15: score += 8
    # location
    if city and city.lower() in t: score += 10
    if state and state.lower() in t: score += 5
    if postcode and postcode in t: score += 10
    # phone tail
    if phone_last4 and phone_last4 in t: score += 20
    # vertical words
    if vertical_words:
        hits = sum(1 for w in vertical_words if w in t)
        if hits >= 3: score += 15
        elif hits == 2: score += 10
        elif hits == 1: score += 5
    # negatives
    if looks_parked(t): score -= 30
    if is_directory_host(host): score -= 40
    return score

# ---------- Core processing ----------
def process_row(row, cfg, sess):
    audit = {"osm_id": row.get("osm_id"), "name": row.get("name"), "city": row.get("city"), "state": row.get("state"),
             "existing": row.get("website") or "", "picked": "", "stage": "", "tried": []}
    name = (row.get("name") or "").strip()
    city = (row.get("city") or "").strip()
    state = (row.get("state") or "").strip()
    postcode = (row.get("postcode") or "").strip()
    website_existing = (row.get("website") or "").strip()
    email = (row.get("email") or "").strip()
    phone = (row.get("phone") or "").strip()

    # Skip/repair gate
    if website_existing:
        host = host_of(website_existing)
        if not is_directory_host(host) and not cfg["repair"]:
            audit["stage"] = "skip_existing"
            return row, audit, False  # unchanged
        if is_directory_host(host) and not cfg["repair"]:
            audit["stage"] = "skip_dir_existing"
            return row, audit, False

    # Stage 1: email-derived domain (offline-safe)
    picked = ""
    if email:
        # emails may be semicolon-separated
        parts = [e.strip() for e in re.split(r"[;, ]+", email) if e.strip()]
        for e in parts:
            m = re.search(r"@([A-Za-z0-9.-]+\.[A-Za-z]{2,})$", e)
            if not m: continue
            dom = m.group(1).lower()
            if dom in FREE_EMAIL_DOMAINS: continue
            # TLD allowlist
            if not any(dom.endswith(tld) for tld in cfg["tlds"]): continue
            if dom.startswith("xn--"): continue
            if is_directory_host(dom): continue
            # Offline accept at safety 0; else confirm with HEAD
            url = rootify("https://" + dom)
            if cfg["safety"] == 0:
                picked = url
                audit["tried"].append({"email_domain": dom, "accepted": True, "why": "offline_email_domain"})
                audit["stage"] = "email_domain_offline"
                row["website"] = picked
                return row, audit, True
            else:
                if resolve_public(dom):
                    sc, ct = head_only(url, sess, cfg["timeout_connect"], cfg["timeout_read"])
                    if sc and sc < 400 and (ct == "" or ct.startswith("text") or ct.startswith("application/xhtml")):
                        picked = url
                        audit["tried"].append({"email_domain": dom, "accepted": True, "why": f"HEAD {sc} {ct}"})
                        audit["stage"] = "email_domain_head"
                        row["website"] = picked
                        return row, audit, True
                    audit["tried"].append({"email_domain": dom, "accepted": False, "why": f"HEAD {sc} {ct}"})
                else:
                    audit["tried"].append({"email_domain": dom, "accepted": False, "why": "DNS_non_public"})

    # Stage 2: deterministic guessing (dental forms)
    tlds = cfg["tlds"]
    cand_hosts = generate_candidates(name, city, tlds, cfg["vertical_words"])
    # Pre-gate by name overlap before any network
    name_toks = tokens(name, remove=NAME_STOPWORDS)
    tried_count = 0
    for host in cand_hosts:
        if tried_count >= cfg["max_candidates"]:
            break
        tried_count += 1

        # quick static gates
        if is_directory_host(host): 
            audit["tried"].append({"host": host, "accepted": False, "why": "directory_host"})
            continue
        if host.startswith("xn--"):
            audit["tried"].append({"host": host, "accepted": False, "why": "punycode"})
            continue
        if not any(host.endswith(tld) for tld in tlds):
            audit["tried"].append({"host": host, "accepted": False, "why": "tld_not_allowed"})
            continue

        sldc = clean_sld(sld_from_host(host))
        overlap = name_overlap_ratio(name_toks, sldc)
        if overlap < 0.5:
            audit["tried"].append({"host": host, "accepted": False, "why": f"low_name_overlap:{overlap:.2f}"})
            continue

        # DNS gate (public IP only)
        if not resolve_public(host):
            audit["tried"].append({"host": host, "accepted": False, "why": "DNS_non_public"})
            continue

        url = "https://" + host
        if cfg["safety"] == 0:
            picked = rootify(url)
            audit["tried"].append({"host": host, "accepted": True, "why": "dns_only_accept"})
            audit["stage"] = "guess_dns_only"
            row["website"] = picked
            return row, audit, True

        elif cfg["safety"] == 1:
            sc, ct = head_only(url, sess, cfg["timeout_connect"], cfg["timeout_read"])
            if sc and sc < 400 and (ct == "" or ct.startswith("text") or ct.startswith("application/xhtml")):
                picked = rootify(url)
                audit["tried"].append({"host": host, "accepted": True, "why": f"HEAD {sc} {ct}"})
                audit["stage"] = "guess_head"
                row["website"] = picked
                return row, audit, True
            else:
                audit["tried"].append({"host": host, "accepted": False, "why": f"HEAD {sc} {ct}"})

        else:  # safety 2: tiny GET + score
            sc, ct, text = read_small(url, sess, cfg["timeout_connect"], cfg["timeout_read"], cfg["max_bytes"])
            if sc and sc < 400 and ct.startswith("text"):
                score = score_candidate(text, host, name, city, state, postcode, last4_digits(phone), set(cfg["vertical_words"]))
                audit["tried"].append({"host": host, "accepted": score >= cfg["threshold"], "score": score})
                if score >= cfg["threshold"]:
                    picked = rootify(url)
                    audit["stage"] = "guess_get_score"
                    row["website"] = picked
                    return row, audit, True
            else:
                audit["tried"].append({"host": host, "accepted": False, "why": f"GET {sc} {ct}"})

    # Nothing accepted
    audit["stage"] = "no_match"
    return row, audit, False

# ---------- CLI ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="Input CSV")
    ap.add_argument("--out", required=True, help="Output CSV (same schema)")
    ap.add_argument("--audit", default="finder_audit.jsonl", help="Audit JSONL path")
    ap.add_argument("--vertical", default="dental", help="Preset vertical (default: dental)")
    ap.add_argument("--vertical-words", default="", help="Comma-separated extra vertical tokens to try/score")
    ap.add_argument("--tlds", default=",".join(DEFAULT_TLDS), help="Allowed TLDs, comma-separated")
    ap.add_argument("--safety", type=int, default=1, choices=[0,1,2], help="0=offline+DNS, 1=HEAD (default), 2=tiny GET")
    ap.add_argument("--threshold", type=int, default=70, help="Score threshold at safety=2")
    ap.add_argument("--max-candidates", type=int, default=6, help="Max candidate hosts per row")
    ap.add_argument("--timeout-connect", type=float, default=3.5)
    ap.add_argument("--timeout-read", type=float, default=4.5)
    ap.add_argument("--max-bytes", type=int, default=100_000)
    ap.add_argument("--repair", type=int, default=0, help="1=replace directory/social 'websites'")
    ap.add_argument("--limit", type=int, default=0, help="Process at most N rows")
    args = ap.parse_args()

    tlds = [t.strip() for t in args.tlds.split(",") if t.strip().startswith(".")]
    vertical_words = VERTICALS.get(args.vertical.lower(), [])
    if args.vertical_words.strip():
        vertical_words = list(dict.fromkeys(vertical_words + [w.strip().lower() for w in args.vertical_words.split(",") if w.strip()]))

    cfg = {
        "tlds": tlds,
        "vertical_words": vertical_words,
        "safety": args.safety,
        "threshold": args.threshold,
        "max_candidates": args.max_candidates,
        "timeout_connect": args.timeout_connect,
        "timeout_read": args.timeout_read,
        "max_bytes": args.max_bytes,
        "repair": bool(args.repair),
    }

    with open(args.inp, newline="", encoding="utf-8") as fin, \
         open(args.out, "w", newline="", encoding="utf-8") as fout, \
         open(args.audit, "w", encoding="utf-8") as faud, \
         requests.Session() as sess:

        rdr = csv.DictReader(fin)
        fieldnames = rdr.fieldnames or []
        required = {"name","city","state","postcode","website","email"}
        missing = [c for c in required if c not in fieldnames]
        if missing:
            sys.exit(f"Input missing required columns: {missing}")

        wtr = csv.DictWriter(fout, fieldnames=fieldnames)
        wtr.writeheader()

        processed = 0
        updated = 0
        skipped = 0
        errors = 0
        start = time.time()

        for row in rdr:
            if args.limit and processed >= args.limit:
                break
            processed += 1
            try:
                new_row, audit, changed = process_row(row, cfg, sess)
                if changed:
                    updated += 1
                else:
                    # Count purely skipped cases (existing good site)
                    if audit.get("stage","").startswith("skip"):
                        skipped += 1
                wtr.writerow(new_row)
                faud.write(json.dumps(audit, ensure_ascii=False) + "\n")
            except Exception as e:
                errors += 1
                # Write original row to keep alignment
                wtr.writerow(row)
                faud.write(json.dumps({"osm_id": row.get("osm_id"), "error": str(e)}, ensure_ascii=False) + "\n")

            if processed % 100 == 0:
                elapsed = time.time() - start
                rate = processed / elapsed if elapsed > 0 else 0.0
                print(f"[{processed}] updated={updated} skipped={skipped} errors={errors} rate={rate:.1f} r/s")

        elapsed = time.time() - start
        print("✅ Done")
        print(f"Processed: {processed}, Updated: {updated}, Skipped: {skipped}, Errors: {errors}, Time: {elapsed:.1f}s")

if __name__ == "__main__":
    main()
