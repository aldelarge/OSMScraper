#!/usr/bin/env python3
# Ultra-safe email + phone cleaner: drop obvious junk emails, normalize phones (supports multiple),
# add lead_score (0–12) and lead_grade (A/B/C/D), optional dedupe.
import csv, re, argparse, os, sys
from urllib.parse import urlsplit

# ---------- Helpers for IDs / text ----------
def _norm_id(osm_id: str) -> str:
    """Return numeric part if id looks like 'type:123', else the id itself."""
    if not osm_id:
        return ""
    osm_id = str(osm_id).strip()
    return osm_id.split(":", 1)[1] if ":" in osm_id else osm_id

def _norm_text(x: str) -> str:
    return re.sub(r"\s+", " ", (x or "").strip().lower())

# ---------- Email cleaning (conservative — only drop proven junk) ----------
EMAIL_RE = re.compile(r"\b[^@\s;]+@[^@\s;]+\.[A-Za-z]{2,63}\b", re.I)

# Static/asset-y endings that are never inboxes (covers ig-badge-view-sprite-24@2x.png)
ASSET_EXTS = (".png",".jpg",".jpeg",".gif",".svg",".webp",".ico",".js",".css",".json",".xml")

# Exact addresses we never want (placeholders, etc.)
EXACT_BLACKLIST = {
    "user@domain.com",
}

# Domains that are obviously machine/no-contact
JUNK_DOMAINS_EXACT = {
    "group.calendar.google.com",
}

# If these substrings appear anywhere in the domain, treat as junk
JUNK_DOMAINS_SUBSTR = (
    "sentry.",        # sentry.io, ingest.sentry.io, sentry-next.wixpress.com, etc.
    "wixpress.",      # *.wixpress.com
)

# Random long hex localparts (common in Sentry-style addresses)
HEX_LOCALPART_RE = re.compile(r"^[a-f0-9]{20,}$", re.I)

def should_drop_email(e: str) -> bool:
    """Conservative junk test: drop only when clearly junk."""
    elow = e.lower()

    # Looks like an asset path masquerading as an email
    if any(elow.endswith(ext) for ext in ASSET_EXTS):
        return True

    if "@" not in elow:
        return True

    if elow in EXACT_BLACKLIST:
        return True

    local, dom = elow.split("@", 1)

    # Known junk domains (exact or substring)
    if dom in JUNK_DOMAINS_EXACT:
        return True
    if any(sub in dom for sub in JUNK_DOMAINS_SUBSTR):
        return True

    # Random long hex blobs
    if HEX_LOCALPART_RE.match(local):
        return True

    # Otherwise, keep it
    return False

def extract_emails(text: str, debug=False):
    """Return a list of *kept* emails (lowercased), preserving order and de-duped."""
    if not text:
        return []
    cands = [m.group(0) for m in EMAIL_RE.finditer(text)]
    out, seen = [], set()
    for e in cands:
        el = e.lower()
        if should_drop_email(el):
            if debug:
                print(f"[drop] {el}")
            continue
        if el not in seen:
            seen.add(el)
            out.append(el)
    return out

# ---------- Phone cleaning ----------
DIGITS_RE = re.compile(r"\D")

def clean_phone(raw: str) -> str:
    """Return normalized US phone or '' if invalid."""
    if not raw:
        return ""
    digits = DIGITS_RE.sub("", raw)
    if len(digits) == 10:
        return f"+1-{digits[0:3]}-{digits[3:6]}-{digits[6:]}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits[0]}-{digits[1:4]}-{digits[4:7]}-{digits[7:]}"
    return ""  # bad/incomplete → blank

def split_multi_phones(s: str):
    """Split by common separators but NOT by spaces (to avoid breaking formats)."""
    if not s:
        return []
    # unify separators to ';'
    tmp = s.replace(",", ";").replace("|", ";").replace("/", ";")
    parts = [p.strip() for p in tmp.split(";")]
    return [p for p in parts if p]

# ---------- Lead scoring ----------
FREE_EMAIL_DOMAINS = {
    "gmail.com","yahoo.com","outlook.com","hotmail.com","aol.com","icloud.com",
    "proton.me","protonmail.com","gmx.com","gmx.de","yandex.com","mail.com",
    "live.com","msn.com","me.com","zoho.com","pm.me"
}

def _host_from_url(url: str) -> str:
    if not url: return ""
    try:
        h = urlsplit(url).hostname or ""
        return h.lower().lstrip("www.")
    except Exception:
        return ""

def _is_free_domain(dom: str) -> bool:
    return dom in FREE_EMAIL_DOMAINS

def _email_domain(e: str) -> str:
    try:
        return e.split("@",1)[1].lower()
    except Exception:
        return ""

def _domains_match(email_dom: str, website_url: str) -> bool:
    if not email_dom or not website_url: return False
    host = _host_from_url(website_url)
    if not host: return False
    # allow subdomain matches either way
    return email_dom == host or email_dom.endswith("." + host) or host.endswith("." + email_dom)

def compute_lead_score(row, email_col="email", phone_col="phone", website_cols=("website","contact:website")):
    """Score 0–12, grade A/B/C/D. Never deletes content—reads already-cleaned fields."""
    emails = [e for e in (row.get(email_col) or "").split(";") if e]
    phones = [p for p in (row.get(phone_col) or "").split(";") if p]

    # pick first website-like value if present
    website = ""
    for wc in website_cols:
        if row.get(wc):
            website = row.get(wc)
            break

    score = 0
    # emails
    if emails:
        score += 3                        # has email
        score += min(len(emails)-1, 2)    # +1 per extra email (cap +2)

        # business vs free
        doms = {_email_domain(e) for e in emails}
        if any(d and not _is_free_domain(d) for d in doms):
            score += 2                    # has a business-domain email

        # domain alignment with website
        if website:
            if any(_domains_match(d, website) for d in doms if d):
                score += 2                # email domain matches site

    # phones
    if phones:
        score += 3                        # has phone
        if len(phones) > 1:
            score += 1                    # multiple phones

    # basic location completeness (optional nudge)
    city_ok = bool((row.get("city") or "").strip())
    state_ok = bool((row.get("state") or "").strip())
    if city_ok and state_ok:
        score += 1

    # clamp + grade
    score = max(0, min(score, 12))
    grade = "A" if score >= 9 else "B" if score >= 6 else "C" if score >= 3 else "D"
    return score, grade

# ---------- Main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="Input CSV")
    ap.add_argument("--email-column", default="email", help="Email column name (default: email)")
    ap.add_argument("--phone-column", default="phone", help="Phone column name (default: phone)")

    # dedupe options
    ap.add_argument("--dedupe", type=int, default=1, help="1=enable dedupe (default), 0=off")
    ap.add_argument("--id-column", default="osm_id", help="OSM ID column (default: osm_id)")
    ap.add_argument("--name-column", default="name", help="Name column (default: name)")
    ap.add_argument("--city-column", default="city", help="City column (default: city)")
    ap.add_argument("--state-column", default="state", help="State column (default: state)")

    ap.add_argument("--debug", type=int, default=0, help="1=print dropped emails (per-row)")

    args = ap.parse_args()

    if not os.path.exists(args.inp):
        print(f"Input not found: {args.inp}"); sys.exit(1)

    base, ext = os.path.splitext(args.inp)
    out = base + ".ultrasafe.csv"

    with open(args.inp, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        rows = list(r)
        raw_fieldnames = r.fieldnames or []
        fieldnames = [fn.strip() for fn in raw_fieldnames if fn is not None and str(fn).strip()]

    # remove any accidental None-key column from each row
    for row in rows:
        if None in row:
            del row[None]

    # ensure email/phone columns exist in header
    if args.email_column not in fieldnames:
        fieldnames.append(args.email_column)
    if args.phone_column not in fieldnames:
        fieldnames.append(args.phone_column)

    # ensure scoring columns exist
    for extra_col in ("lead_score","lead_grade"):
        if extra_col not in fieldnames:
            fieldnames.append(extra_col)

    rows_processed = len(rows)
    emails_kept = 0
    emails_dropped = 0
    phones_kept = 0
    rows_changed = 0

    kept_rows = []
    seen_ids = set()
    seen_name_loc = set()
    dupes_skipped = 0

    for row in rows:
        # ----- Emails -----
        raw_email = (row.get(args.email_column) or "").strip()
        # candidates before filtering (to count drops safely)
        cands = [m.group(0).lower() for m in EMAIL_RE.finditer(raw_email)] if raw_email else []
        filtered = extract_emails(raw_email, debug=bool(args.debug))
        new_email = ";".join(filtered)
        new_email = re.sub(r";{2,}", ";", new_email).strip("; ").strip()
        if new_email != raw_email:
            rows_changed += 1
        row[args.email_column] = new_email
        emails_kept += len(filtered)
        emails_dropped += max(len(cands) - len(filtered), 0)

        # ----- Phones (support multiple) -----
        raw_phone = (row.get(args.phone_column) or "").strip()
        tokens = split_multi_phones(raw_phone) if raw_phone else []
        cleaned_list = []
        for tok in tokens:
            norm = clean_phone(tok)
            if norm:
                cleaned_list.append(norm)
        # de-dupe preserving order
        cleaned_list = list(dict.fromkeys(cleaned_list))
        new_phone = ";".join(cleaned_list)
        if new_phone != raw_phone:
            rows_changed += 1
        row[args.phone_column] = new_phone
        phones_kept += len(cleaned_list)

        # ----- Scoring (after cleaning) -----
        score, grade = compute_lead_score(
            row,
            email_col=args.email_column,
            phone_col=args.phone_column,
            website_cols=("website","contact:website")
        )
        row["lead_score"] = score
        row["lead_grade"] = grade

        # ----- DEDUPE (OSM id first, then name+city+state) -----
        if args.dedupe:
            # 1) OSM id (treat 'node:123' and '123' as same)
            oid = _norm_id(row.get(args.id_column, ""))
            if oid:
                if oid in seen_ids:
                    dupes_skipped += 1
                    continue
                seen_ids.add(oid)
            else:
                # 2) Fallback: name + city + state
                nm = _norm_text(row.get(args.name_column, ""))
                city = _norm_text(row.get(args.city_column, ""))
                state = (row.get(args.state_column, "") or "").strip().upper()
                key = (nm, city, state)
                if key in seen_name_loc:
                    dupes_skipped += 1
                    continue
                seen_name_loc.add(key)

        kept_rows.append(row)

    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(kept_rows)

    print(f"Rows processed:             {rows_processed}")
    print(f"Emails kept (total):        {emails_kept}")
    print(f"Emails dropped (junk):      {emails_dropped}")
    print(f"Phones kept (total):        {phones_kept}")
    print(f"Rows changed (cleaned):     {rows_changed}")
    if args.dedupe:
        print(f"Duplicates removed:         {dupes_skipped}")
        print(f"Rows written:               {len(kept_rows)}")
    else:
        print(f"Rows written:               {len(kept_rows)} (no dedupe)")

if __name__ == "__main__":
    main()
