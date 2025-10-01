#!/usr/bin/env python3
"""
(CSV Enricher) — Polished + Robust
- Reads websites from your CSV (no API pulling)
- Enriches with website email/socials/tech-stack (best-effort, polite)
- Handles age-gates, Cloudflare obfuscation, sitemap contact discovery
- Cleans/normalizes: url cleanup, phone normalization, email_status, lead_grade
- Appends to an output CSV (can resume; tolerant to unknown input column names)

Usage:
  python website_enrich.py --in all_breweries_usa.csv --out enriched_breweries.csv --limit 500 --throttle 1.0
"""

import csv
import re
import time
import argparse
import os, json, signal, sys, io
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from urllib.parse import urljoin, urlsplit, urlunsplit, parse_qsl

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from lxml import html as lxml_html

# For hard per-row wall clock protection (optional but recommended)
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, TimeoutError as ProcTimeout

# ---------- timeouts / limits ----------
DEFAULT_CONNECT_TIMEOUT = 6
DEFAULT_READ_TIMEOUT = 12
MAX_BYTES_PER_PAGE = 1_500_000   # ~1.5 MB cap per fetch
MAX_FETCHES_PER_ROW = 8          # homepage + contact + robots + sitemap + variants, etc.
ROW_WATCHDOG_SEC = 75            # hard stop per row (wall clock)

# --- short backoff for repeatedly failing/toxic hosts ---
BAD_HOST_TTL_SEC = 30 * 60  # 30 minutes
MAX_HOST_FAILS   = 2
_HOST_FAILS = {}   # host -> consecutive fail count
_BAD_HOSTS  = {}   # host -> expires_at (monotonic seconds)

try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

def _host(u: str) -> str:
    try:
        return urlsplit(u).netloc.lower().replace("www.", "")
    except Exception:
        return ""

def _bad_host(host: str) -> bool:
    if not host:
        return False
    exp = _BAD_HOSTS.get(host)
    if not exp:
        return False
    if time.monotonic() > exp:
        _BAD_HOSTS.pop(host, None)
        _HOST_FAILS.pop(host, None)
        return False
    return True

def _mark_host_failure(url_or_host: str):
    h = url_or_host if (url_or_host and "/" not in url_or_host) else _host(url_or_host)
    if not h:
        return
    _HOST_FAILS[h] = _HOST_FAILS.get(h, 0) + 1
    if _HOST_FAILS[h] >= MAX_HOST_FAILS:
        _BAD_HOSTS[h] = time.monotonic() + BAD_HOST_TTL_SEC

# -------------------------- HTTP session (cookies + retries) --------------------------

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; BreweryLeadBot/1.2; +https://openbrewerydb.org/)",
    "Accept-Language": "en-US,en;q=0.8",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

_retry = Retry(
    total=3,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods={"GET"},
    respect_retry_after_header=True,
    raise_on_status=False,
)
_adapter = HTTPAdapter(max_retries=_retry, pool_connections=20, pool_maxsize=20)
SESSION.mount("http://", _adapter)
SESSION.mount("https://", _adapter)

# -------------------------- parsing helpers --------------------------

_XML_DECL_RE = re.compile(r'^\s*<\?xml[^>]*\?>', re.IGNORECASE)
EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)

# Fallback/scan patterns
PHONE_SCAN_RE = re.compile(r"(?:\+?1[\s\-\.)]?)?(?:\(?\d{3}\)?[\s\-.]?)\d{3}[\s\-.]?\d{4}")
DIGITS_RE = re.compile(r"\D")

ENRICH_STATS_PATH = os.environ.get("ENRICH_STATS_PATH", "enrich_stats.json")

def _write_enrich_stats(path, stats: dict):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(stats, f)
    os.replace(tmp, path)

def to_html_doc(html_text: str):
    """Return an lxml HtmlElement from possibly wonky HTML/XML."""
    if not html_text:
        return None
    cleaned = _XML_DECL_RE.sub("", html_text)
    try:
        return lxml_html.fromstring(cleaned.encode("utf-8", "ignore"))
    except Exception:
        return lxml_html.fromstring(cleaned)

def compose_address(street, city, state, postal):
    parts = [p for p in [street, city, state, postal] if p]
    return ", ".join(parts)

def absolutize(href: str, base_url: str) -> str:
    if not href:
        return ""
    if href.startswith("http://") or href.startswith("https://"):
        return href
    return urljoin(base_url, href)

# ---- watchdog helpers ----
def row_budget_init():
    return {
        "max": MAX_FETCHES_PER_ROW,
        "fetches": 0,
        "deadline": time.monotonic() + ROW_WATCHDOG_SEC,
    }

def row_time_left(budget) -> float:
    return budget["deadline"] - time.monotonic()

def maybe_sleep(seconds: float, budget):
    # Never sleep past the deadline
    if seconds <= 0:
        return
    remaining = row_time_left(budget)
    if remaining <= 0:
        return
    time.sleep(min(seconds, remaining))

# -------------------------- network fetchers (streaming, capped) --------------------------

def fetch_html(url: str,
               timeout=(DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT),
               session: requests.Session = SESSION,
               max_bytes: int = MAX_BYTES_PER_PAGE,
               budget=None):
    """
    Stream HTML with a byte cap; respect per-row budget/deadline.
    Returns (text, final_url) or (None, None).
    """
    if not url:
        return None, None
    if not url.startswith(("http://", "https://")):
        url = "http://" + url

    # Back off toxic hosts fast
    if _bad_host(_host(url)):
        return None, None

    # Budget/deadline checks
    if budget is not None:
        if row_time_left(budget) <= 0 or budget["fetches"] >= budget["max"]:
            return None, None
        budget["fetches"] += 1

    try:
        r = session.get(url, timeout=timeout, allow_redirects=True, stream=True)
        with session.get(url, timeout=timeout, allow_redirects=True, stream=True) as r:
            ctype = (r.headers.get("content-type") or "").lower()
            if not (200 <= r.status_code < 400):
                _mark_host_failure(url)
                return None, None
            if ("text/html" not in ctype) and ("application/xhtml+xml" not in ctype):
                _mark_host_failure(url)
                return None, None

            total = 0
            chunks = []
            for chunk in r.iter_content(chunk_size=8192):
                if not chunk:
                    break
                total += len(chunk)
                if total > max_bytes:
                    break
                chunks.append(chunk)
                if budget is not None and row_time_left(budget) <= 0:
                    break
            text = b"".join(chunks).decode(r.encoding or "utf-8", errors="ignore")
            return text, r.url

    except requests.RequestException:
        _mark_host_failure(url)
        return None, None

def _root_url(u: str) -> str:
    try:
        p = urlsplit(u)
        return urlunsplit((p.scheme or "https", p.netloc, "", "", ""))
    except Exception:
        return u

def _fetch_text(u: str,
                timeout=(DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT),
                max_bytes: int = MAX_BYTES_PER_PAGE,
                budget=None) -> str:
    """
    Get text-ish content (robots.txt, sitemap.xml). Accepts text/* or xml types.
    """
    if not u:
        return ""
    if _bad_host(_host(u)):
        return ""
    if budget is not None:
        if row_time_left(budget) <= 0 or budget["fetches"] >= budget["max"]:
            return ""
        budget["fetches"] += 1
    try:
        r = SESSION.get(u, timeout=timeout, allow_redirects=True, stream=True)
        if not (200 <= r.status_code < 400):
            _mark_host_failure(u)
            return ""
        ctype = (r.headers.get("content-type") or "").lower()
        if not (ctype.startswith("text/") or "xml" in ctype):
            _mark_host_failure(u)
            return ""
        total = 0
        chunks = []
        for chunk in r.iter_content(chunk_size=8192):
            if not chunk:
                break
            total += len(chunk)
            if total > max_bytes:
                break
            chunks.append(chunk)
            if budget is not None and row_time_left(budget) <= 0:
                break
        return b"".join(chunks).decode(r.encoding or "utf-8", errors="ignore")
    except requests.RequestException:
        _mark_host_failure(u)
        return ""

# -------------------------- email extraction boosters --------------------------

# Age-gate detection
AGE_GATE_SIGNS = (
    "you must be 21", "over 21", "21+", "age verification",
    "verify your age", "are you over 21", "date of birth"
)
def looks_like_age_gate(html: str) -> bool:
    h = (html or "").lower()
    return any(sig in h for sig in AGE_GATE_SIGNS)

# Cloudflare email obfuscation decode
def _cf_decode(hexstr: str) -> str:
    try:
        key = int(hexstr[:2], 16)
        out = []
        for i in range(2, len(hexstr), 2):
            out.append(chr(int(hexstr[i:i+2], 16) ^ key))
        return "".join(out)
    except Exception:
        return ""

# Text de-obfuscation (info [at] example [dot] com)
_OB_PATTERNS = [
    (r"\s*\[\s*at\s*\]\s*", "@"),
    (r"\s*\(\s*at\s*\)\s*", "@"),
    (r"\s+at\s+", "@"),
    (r"\s*\[\s*dot\s*\]\s*", "."),
    (r"\s*\(\s*dot\s*\)\s*", "."),
    (r"\s+dot\s+", "."),
]
def deobfuscate_text(text: str) -> str:
    t = text or ""
    for pat, repl in _OB_PATTERNS:
        t = re.sub(pat, repl, t, flags=re.I)
    return t

def extract_emails(text: str):
    # Prefer business-y emails; still keep others if nothing else found
    found = set(m.group(0).lower() for m in EMAIL_RE.finditer(text or ""))
    if not found:
        return set()
    priority_prefixes = ("info@", "contact@", "hello@", "support@", "sales@", "orders@", "booking@", "press@")
    biz = {e for e in found if e.startswith(priority_prefixes)}
    non_free = {e for e in found if not any(f"@{d}." in e for d in ["gmail", "yahoo", "hotmail", "proton", "outlook", "aol", "icloud"])}
    return biz or non_free or found

# Local-part role prefixes (without the "@")
PRIO_LOCAL_PREFIXES = ('info','contact','sales','hello','orders','booking','press','support')

def extract_emails_rich(doc: lxml_html.HtmlElement, html_text: str):
    """Regex + mailto + Cloudflare (data-cfemail & link) + JSON-LD + noscript + email-ish attrs."""
    emails = set()

    # 1) Plain text (with deobfuscation)
    if html_text:
        emails.update(extract_emails(deobfuscate_text(html_text)))

    if doc is not None:
        # 2) mailto: addresses in href  (fixed .startswith)
        for a in doc.xpath("//a[@href]"):
            href = a.get("href") or ""
            if (href or "").lower().startswith("mailto:"):
                addr = href[7:].split("?", 1)[0]
                for part in re.split(r"[;,]", addr):
                    emails.update(extract_emails(part))

        # 3) Cloudflare obfuscation: data-cfemail
        for el in doc.xpath("//*[@data-cfemail]"):
            decoded = _cf_decode(el.get("data-cfemail") or "")
            if decoded:
                emails.update(extract_emails(decoded))

        # 4) Cloudflare obfuscation: /cdn-cgi/l/email-protection#HEX
        for a in doc.xpath("//a[starts-with(@href, '/cdn-cgi/l/email-protection')]"):
            href = a.get("href") or ""
            if "#" in href:
                hexpart = href.split("#", 1)[1]
                decoded = _cf_decode(hexpart)
                if decoded:
                    emails.update(extract_emails(decoded))

        # 5) JSON-LD email fields
        for s in doc.xpath("//script[@type='application/ld+json']"):
            try:
                data = json.loads(s.text or "")
            except Exception:
                continue
            def _walk(x):
                if isinstance(x, dict):
                    for k, v in x.items():
                        if isinstance(v, str) and k.lower() == "email":
                            emails.update(extract_emails(v))
                        else:
                            _walk(v)
                elif isinstance(x, list):
                    for it in x: _walk(it)
            _walk(data)

        # 6) noscript
        for ns in doc.xpath("//noscript"):
            emails.update(extract_emails(deobfuscate_text(ns.text_content() or "")))

        # 7) elements with email-ish attributes/classes/ids
        for el in doc.xpath(
            "//*[@data-email or @data-mail or "
            "contains(translate(@class,'EMAIL','email'),'email') or "
            "contains(translate(@id,'EMAIL','email'),'email')]"
        ):
            bits = [
                el.get("data-email", ""), el.get("data-mail", ""),
                el.get("content", ""), el.text_content() or ""
            ]
            emails.update(extract_emails(deobfuscate_text(" ".join(bits))))

    return sorted(emails)

# -------------------------- contact discovery helpers --------------------------

# try common contact/visit/about/legal/careers paths
_CONTACT_PATHS = (
    "/contact", "/contact/", "/contact-us", "/contact-us/", "/contactus",
    "/visit", "/visit-us", "/about", "/about-us", "/find-us",
    "/privacy", "/privacy-policy", "/terms", "/terms-and-conditions",
    "/jobs", "/careers"
)

def try_contact_variants(base_url: str, throttle: float, budget=None):
    for path in _CONTACT_PATHS:
        u = urljoin(base_url, path)
        c_html, _ = fetch_html(u, budget=budget)
        if c_html:
            maybe_sleep(throttle, budget)
            yield u, c_html

CONTACT_HINTS_RE = re.compile(r"/(contact|contact-us|about|visit|find-us|locations|privacy|terms|careers|jobs)(/|\.|$)", re.I)

def _sitemap_urls(xml_text: str):
    urls = []
    try:
        tree = ET.parse(io.StringIO(xml_text))
        root = tree.getroot()
        ns = "{http://www.sitemaps.org/schemas/sitemap/0.9}"
        for loc in root.findall(f".//{ns}loc"):
            if loc.text:
                urls.append(loc.text.strip())
    except Exception:
        urls += re.findall(r"<loc>\s*(https?://[^<]+)\s*</loc>", xml_text or "", flags=re.I)
    return urls

def find_contact_via_sitemap(base_url: str, throttle: float, budget=None):
    root = _root_url(base_url)
    # robots.txt → Sitemap:
    robots = _fetch_text(urljoin(root, "/robots.txt"), budget=budget)
    if robots:
        for line in robots.splitlines():
            if line.lower().startswith("sitemap:"):
                sm_url = line.split(":", 1)[1].strip()
                sm = _fetch_text(sm_url, budget=budget)
                if sm:
                    maybe_sleep(throttle, budget)
                    for u in _sitemap_urls(sm):
                        if CONTACT_HINTS_RE.search(u):
                            return u
    # /sitemap.xml fallback
    sm = _fetch_text(urljoin(root, "/sitemap.xml"), budget=budget)
    if sm:
        maybe_sleep(throttle, budget)
        for u in _sitemap_urls(sm):
            if CONTACT_HINTS_RE.search(u):
                return u
    return ""

# -------------------------- socials / phones / product polish --------------------------

def extract_socials(doc: lxml_html.HtmlElement, base_url: str):
    out = {"instagram": "", "facebook": "", "tiktok": ""}
    for a in doc.xpath("//a[@href]"):
        href = a.get("href") or ""
        href_abs = absolutize(href, base_url)
        h = href_abs.lower()
        if "instagram.com" in h and not out["instagram"]:
            out["instagram"] = href_abs.split("?")[0]
        elif "facebook.com" in h and not out["facebook"]:
            out["facebook"] = href_abs.split("?")[0]
        elif "tiktok.com" in h and not out["tiktok"]:
            out["tiktok"] = href_abs.split("?")[0]
        if out["instagram"] and out["facebook"] and out["tiktok"]:
            break
    return out

def clean_phone(raw: str) -> str:
    """
    Normalize to +1-AAA-EEE-NNNN and basic NANP rules.
    """
    if not raw:
        return ""
    d = DIGITS_RE.sub("", raw)
    if len(d) == 11 and d.startswith("1"):
        d = d[1:]
    if len(d) != 10:
        return ""
    area, exch, line = d[0:3], d[3:6], d[6:10]
    if area[0] in "01" or exch[0] in "01":
        return ""
    if area == "000" or exch == "000" or exch == "555":
        return ""
    return f"+1-{area}-{exch}-{line}"

def extract_phones(doc: lxml_html.HtmlElement, html_text: str):
    """Find phone candidates (tel: links + page text), normalize, dedupe."""
    candidates = set()
    if doc is not None:
        for a in doc.xpath("//a[starts-with(translate(@href,'TEL','tel'),'tel:')]"):
            href = (a.get("href") or "").strip()
            if href.lower().startswith("tel:"):
                candidates.add(href[4:])
    text = ((doc.text_content() if doc is not None else "") or "") + " " + (html_text or "")
    for m in PHONE_SCAN_RE.finditer(text):
        candidates.add(m.group(0))
    out = []
    seen = set()
    for c in candidates:
        norm = clean_phone(c)
        if norm and norm not in seen:
            seen.add(norm)
            out.append(norm)
    return out

def extract_tel_links(doc: lxml_html.HtmlElement):
    if doc is None:
        return []
    out, seen = [], set()
    for a in doc.xpath("//a[starts-with(translate(@href,'TEL','tel'),'tel:')]"):
        href = (a.get("href") or "").strip()
        if href.lower().startswith("tel:"):
            norm = clean_phone(href[4:])
            if norm and norm not in seen:
                seen.add(norm)
                out.append(norm)
    return out

def split_multi(s: str):
    if not s:
        return []
    tmp = s.replace(",", ";").replace("|", ";").replace("/", ";")
    parts = [p.strip() for p in tmp.split(";")]
    return [p for p in parts if p]

TRACK_QS = {'utm_source','utm_medium','utm_campaign','utm_term','utm_content','fbclid','gclid','mc_cid','mc_eid'}
def clean_url(u):
    if not u: return ''
    try:
        p = urlsplit(u)
        qs = [(k,v) for k,v in parse_qsl(p.query, keep_blank_values=True) if k.lower() not in TRACK_QS]
        return urlunsplit((p.scheme, p.netloc.lower().replace('www.',''), p.path, "&".join([f"{k}={v}" for k,v in qs]), ''))
    except Exception:
        return u

def clean_urls_in_row(r):
    for k in ['website','instagram','facebook','tiktok','contact_page_url']:
        r[k] = clean_url(r.get(k,''))
    return r

def format_us_phones_field(s):
    phones = split_multi(s or "")
    cleaned = []
    seen = set()
    for p in phones:
        norm = clean_phone(p)
        if not norm:
            continue
        nd = norm.replace("+1-", "").replace("-", "")
        area, exch = nd[0:3], nd[3:6]
        if area == "000" or exch == "000" or exch == "555":
            continue
        if norm not in seen:
            seen.add(norm)
            cleaned.append(norm)
        if len(cleaned) >= 2:
            break
    return ";".join(cleaned)

FREE_DOMAINS = ('gmail.','yahoo.','hotmail.','aol.','outlook.','proton.','icloud.')
PRIO_PREFIXES = ('info@','contact@','sales@','hello@','orders@','booking@','press@','support@')

def email_quality(email_field):
    emails = [e.strip() for e in (email_field or '').split(';') if e.strip()]
    if not emails: return 'missing'
    biz = [e for e in emails if not any(fd in e for fd in FREE_DOMAINS)]
    prio = [e for e in emails if e.startswith(PRIO_PREFIXES)]
    if prio: return 'business-priority'
    if biz:  return 'business'
    return 'generic'

def prioritize_emails(emails_set):
    """Rank emails best-first:
       0) personal @business
       1) role     @business (local-part starts with info/contact/…)
       2) personal @freemail
       3) role     @freemail
       Tie-breakers: shorter localpart first, then alphabetical.
    """
    emails = list({(e or "").strip().lower() for e in (emails_set or []) if e})

    def split(e):
        if "@" in e:
            local, domain = e.split("@", 1)
        else:
            local, domain = e, ""
        return local, domain

    def is_free(domain: str) -> bool:
        return any(fd in domain for fd in FREE_DOMAINS)

    def is_role(local: str) -> bool:
        return local.startswith(PRIO_LOCAL_PREFIXES)

    def rank(e: str):
        local, domain = split(e)
        free = is_free(domain)
        role = is_role(local)
        group = (
            0 if (not free and not role) else
            1 if (not free and role)      else
            2 if (free and not role)      else
            3
        )
        return (group, len(local), e)

    return sorted(emails, key=rank)

def handle_from_url(u):
    if not u: return ''
    m = re.search(r'instagram\.com/([^/?#]+)', u.lower())
    if m: return '@' + m.group(1)
    m = re.search(r'facebook\.com/([^/?#]+)', u.lower())
    if m: return m.group(1)
    m = re.search(r'tiktok\.com/@([^/?#]+)', u.lower())
    if m: return '@' + m.group(1)
    return ''

def titleish(s):
    return ' '.join(w.capitalize() for w in (s or '').split())

def normalize_row(r):
    r['business_name'] = titleish(r.get('business_name',''))
    r['city'] = titleish(r.get('city',''))
    st = (r.get('state') or '').strip()
    r['state'] = st.upper() if st else ''
    r['country'] = r.get('country') or 'United States'
    r['address'] = (r.get('address') or '').strip()
    r['postal_code'] = (r.get('postal_code') or '').strip()   # <-- add this line

    for k,v in list(r.items()):
        if v is None: r[k] = ''
    return r

def lead_score(row):
    score = 0
    if row.get("instagram"): score += 1
    if row.get("email"): score += 2
    if row.get("tech_stack") in ("shopify", "wordpress"): score += 1
    return score

def grade(score):
    if score >= 4: return 'A'
    if score >= 2: return 'B'
    return 'C'

def norm(s):
    return re.sub(r'[^a-z0-9]+','', (s or '').lower())

def extract_domain(url):
    if not url: return ''
    m = re.search(r'https?://([^/]+)', url)
    return (m.group(1) if m else url).lower().replace('www.','')

def dedupe_rows(rows):
    """Keep best (highest lead_score) per domain, else name+city+state."""
    best = {}
    for r in rows:
        key1 = extract_domain(r.get('website','')) or f"{norm(r.get('business_name'))}_{norm(r.get('city'))}_{norm(r.get('state'))}"
        prior = best.get(key1)
        if (prior is None) or (r.get('lead_score',0) > prior.get('lead_score',0)):
            best[key1] = r
    return list(best.values())

# tiny link/tech detectors
def first_link(doc, base_url, keywords):
    for a in doc.xpath("//a[@href]"):
        href = a.get("href") or ""
        text = (a.text_content() or "").strip().lower()
        h = (href or "").lower()
        if any(k in text or k in h for k in keywords):
            return absolutize(href, base_url)
    return ""

def detect_order_reservation(html_text):
    h = (html_text or "").lower()
    platforms = []
    if "toasttab.com" in h: platforms.append("toasttab")
    if "square.site" in h or "squareup.com" in h: platforms.append("square")
    if "doordash.com" in h: platforms.append("doordash")
    if "ubereats.com" in h: platforms.append("ubereats")
    if "grubhub.com" in h: platforms.append("grubhub")
    if "clover.com" in h: platforms.append("clover")
    reservation = ""
    if "opentable.com" in h: reservation = "opentable"
    elif "resy.com" in h:    reservation = "resy"
    return ";".join(dict.fromkeys(platforms)), reservation

_ANALYTICS_GA4_RE = re.compile(r"\bG-[A-Z0-9]{6,12}\b")
_ANALYTICS_UA_RE  = re.compile(r"\bUA-\d{4,}-\d+\b")
_ANALYTICS_GTM_RE = re.compile(r"\bGTM-[A-Z0-9]{4,8}\b")
_FB_PIXEL_RE      = re.compile(r"fbq\(['\"]init['\"],\s*['\"][0-9]{5,20}['\"]\)")

def detect_analytics(html_text):
    txt = html_text or ""
    ids = set()
    ids.update(_ANALYTICS_GA4_RE.findall(txt))
    ids.update(_ANALYTICS_UA_RE.findall(txt))
    ids.update(_ANALYTICS_GTM_RE.findall(txt))
    fb_pixel = "yes" if _FB_PIXEL_RE.search(txt) else "no"
    return ";".join(sorted(ids)), fb_pixel

# -------------------------- core --------------------------

# OUTPUT schema (no obdb_id since we read from CSV)
SCHEMA = [
    "business_name","address","city","state","country","postal_code","latitude","longitude",
    "website","email","email_status","phone","instagram","instagram_handle","facebook","tiktok",
    "google_maps_url","yelp_url","hours_text","avg_rating","review_count","price_tier",
    "tech_stack","employee_count_est","contact_page_url","about_text_excerpt","recent_post_date",
    "data_source","last_seen_utc","lead_score","lead_grade","notes","order_platforms",
    "reservation_platform","analytics_ids","fb_pixel",
]
def to_sellable_row(ob):
    """Map whatever CSV columns exist into our schema (street line only for 'address')."""
    def first(*keys):
        for k in keys:
            v = ob.get(k)
            if v not in (None, ""):
                return v
        return ""

    # Street & house number from common variants
    street_raw  = first("street","street_name","address1","address_1","addr:street","address")
    housenum    = first("housenumber","house_number","addr:housenumber","street_number","no","number")

    city        = first("city","town")
    state       = first("state","region","province")
    postal      = first("postal_code","zip","zipcode","postcode")
    website_url = first("website_url","website","url","site")
    phone       = first("phone","telephone","tel")
    lat         = first("latitude","lat")
    lon         = first("longitude","lon","lng","long")
    name        = first("name","business_name")

    # If there’s a one-line address like "123 W Main St", split the house number
    if (not housenum) and street_raw:
        m = re.match(r"\s*(\d+[A-Za-z\-]*)\s+(.*)$", street_raw.strip())
        if m:
            housenum, street_raw = m.group(1), m.group(2)

    # Final street line (house number + street)
    street_line = (f"{housenum} {street_raw}".strip() if housenum else (street_raw or "")).strip()

    return {
        "business_name": (name or "").strip(),
        "address": street_line,
        "city": city or "",
        "state": state or "",
        "country": first("country") or "United States",
        "postal_code": postal or "",
        "latitude": lat or "",
        "longitude": lon or "",
        "website": website_url or "",
        "email": "",
        "email_status": "missing",
        "phone": phone or "",
        "instagram": "",
        "instagram_handle": "",
        "facebook": "",
        "tiktok": "",
        "google_maps_url": "",
        "yelp_url": "",
        "hours_text": "",
        "avg_rating": "",
        "review_count": "",
        "price_tier": "",
        "tech_stack": "",
        "employee_count_est": "",
        "contact_page_url": "",
        "about_text_excerpt": "",
        "recent_post_date": "",
        "data_source": "enrich:web",
        "last_seen_utc": "",
        "lead_score": 0,
        "lead_grade": "C",
        "notes": "",
        "order_platforms": "",
        "reservation_platform": "",
        "analytics_ids": "",
        "fb_pixel": "",
    }


def find_contact_page(doc: lxml_html.HtmlElement, base_url: str):
    candidates = []
    if doc is None:
        return ""
    for a in doc.xpath("//a[@href]"):
        href = a.get("href") or ""
        label = (a.text_content() or "").strip().lower()
        if "contact" in label or "contact" in href or "visit" in label or "find us" in label:
            candidates.append(absolutize(href, base_url))
    return candidates[0] if candidates else ""

def extract_about_excerpt(doc: lxml_html.HtmlElement, n_chars=240):
    if doc is None:
        return ""
    for p in doc.xpath("//p"):
        t = (p.text_content() or "").strip()
        if len(t) >= 120:
            return (t[:n_chars] + "…") if len(t) > n_chars else t
    return ""

def guess_stack(html_text: str) -> str:
    if not html_text:
        return ""
    h = html_text.lower()
    if "cdn.shopify.com" in h or "myshopify.com" in h:
        return "shopify"
    if "wp-content" in h or "wp-json" in h or "wordpress" in h:
        return "wordpress"
    if "wixstatic.com" in h:
        return "wix"
    if "squarespace.com" in h or "static1.squarespace.com" in h:
        return "squarespace"
    if "bigcommerce" in h:
        return "bigcommerce"
    if "weebly" in h:
        return "weebly"
    return "unknown"

def enrich_row(row, throttle=0.8):
    # --- per-row watchdog + fetch budget ---
    budget = row_budget_init()

    site = (row.get("website") or "").strip()

    # If host is currently in backoff, skip quickly
    if site and _bad_host(_host(site)):
        row["notes"] = (row.get("notes","") + "; host_backoff").strip("; ")
        row = clean_urls_in_row(row)
        row = normalize_row(row)
        row["phone"] = format_us_phones_field(row.get("phone",""))
        row["email_status"] = email_quality(row.get("email",""))
        row["instagram_handle"] = handle_from_url(row.get("instagram",""))
        row["lead_score"] = lead_score(row)
        row["lead_grade"] = grade(row.get("lead_score",0))
        return row

    html_text, final_url = (None, None)

    if site and row_time_left(budget) > 0:
        html_text, final_url = fetch_html(site, budget=budget)
        maybe_sleep(throttle, budget)  # polite

    if html_text and row_time_left(budget) > 0:
        doc = to_html_doc(html_text)
        if doc is not None:
            # Contact page via link
            contact_url = find_contact_page(doc, base_url=final_url or site)
            c_html = None
            contact_doc = None

            # If no link AND homepage looks like an age gate, try common contact URLs
            if not contact_url and looks_like_age_gate(html_text) and row_time_left(budget) > 0:
                for u, html_candidate in try_contact_variants(final_url or site, throttle, budget=budget):
                    contact_url = u
                    c_html = html_candidate
                    contact_doc = to_html_doc(c_html)
                    break

            # If still nothing, try sitemap/robots to discover a contact-like URL
            if not contact_url and row_time_left(budget) > 0:
                discovered = find_contact_via_sitemap(final_url or site, throttle, budget=budget)
                if discovered and row_time_left(budget) > 0:
                    contact_url = discovered
                    c_html, _ = fetch_html(contact_url, budget=budget)
                    if c_html:
                        maybe_sleep(throttle, budget)
                        contact_doc = to_html_doc(c_html)

            # If we DID find a contact link and haven't fetched yet, fetch it
            if contact_url and c_html is None and row_time_left(budget) > 0:
                c_html, _ = fetch_html(contact_url, budget=budget)
                if c_html:
                    maybe_sleep(throttle, budget)
                    contact_doc = to_html_doc(c_html)

            # Emails (home + contact) using richer extractor
            emails = set()
            if row_time_left(budget) > 0:
                emails.update(extract_emails_rich(doc, html_text))
            if (contact_doc is not None or c_html) and row_time_left(budget) > 0:
                emails.update(extract_emails_rich(contact_doc, c_html or ""))

            # Socials / about / stack
            socials = extract_socials(doc, base_url=final_url or site) if row_time_left(budget) > 0 else {"instagram":"","facebook":"","tiktok":""}
            about_excerpt = extract_about_excerpt(doc) if row_time_left(budget) > 0 else ""
            stack = guess_stack(html_text) if row_time_left(budget) > 0 else ""

            # Phones (priority: contact tel: > homepage tel: > CSV phone > text fallback)
            phones_accum = []
            if contact_doc is not None and row_time_left(budget) > 0:
                contact_tels = extract_tel_links(contact_doc)
                for p in contact_tels:
                    if p not in phones_accum:
                        phones_accum.append(p)
            if doc is not None and row_time_left(budget) > 0:
                home_tels = extract_tel_links(doc)
                for p in home_tels:
                    if p not in phones_accum:
                        phones_accum.append(p)
            csv_phone = (row.get("phone") or "").strip()
            csv_norm = clean_phone(csv_phone)
            if csv_norm and csv_norm not in phones_accum:
                phones_accum.append(csv_norm)
            if len(phones_accum) < 2 and row_time_left(budget) > 0:
                if contact_doc is not None and c_html:
                    for p in extract_phones(contact_doc, c_html):
                        if p not in phones_accum:
                            phones_accum.append(p)
                        if len(phones_accum) >= 2: break
                if len(phones_accum) < 2 and row_time_left(budget) > 0:
                    for p in extract_phones(doc, html_text):
                        if p not in phones_accum:
                            phones_accum.append(p)
                        if len(phones_accum) >= 2: break

            row.update({
                "email": ";".join(prioritize_emails(emails)[:3])[:255],
                "instagram": socials.get("instagram",""),
                "facebook": socials.get("facebook",""),
                "tiktok": socials.get("tiktok",""),
                "contact_page_url": contact_url or "",
                "about_text_excerpt": about_excerpt,
                "tech_stack": stack,
                "phone": ";".join(phones_accum),
                **dict(zip(("order_platforms","reservation_platform"), detect_order_reservation(html_text))),
                **dict(zip(("analytics_ids","fb_pixel"), detect_analytics(html_text))),
            })

    # Annotate if we hit watchdog or budget
    if row_time_left(budget) <= 0:
        row["notes"] = (row.get("notes","") + "; row_watchdog_timeout").strip("; ")
        # Light debug so you can see which sites are the culprits
        what = site or row.get("business_name") or "(no site)"
        print(f"⏱️  watchdog timeout: {what}", flush=True)
        _mark_host_failure(site or "")
    elif MAX_FETCHES_PER_ROW and budget["fetches"] >= MAX_FETCHES_PER_ROW:
        row["notes"] = (row.get("notes","") + "; row_fetch_budget_exhausted").strip("; ")
        _mark_host_failure(site or "")

    # Post-enrichment polish
    row = clean_urls_in_row(row)
    row["phone"] = format_us_phones_field(row.get("phone",""))  # Normalize any multi-phone field
    row["email_status"] = email_quality(row.get("email",""))
    row["instagram_handle"] = handle_from_url(row.get("instagram",""))
    row["lead_score"] = lead_score(row)
    row["lead_grade"] = grade(row.get("lead_score",0))
    row = normalize_row(row)
    return row

# ---------- optional: run each row in a subprocess to enforce hard wall clock ----------

def _row_enrich_worker(row: dict, throttle: float):
    # child process work: isolate any blocking I/O
    return enrich_row(dict(row), throttle=throttle)

class RowRunner:
    def __init__(self, timeout_s: float):
        self.timeout_s = timeout_s
        self._new_pool()

    def _new_pool(self):
        ctx = multiprocessing.get_context("spawn")
        self.pool = ProcessPoolExecutor(max_workers=1, mp_context=ctx)

    def run(self, row: dict, throttle: float) -> dict:
        fut = self.pool.submit(_row_enrich_worker, dict(row), throttle)
        try:
            return fut.result(timeout=self.timeout_s)
        except ProcTimeout:
            _mark_host_failure(row.get("website",""))
            row["notes"] = (row.get("notes","") + "; row_watchdog_timeout; killed_subprocess").strip("; ")
            print(f"⏱️ watchdog hard-kill: {row.get('website') or row.get('business_name')}", flush=True)
            try:
                self.pool.shutdown(cancel_futures=True)
            finally:
                self._new_pool()
            row = clean_urls_in_row(row)
            row = normalize_row(row)
            row["phone"] = format_us_phones_field(row.get("phone",""))
            row["email_status"] = email_quality(row.get("email",""))
            row["instagram_handle"] = handle_from_url(row.get("instagram",""))
            row["lead_score"] = lead_score(row)
            row["lead_grade"] = grade(row.get("lead_score",0))
            return row

    def shutdown(self):
        try:
            self.pool.shutdown(cancel_futures=True)
        except Exception:
            pass

# -------------------------- CSV ingest --------------------------
def count_rows_in_csv(path: str) -> int:
    """Count data rows (excludes header). Returns 0 if file missing/empty."""
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return 0
    try:
        with open(path, newline="", encoding="utf-8") as f:
            return sum(1 for _ in csv.DictReader(f))
    except Exception:
        return 0

def fetch_from_csv(path, limit=0, skip=0):
    """Yield rows from CSV, skipping the first `skip` data rows."""
    total = 0
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for i, row in enumerate(r):
            if i < skip:
                continue
            yield row
            total += 1
            if limit and total >= limit:
                return

# -------------------------- main --------------------------

def file_exists_nonempty(path: str) -> bool:
    return os.path.exists(path) and os.path.getsize(path) > 0

def write_header_if_needed(out_path: str, fieldnames: list):
    if not file_exists_nonempty(out_path):
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=fieldnames).writeheader()

def load_seen_ids(out_path: str) -> set:
    # Kept for compatibility; will try to resume if an 'obdb_id' exists in your output (often not).
    seen = set()
    if not file_exists_nonempty(out_path):
        return seen
    try:
        with open(out_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                oid = (row.get("obdb_id") or "").strip()
                if oid: seen.add(oid)
    except Exception:
        pass
    return seen

class GracefulStop:
    stopping = False

def _handle(sig, frame):
    # hard interrupt so we break out of any blocking I/O immediately
    print("\n🛑 Stopping now...", flush=True)
    raise KeyboardInterrupt

for _sig in (signal.SIGINT, signal.SIGTERM):
    try:
        signal.signal(_sig, _handle)
    except Exception:
        pass

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="Input CSV path (e.g., all_breweries_usa.csv)")
    ap.add_argument("--out", required=True, help="Output CSV path (e.g., enriched_breweries.csv)")
    ap.add_argument("--limit", type=int, default=0, help="Limit # of rows for testing")
    ap.add_argument("--throttle", type=float, default=0.8, help="Seconds between website fetches")
    ap.add_argument("--no-subprocess", action="store_true", help="Disable per-row subprocess timeout")
    ap.add_argument("--resume", type=int, default=1,
                help="1=auto-skip rows already in --out (default), 0=off")
    ap.add_argument("--resume-from", type=int, default=-1,
                help="Start at this 0-based input row index (overrides --resume).")

    args = ap.parse_args()

    base_fieldnames = list(SCHEMA)
    write_header_if_needed(args.out, base_fieldnames)
    # Determine resume point
    skip_n = 0
    if args.resume_from >= 0:
        skip_n = max(0, args.resume_from)
    elif args.resume:
        skip_n = count_rows_in_csv(args.out)

    print(f"↩️  Resume config: skipping first {skip_n} input rows; appending to {args.out}", flush=True)

    seen = load_seen_ids(args.out)  # may be empty; fine
    print(f"↩️  Resuming: {len(seen)} rows already in {args.out}", flush=True)

    start = time.time()
    processed = written = 0
    current_fieldnames = base_fieldnames[:]  # may grow if enrichment adds keys

    # Live stats for the dashboard
    enrich_stats = {
        "started_at": time.time(),
        "written": 0,
        "processed": 0,
        "timeouts": 0,
        "fetch_budget_exhausted": 0,
        "errors": 0,
        "rate_rows_per_sec": 0.0,
        "elapsed_sec": 0.0,
        "last_site": "",
        "last_status": "",
        "last_note": "",
        "last_updated": time.time(),
    }
    _write_enrich_stats(ENRICH_STATS_PATH, enrich_stats)

    runner = None
    if not args.no_subprocess:
        runner = RowRunner(timeout_s=ROW_WATCHDOG_SEC + 5)

    try:
        with open(args.out, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=current_fieldnames)

            for ob in fetch_from_csv(args.inp, limit=args.limit, skip=skip_n):
                # tolerate either "id" or "obdb_id" in your CSV for resume (if present)
                ob_id = str(ob.get("id") or ob.get("obdb_id") or "")
                if ob_id and ob_id in seen:
                    processed += 1
                    continue

                try:
                    row = to_sellable_row(ob)
                    if runner is None:
                        row = enrich_row(row, throttle=args.throttle)
                    else:
                        row = runner.run(row, throttle=args.throttle)
                except Exception as ex:
                    row = to_sellable_row(ob)
                    row["notes"] = (row.get("notes","") + f"; enrich_error: {ex}").strip("; ")

                # if enrichment introduced new keys, extend the writer’s fieldnames
                extra = sorted(set(row.keys()) - set(current_fieldnames))
                if extra:
                    current_fieldnames += extra
                    writer = csv.DictWriter(f, fieldnames=current_fieldnames)

                safe_row = {k: row.get(k, "") for k in current_fieldnames}
                writer.writerow(safe_row)
                f.flush()

                written += 1
                processed += 1
                # --- Update live stats for dashboard ---
                note = (row.get("notes") or "")
                status = "ok"
                if "enrich_error" in note:
                    enrich_stats["errors"] += 1
                    status = "error"
                elif "row_watchdog_timeout" in note:
                    enrich_stats["timeouts"] += 1
                    status = "watchdog_timeout"
                elif "row_fetch_budget_exhausted" in note:
                    enrich_stats["fetch_budget_exhausted"] += 1
                    status = "fetch_budget_exhausted"

                elapsed_mid = time.time() - start
                enrich_stats.update({
                    "written": written,
                    "processed": processed,
                    "elapsed_sec": elapsed_mid,
                    "rate_rows_per_sec": (written / max(1.0, elapsed_mid)),
                    "last_site": (row.get("website") or row.get("business_name") or ""),
                    "last_status": status,
                    "last_note": note,
                    "last_updated": time.time(),
                })
                _write_enrich_stats(ENRICH_STATS_PATH, enrich_stats)

                if ob_id:
                    seen.add(ob_id)

                if written % 25 == 0:
                    elapsed_mid = time.time() - start
                    rate = written / max(0.001, elapsed_mid)
                    print(f"✅ wrote {written} | {rate:.2f} rows/sec | elapsed {elapsed_mid/60:.1f} min", flush=True)

                if args.limit and written >= args.limit:
                    break

    except KeyboardInterrupt:
        print("\n🟡 Interrupted by user (Ctrl-C). Flushing and exiting.", flush=True)
    finally:
        if runner is not None:
            try:
                runner.shutdown()
            except Exception:
                pass

    elapsed = time.time() - start
    print("🎉 Done" if not GracefulStop.stopping else "🟡 Stopped gracefully")
    print(f"processed={processed} written={written} elapsed={elapsed/60:.1f} min")

if __name__ == "__main__":
    main()
