# OSM → Leads: Seed + Website Enrichment

Tile-based OpenStreetMap seeding → contact discovery → clean CSVs you can sell or feed into CRMs.

## What it does
- **Seed** US business lists from OSM (tile batching, resume, throttling)
- **Backfill** address parts / admin areas
- **Enrich** websites for emails, phones, socials, and contact pages (best-effort; polite)
- **Stats + dashboard** JSON for quick progress visuals

## Quickstart
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # optional; user agents, timeouts, etc.
pre-commit install    # if you want auto-format on commit
