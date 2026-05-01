#!/usr/bin/env python3
"""
Biblical Jurisprudence — Court Seeder v2

Fetches courts from CourtListener and upserts into the courts table.
Covers all 50 US states + SCOTUS + all 14 federal circuit courts.

Usage:
    python seed_courts_v2.py              # seed all target courts
    python seed_courts_v2.py --dry-run   # preview without writing
    python seed_courts_v2.py --list      # list courts currently in DB
"""

import argparse
import os
import re
import sys
import time

import httpx
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

COURTLISTENER_API = "https://www.courtlistener.com/api/rest/v4"
COURTLISTENER_API_KEY = os.getenv("COURTLISTENER_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

# ---------------------------------------------------------------------------
# All 50 states — longer names first to avoid substring collisions
# ---------------------------------------------------------------------------

ALL_STATES = [
    "West Virginia",
    "North Carolina",
    "South Carolina",
    "North Dakota",
    "South Dakota",
    "New Hampshire",
    "New Mexico",
    "New Jersey",
    "New York",
    "Rhode Island",
    "Connecticut",
    "Massachusetts",
    "Pennsylvania",
    "Mississippi",
    "Minnesota",
    "Michigan",
    "Missouri",
    "Montana",
    "Maryland",
    "Louisiana",
    "Kentucky",
    "Kansas",
    "Indiana",
    "Illinois",
    "Georgia",
    "Florida",
    "Delaware",
    "Colorado",
    "California",
    "Arkansas",
    "Arizona",
    "Alabama",
    "Alaska",
    "Hawaii",
    "Idaho",
    "Iowa",
    "Maine",
    "Nebraska",
    "Nevada",
    "Ohio",
    "Oklahoma",
    "Oregon",
    "Tennessee",
    "Texas",
    "Utah",
    "Vermont",
    "Virginia",
    "Washington",
    "Wisconsin",
    "Wyoming",
]

# Federal circuit court IDs in CourtListener (added explicitly, not via name match)
FEDERAL_CIRCUIT_IDS = {
    "ca1":   ("United States Court of Appeals for the First Circuit",   "First Circuit"),
    "ca2":   ("United States Court of Appeals for the Second Circuit",  "Second Circuit"),
    "ca3":   ("United States Court of Appeals for the Third Circuit",   "Third Circuit"),
    "ca4":   ("United States Court of Appeals for the Fourth Circuit",  "Fourth Circuit"),
    "ca5":   ("United States Court of Appeals for the Fifth Circuit",   "Fifth Circuit"),
    "ca6":   ("United States Court of Appeals for the Sixth Circuit",   "Sixth Circuit"),
    "ca7":   ("United States Court of Appeals for the Seventh Circuit", "Seventh Circuit"),
    "ca8":   ("United States Court of Appeals for the Eighth Circuit",  "Eighth Circuit"),
    "ca9":   ("United States Court of Appeals for the Ninth Circuit",   "Ninth Circuit"),
    "ca10":  ("United States Court of Appeals for the Tenth Circuit",   "Tenth Circuit"),
    "ca11":  ("United States Court of Appeals for the Eleventh Circuit","Eleventh Circuit"),
    "cadc":  ("United States Court of Appeals for the D.C. Circuit",   "D.C. Circuit"),
    "cafc":  ("United States Court of Appeals for the Federal Circuit", "Federal Circuit"),
    "scotus":("Supreme Court of the United States",                     "SCOTUS"),
}

# Courts to skip even if they match
SKIP_COURT_IDS = {
    "kyctapphigh",      # Kentucky Court of Appeals pre-1976
    "tennworkcompapp",  # Tennessee Workers' Compensation Appeals Board
}


def get_level(name: str) -> str | None:
    """Return 'supreme' or 'appellate' based on court name, or None to skip."""
    n = name.lower()
    if "supreme" in n:
        return "supreme"
    if "appeal" in n or "appellate" in n:
        return "appellate"
    return None


def match_state(court_name: str) -> str | None:
    """Return matched state name, or None. Checks longer names first."""
    for state in ALL_STATES:
        if re.search(rf"\b{re.escape(state)}\b", court_name):
            return state
    return None


def fetch_all_courts(client: httpx.Client) -> list[dict]:
    """Fetch all in-use courts from CourtListener (paginated)."""
    courts = []
    url = f"{COURTLISTENER_API}/courts/?in_use=true&format=json"
    while url:
        resp = client.get(url)
        resp.raise_for_status()
        data = resp.json()
        courts.extend(data["results"])
        url = data.get("next")
        time.sleep(0.5)
    return courts


def filter_state_courts(courts: list[dict]) -> list[dict]:
    """Filter to state supreme/appellate courts across all 50 states."""
    results = []
    for c in courts:
        if c["id"] in SKIP_COURT_IDS:
            continue
        if c["id"] in FEDERAL_CIRCUIT_IDS:
            continue  # handled separately
        name = c.get("full_name", "")
        state = match_state(name)
        if not state:
            continue
        level = get_level(name)
        if not level:
            continue
        results.append({
            "id": c["id"],
            "name": name,
            "short_name": c.get("short_name", ""),
            "jurisdiction": "state",
            "level": level,
            "state": state,
        })
    return sorted(results, key=lambda x: (x["state"], x["level"], x["id"]))


def build_federal_courts() -> list[dict]:
    """Return hardcoded federal circuit + SCOTUS records."""
    results = []
    for court_id, (full_name, short_name) in FEDERAL_CIRCUIT_IDS.items():
        level = "supreme" if court_id == "scotus" else "appellate"
        results.append({
            "id": court_id,
            "name": full_name,
            "short_name": short_name,
            "jurisdiction": "federal",
            "level": level,
            "state": None,
        })
    return sorted(results, key=lambda x: x["id"])


def main():
    parser = argparse.ArgumentParser(description="Seed courts table (v2 — all 50 states + federal)")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    parser.add_argument("--list", action="store_true", help="List courts currently in DB")
    args = parser.parse_args()

    if not SUPABASE_KEY:
        sys.exit("Error: set SUPABASE_SERVICE_KEY in .env")

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    if args.list:
        rows = sb.table("courts").select("*").order("state").execute().data
        for r in rows:
            state = r.get("state") or "federal"
            print(f"  {r['id']:25} [{r['level']:10}] {state:20} {r['name']}")
        print(f"\nTotal: {len(rows)} courts")
        return

    client = httpx.Client(
        headers={"Authorization": f"Token {COURTLISTENER_API_KEY}"},
        timeout=30,
    )

    print("Fetching courts from CourtListener...", flush=True)
    all_courts = fetch_all_courts(client)
    print(f"Fetched {len(all_courts)} courts total.", flush=True)
    client.close()

    state_courts = filter_state_courts(all_courts)
    federal_courts = build_federal_courts()
    target = state_courts + federal_courts

    print(f"\nFiltered to {len(state_courts)} state courts + {len(federal_courts)} federal courts = {len(target)} total:\n")
    for c in sorted(target, key=lambda x: (x.get("state") or "zzz_federal", x["id"])):
        jurisdiction = c.get("state") or "federal"
        print(f"  {c['id']:25} [{c['level']:10}] {jurisdiction:20} {c['name']}")

    if args.dry_run:
        print("\n[dry run] no changes written.")
        return

    print(f"\nUpserting {len(target)} courts...", flush=True)
    for c in target:
        sb.table("courts").upsert(c, on_conflict="id").execute()
    print("Done.", flush=True)

    # Write court IDs file for run_v2.sh
    ids_file = os.path.join(os.path.dirname(__file__), ".court_ids_v2.txt")
    court_ids = [c["id"] for c in target]
    with open(ids_file, "w") as f:
        f.write("\n".join(court_ids))
    print(f"\nCourt IDs written to {ids_file}")
    print(f"Run: bash run_v2.sh")


if __name__ == "__main__":
    main()
