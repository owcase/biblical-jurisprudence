#!/usr/bin/env python3
"""
Biblical Jurisprudence - Court Seeder

Fetches courts from CourtListener and upserts them into the courts table.
Filters to target states, supreme and appellate level only.

Usage:
    python seed_courts.py              # seed target states
    python seed_courts.py --dry-run   # preview without writing
    python seed_courts.py --list      # list all courts in DB
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
# Target states — Alabama already seeded; listed here for completeness
# Order matters: longer names must come before shorter substrings
# (e.g. "West Virginia" before "Virginia")
# ---------------------------------------------------------------------------

TARGET_STATES = [
    "West Virginia",
    "Arkansas",
    "Florida",
    "Georgia",
    "Kentucky",
    "Louisiana",
    "Mississippi",
    "North Carolina",
    "South Carolina",
    "Tennessee",
    "Texas",
    "Virginia",
    "Utah",
    # Alabama already seeded but included so the file is the complete record
    "Alabama",
]

# Courts to skip even if they match (pre-date range, specialised, or irrelevant)
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
    """
    Return the matched target state name or None.
    Checks longer names first to avoid 'Virginia' matching 'West Virginia'.
    """
    for state in TARGET_STATES:
        # Whole-word match to avoid e.g. "Georgia" matching "New Georgia"
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
        time.sleep(0.5)  # gentle rate limiting
    return courts


def filter_courts(courts: list[dict]) -> list[dict]:
    """Filter to target state supreme/appellate courts."""
    results = []
    for c in courts:
        if c["id"] in SKIP_COURT_IDS:
            continue
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


def main():
    parser = argparse.ArgumentParser(description="Seed courts table from CourtListener")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    parser.add_argument("--list", action="store_true", help="List courts currently in DB")
    args = parser.parse_args()

    if not SUPABASE_KEY:
        sys.exit("Error: set SUPABASE_SERVICE_KEY in .env")

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    if args.list:
        rows = sb.table("courts").select("*").order("state").execute().data
        for r in rows:
            print(f"  {r['id']:25} [{r['level']:10}] {r['state']:15} {r['name']}")
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

    target = filter_courts(all_courts)
    print(f"\nFiltered to {len(target)} target courts:\n")

    for c in target:
        print(f"  {c['id']:25} [{c['level']:10}] {c['state']:15} {c['name']}")

    if args.dry_run:
        print("\n[dry run] no changes written.")
        return

    print(f"\nUpserting {len(target)} courts...", flush=True)
    for c in target:
        sb.table("courts").upsert(c, on_conflict="id").execute()

    print("Done.")

    # Output court IDs for use by run_states.sh
    court_ids = [c["id"] for c in target]
    ids_file = os.path.join(os.path.dirname(__file__), ".court_ids.txt")
    with open(ids_file, "w") as f:
        f.write("\n".join(court_ids))
    print(f"\nCourt IDs written to {ids_file} for use by run_states.sh")


if __name__ == "__main__":
    main()
