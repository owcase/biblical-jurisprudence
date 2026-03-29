#!/usr/bin/env python3
"""
Biblical Jurisprudence — CourtListener Ingestion Pipeline

Searches CourtListener for opinions matching active search terms,
fetches full opinion text, and writes results to Supabase.

Usage:
    python ingest.py --court ala --term "Bible"
    python ingest.py --court ala  # runs all active search terms
    python ingest.py --court all  # all courts (slow — use carefully)
    python ingest.py --court ala --after 1980-01-01 --before 2000-12-31
    python ingest.py --list-courts
    python ingest.py --list-terms
"""

import argparse
import os
import re
import sys
import time
from datetime import datetime, date

import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

COURTLISTENER_API = "https://www.courtlistener.com/api/rest/v4"
COURTLISTENER_API_KEY = os.getenv("COURTLISTENER_API_KEY", "9f6c698c7c50d3c5a0ec781e03480d5e50d522d4")
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://pwrtjhvhbofoteiflhna.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")  # service role key for writes

# Rate limiting: CourtListener allows ~10 req/60s for authenticated users.
# We use 8/60s to stay safely under the limit.
RATE_LIMIT_REQUESTS = 8
RATE_LIMIT_PERIOD = 60
REQUEST_INTERVAL = RATE_LIMIT_PERIOD / RATE_LIMIT_REQUESTS  # ~7.5s between requests

# Excerpt context: characters to include around the matched term
EXCERPT_WINDOW = 1000  # characters either side of match


# ---------------------------------------------------------------------------
# Rate-limited HTTP client
# ---------------------------------------------------------------------------

class RateLimitedClient:
    """Wraps httpx with per-request rate limiting and exponential backoff."""

    def __init__(self, api_key: str):
        self.client = httpx.Client(
            headers={"Authorization": f"Token {api_key}"},
            timeout=30,
        )
        self._last_request = 0.0

    def _wait(self):
        elapsed = time.time() - self._last_request
        if elapsed < REQUEST_INTERVAL:
            time.sleep(REQUEST_INTERVAL - elapsed)

    def get(self, url: str, **kwargs) -> httpx.Response:
        for attempt in range(5):
            self._wait()
            self._last_request = time.time()
            try:
                resp = self.client.get(url, **kwargs)
                if resp.status_code == 429:
                    wait = int(resp.headers.get("Retry-After", 60))
                    print(f"  [rate limit] waiting {wait}s...", flush=True)
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp
            except httpx.HTTPStatusError as e:
                if attempt == 4:
                    raise
                wait = 2 ** attempt
                print(f"  [HTTP {e.response.status_code}] retry {attempt + 1}/5 in {wait}s", flush=True)
                time.sleep(wait)
        raise RuntimeError("Max retries exceeded")

    def close(self):
        self.client.close()


# ---------------------------------------------------------------------------
# Text helpers
# ---------------------------------------------------------------------------

def html_to_text(html: str) -> str:
    """Strip HTML tags and normalise whitespace."""
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator=" ")
    # Collapse runs of whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_excerpt(full_text: str, term: str, window: int = EXCERPT_WINDOW) -> list[dict]:
    """
    Find all occurrences of `term` (case-insensitive) in `full_text`.
    Returns a list of dicts with 'excerpt' and 'excerpt_context' for each hit.
    """
    results = []
    pattern = re.compile(re.escape(term), re.IGNORECASE)
    for match in pattern.finditer(full_text):
        start = match.start()
        end = match.end()
        # Tight excerpt: sentence containing the match
        sent_start = full_text.rfind(".", 0, start)
        sent_start = sent_start + 1 if sent_start != -1 else max(0, start - 200)
        sent_end = full_text.find(".", end)
        sent_end = sent_end + 1 if sent_end != -1 else min(len(full_text), end + 200)
        excerpt = full_text[sent_start:sent_end].strip()

        # Wider context window
        ctx_start = max(0, start - window)
        ctx_end = min(len(full_text), end + window)
        context = full_text[ctx_start:ctx_end].strip()

        results.append({"excerpt": excerpt, "excerpt_context": context})
    return results


# ---------------------------------------------------------------------------
# CourtListener fetchers
# ---------------------------------------------------------------------------

def search_opinions(
    client: RateLimitedClient,
    term: str,
    court: str,
    after: str,
    before: str,
    page_size: int = 100,
) -> list[dict]:
    """Fetch all search results for a term/court combination (paginated)."""
    params = {
        "q": term,
        "type": "o",
        "order_by": "dateFiled asc",
        "filed_after": after,
    }
    if before:
        params["filed_before"] = before
    if court and court != "all":
        params["court"] = court

    results = []
    url = f"{COURTLISTENER_API}/search/"
    page = 1

    while url:
        print(f"  [search] page {page} — {len(results)} results so far", flush=True)
        resp = client.get(url, params=params if page == 1 else None)
        data = resp.json()
        batch = data.get("results", [])
        results.extend(batch)
        url = data.get("next")
        params = None  # next URL already has params embedded
        page += 1

    return results


def fetch_opinion_full_text(client: RateLimitedClient, opinion_id: int) -> tuple[str, str]:
    """
    Fetch a single opinion by ID and return (plain_text, html_source).
    Tries plain_text first, falls back to html_lawbox, then html_with_citations.
    Returns (plain_text, raw_html).
    """
    url = f"{COURTLISTENER_API}/opinions/{opinion_id}/"
    resp = client.get(url)
    data = resp.json()

    plain = data.get("plain_text", "").strip()
    if plain:
        return plain, ""

    for field in ("html_lawbox", "html_with_citations", "html", "html_columbia", "html_anon_2020"):
        html = data.get(field, "").strip()
        if html:
            return html_to_text(html), html

    return "", ""


# ---------------------------------------------------------------------------
# Supabase helpers
# ---------------------------------------------------------------------------

def get_or_create_opinion(sb: Client, cluster_id: int, result: dict, court_name: str) -> int | None:
    """
    Insert opinion metadata if not already present. Returns the local opinion ID.
    """
    # Check for existing record
    existing = (
        sb.table("opinions")
        .select("id")
        .eq("courtlistener_cluster_id", cluster_id)
        .execute()
    )
    if existing.data:
        return existing.data[0]["id"]

    # Extract primary opinion ID from result
    opinions_list = result.get("opinions", [])
    primary_opinion_id = opinions_list[0]["id"] if opinions_list else None

    # Build citation string array
    citation = result.get("citation", [])
    if isinstance(citation, str):
        citation = [citation]

    # court_id from search result (e.g. 'ala'); court full name is 'court' field
    result_court_id = result.get("court_id") or None
    result_court_name = result.get("court") or court_name

    row = {
        "courtlistener_cluster_id": cluster_id,
        "courtlistener_opinion_id": primary_opinion_id,
        "case_name": result.get("caseName") or result.get("case_name", ""),
        "case_name_full": result.get("caseNameFull") or result.get("case_name_full") or None,
        "citation": citation or None,
        "docket_number": result.get("docketNumber") or result.get("docket_number") or None,
        "court_id": result_court_id,
        "court_name": result_court_name,
        "date_filed": result.get("dateFiled") or result.get("date_filed") or None,
        "judge": result.get("judge") or None,
        "status": result.get("status") or None,
        "source_url": f"https://www.courtlistener.com{result.get('absolute_url', '')}",
    }

    inserted = sb.table("opinions").insert(row).execute()
    if inserted.data:
        return inserted.data[0]["id"]
    return None


def opinion_match_exists(sb: Client, opinion_id: int, term_id: int, excerpt: str) -> bool:
    """Check whether this exact match is already recorded."""
    existing = (
        sb.table("opinion_matches")
        .select("id")
        .eq("opinion_id", opinion_id)
        .eq("search_term_id", term_id)
        .eq("excerpt", excerpt)
        .execute()
    )
    return bool(existing.data)


def save_match(
    sb: Client,
    opinion_id: int,
    term_id: int,
    section: str,
    excerpt: str,
    context: str,
):
    if opinion_match_exists(sb, opinion_id, term_id, excerpt):
        return
    sb.table("opinion_matches").insert({
        "opinion_id": opinion_id,
        "search_term_id": term_id,
        "opinion_section": section,
        "excerpt": excerpt,
        "excerpt_context": context,
    }).execute()


def update_full_text(sb: Client, opinion_id: int, full_text: str):
    sb.table("opinions").update({
        "full_text": full_text,
        "full_text_retrieved_at": datetime.utcnow().isoformat(),
    }).eq("id", opinion_id).execute()


def log_run(sb: Client, term_id: int | None, court: str, after: str, before: str) -> int:
    row = {
        "search_term_id": term_id,
        "court_filter": court,
        "date_filed_after": after or None,
        "date_filed_before": before or None,
        "status": "running",
    }
    result = sb.table("ingestion_runs").insert(row).execute()
    return result.data[0]["id"]


def finish_run(sb: Client, run_id: int, total: int, ingested: int, status: str = "completed", error: str = None):
    sb.table("ingestion_runs").update({
        "total_results": total,
        "opinions_ingested": ingested,
        "status": status,
        "error_message": error,
        "completed_at": datetime.utcnow().isoformat(),
    }).eq("id", run_id).execute()


# ---------------------------------------------------------------------------
# Core ingestion logic
# ---------------------------------------------------------------------------

def ingest(
    term_row: dict,
    court: str,
    after: str,
    before: str,
    http: RateLimitedClient,
    sb: Client,
    dry_run: bool = False,
):
    term = term_row["term"]
    term_id = term_row["id"]
    print(f"\n{'='*60}", flush=True)
    print(f"Term: '{term}' | Court: {court} | After: {after} | Before: {before or 'now'}", flush=True)
    print(f"{'='*60}", flush=True)

    run_id = None if dry_run else log_run(sb, term_id, court, after, before)

    # 1. Search
    print("Searching CourtListener...", flush=True)
    results = search_opinions(http, term, court, after, before)
    print(f"Found {len(results)} results.", flush=True)

    if dry_run:
        print("[dry run] skipping write", flush=True)
        return

    ingested_count = 0

    try:
        for i, result in enumerate(results):
            cluster_id = result.get("cluster_id")
            court_name = result.get("court", "")
            opinions_list = result.get("opinions", [])

            if not cluster_id:
                continue

            print(f"\n[{i+1}/{len(results)}] cluster {cluster_id} — {result.get('caseName', '')}", flush=True)

            # 2. Store opinion metadata
            opinion_id = get_or_create_opinion(sb, cluster_id, result, court_name)
            if not opinion_id:
                print("  [skip] failed to insert opinion", flush=True)
                continue

            # 3. Fetch full text for each opinion document in this cluster
            all_text_parts = []
            for op in opinions_list:
                op_id = op.get("id")
                op_type = op.get("type", "unknown")
                if not op_id:
                    continue
                print(f"  fetching opinion {op_id} ({op_type})...", flush=True)
                try:
                    plain_text, _ = fetch_opinion_full_text(http, op_id)
                    if plain_text:
                        all_text_parts.append((op_type, plain_text))
                except Exception as e:
                    print(f"  [warn] failed to fetch opinion {op_id}: {e}", flush=True)

            if not all_text_parts:
                # Fall back to search snippet
                snippet = opinions_list[0].get("snippet", "") if opinions_list else ""
                if snippet:
                    plain_text = html_to_text(snippet)
                    all_text_parts = [("snippet", plain_text)]

            # 4. Store full text (concatenated across all opinion sections)
            combined_text = "\n\n".join(text for _, text in all_text_parts)
            if combined_text:
                update_full_text(sb, opinion_id, combined_text)

            # 5. Extract and store matches
            match_count = 0
            for section_type, section_text in all_text_parts:
                hits = extract_excerpt(section_text, term)
                for hit in hits:
                    save_match(
                        sb,
                        opinion_id,
                        term_id,
                        section_type,
                        hit["excerpt"],
                        hit["excerpt_context"],
                    )
                    match_count += 1

            print(f"  {match_count} match(es) stored", flush=True)
            ingested_count += 1

    except Exception as e:
        print(f"\n[ERROR] {e}", flush=True)
        if run_id:
            finish_run(sb, run_id, len(results), ingested_count, "failed", str(e))
        raise

    if run_id:
        finish_run(sb, run_id, len(results), ingested_count)
    print(f"\nDone. {ingested_count}/{len(results)} opinions processed.", flush=True)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_supabase_client() -> Client:
    key = SUPABASE_KEY
    if not key:
        # Try anon key as fallback (read-only on RLS-protected tables, but fine for public schema)
        print("[warn] SUPABASE_SERVICE_KEY not set — trying anon key", flush=True)
        key = os.getenv("SUPABASE_ANON_KEY")
    if not key:
        sys.exit("Error: set SUPABASE_SERVICE_KEY (or SUPABASE_ANON_KEY) in .env")
    return create_client(SUPABASE_URL, key)


def main():
    parser = argparse.ArgumentParser(description="Biblical Jurisprudence ingestion pipeline")
    parser.add_argument("--court", default="ala", help="CourtListener court ID, or 'all'")
    parser.add_argument("--term", help="Search term (overrides active terms in DB)")
    parser.add_argument("--after", default="1980-01-01", help="Filed after date (YYYY-MM-DD)")
    parser.add_argument("--before", default="", help="Filed before date (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Search only, no writes")
    parser.add_argument("--list-courts", action="store_true", help="Print courts table and exit")
    parser.add_argument("--list-terms", action="store_true", help="Print active search terms and exit")
    args = parser.parse_args()

    http = RateLimitedClient(COURTLISTENER_API_KEY)
    sb = build_supabase_client()

    if args.list_courts:
        rows = sb.table("courts").select("*").order("state").execute().data
        if not rows:
            print("No courts in database yet.")
        for r in rows:
            print(f"  {r['id']:20} {r['name']}")
        return

    if args.list_terms:
        rows = sb.table("search_terms").select("*").order("category").execute().data
        for r in rows:
            status = "active" if r["active"] else "inactive"
            print(f"  [{status:8}] {r['category']:15} {r['term']}")
        return

    # Resolve search terms
    if args.term:
        term_rows = (
            sb.table("search_terms")
            .select("id, term")
            .eq("term", args.term)
            .execute()
            .data
        )
        if not term_rows:
            sys.exit(f"Error: term '{args.term}' not found in search_terms table")
    else:
        term_rows = (
            sb.table("search_terms")
            .select("id, term")
            .eq("active", True)
            .execute()
            .data
        )

    if not term_rows:
        sys.exit("No active search terms found.")

    print(f"Running {len(term_rows)} term(s) against court '{args.court}'")

    for term_row in term_rows:
        try:
            ingest(
                term_row=term_row,
                court=args.court,
                after=args.after,
                before=args.before,
                http=http,
                sb=sb,
                dry_run=args.dry_run,
            )
        except Exception as e:
            print(f"[SKIP] term '{term_row['term']}' failed: {e}", flush=True)
            continue

    http.close()
    print("\nAll done.", flush=True)


if __name__ == "__main__":
    main()
