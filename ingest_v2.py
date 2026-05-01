#!/usr/bin/env python3
"""
Biblical Jurisprudence — Ingestion Pipeline v2

Key differences from ingest.py:
  - Writes to opinions_v2 / opinion_matches_v2 / ingestion_runs_v2
  - No regex pre-filter (classify.py not used); all matches go to LLM review
  - Common-name Bible books (John, Matthew, etc.) are searched chapter-by-chapter
    ("John 1:", "John 2:", ...) to avoid millions of false hits
  - excerpt_pattern from search_terms used for excerpt detection on chapter-query terms
  - Records search_query on every match row

Usage:
    python ingest_v2.py --court scotus --after 1975-01-01
    python ingest_v2.py --court ca9 --after 1975-01-01
    python ingest_v2.py --court ala --after 1975-01-01
    python ingest_v2.py --list-courts
    python ingest_v2.py --list-terms
    python ingest_v2.py --dry-run --court scotus --term Bible
"""

import argparse
import os
import re
import sys
import time
from datetime import datetime

import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

COURTLISTENER_API        = "https://www.courtlistener.com/api/rest/v4"
COURTLISTENER_API_KEY    = os.getenv("COURTLISTENER_API_KEY")
SUPABASE_URL             = os.getenv("SUPABASE_URL")
SUPABASE_KEY             = os.getenv("SUPABASE_SERVICE_KEY")

RATE_LIMIT_REQUESTS = 8
RATE_LIMIT_PERIOD   = 60
REQUEST_INTERVAL    = RATE_LIMIT_PERIOD / RATE_LIMIT_REQUESTS  # 7.5s

EXCERPT_WINDOW = 1000  # characters either side of match


# ---------------------------------------------------------------------------
# Rate-limited HTTP client (identical to ingest.py)
# ---------------------------------------------------------------------------

class RateLimitedClient:
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
                print(f"  [HTTP {e.response.status_code}] retry {attempt+1}/5 in {wait}s", flush=True)
                time.sleep(wait)
        raise RuntimeError("Max retries exceeded")

    def close(self):
        self.client.close()


# ---------------------------------------------------------------------------
# Text helpers
# ---------------------------------------------------------------------------

def html_to_text(html: str) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator=" ")
    return re.sub(r"\s+", " ", text).strip()


def extract_excerpt(full_text: str, search_pattern: re.Pattern, window: int = EXCERPT_WINDOW) -> list[dict]:
    """
    Find all regex matches of search_pattern in full_text.
    Returns list of {excerpt, excerpt_context}.
    """
    results = []
    for match in search_pattern.finditer(full_text):
        start = match.start()
        end   = match.end()

        sent_start = full_text.rfind(".", 0, start)
        sent_start = sent_start + 1 if sent_start != -1 else max(0, start - 200)
        sent_end   = full_text.find(".", end)
        sent_end   = sent_end + 1 if sent_end != -1 else min(len(full_text), end + 200)
        excerpt = full_text[sent_start:sent_end].strip()

        ctx_start = max(0, start - window)
        ctx_end   = min(len(full_text), end + window)
        context   = full_text[ctx_start:ctx_end].strip()

        results.append({"excerpt": excerpt, "excerpt_context": context})
    return results


def build_search_pattern(term_row: dict) -> re.Pattern:
    """
    For chapter-query terms: use excerpt_pattern from DB (matches BookName Ch:Verse).
    For regular terms: match the bare term word-boundary.
    """
    if term_row.get("use_chapter_queries") and term_row.get("excerpt_pattern"):
        # excerpt_pattern stored as Postgres \m word-boundary; convert to Python \b
        pat = term_row["excerpt_pattern"].replace(r"\m", r"\b")
        return re.compile(pat, re.IGNORECASE)
    return re.compile(r"\b" + re.escape(term_row["term"]) + r"\b", re.IGNORECASE)


def chapter_queries(term_row: dict) -> list[str]:
    """
    For a chapter-query term, yield '"BookName 1:"', '"BookName 2:"', ...
    Quoted so CourtListener treats it as a phrase search.
    """
    term = term_row["term"]
    max_ch = term_row["max_chapter"]
    return [f'"{term} {ch}:"' for ch in range(1, max_ch + 1)]


# ---------------------------------------------------------------------------
# CourtListener fetchers
# ---------------------------------------------------------------------------

def search_opinions(
    client: RateLimitedClient,
    query: str,
    court: str,
    after: str,
    before: str,
) -> list[dict]:
    params = {
        "q": query,
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
        params = None
        page += 1

    return results


def fetch_opinion_full_text(client: RateLimitedClient, opinion_id: int) -> tuple[str, str]:
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
# Supabase helpers (v2 tables)
# ---------------------------------------------------------------------------

def get_or_create_opinion(sb: Client, cluster_id: int, result: dict, court_name: str) -> int | None:
    existing = (
        sb.table("opinions_v2")
        .select("id")
        .eq("courtlistener_cluster_id", cluster_id)
        .execute()
    )
    if existing.data:
        return existing.data[0]["id"]

    opinions_list = result.get("opinions", [])
    primary_opinion_id = opinions_list[0]["id"] if opinions_list else None

    citation = result.get("citation", [])
    if isinstance(citation, str):
        citation = [citation]

    row = {
        "courtlistener_cluster_id": cluster_id,
        "courtlistener_opinion_id": primary_opinion_id,
        "case_name":      result.get("caseName") or result.get("case_name", ""),
        "case_name_full": result.get("caseNameFull") or result.get("case_name_full") or None,
        "citation":       citation or None,
        "docket_number":  result.get("docketNumber") or result.get("docket_number") or None,
        "court_id":       result.get("court_id") or None,
        "court_name":     result.get("court") or court_name,
        "date_filed":     result.get("dateFiled") or result.get("date_filed") or None,
        "judge":          result.get("judge") or None,
        "status":         result.get("status") or None,
        "source_url":     f"https://www.courtlistener.com{result.get('absolute_url', '')}",
    }

    inserted = sb.table("opinions_v2").insert(row).execute()
    if inserted.data:
        return inserted.data[0]["id"]
    return None


def match_exists(sb: Client, opinion_id: int, term_id: int | None, search_query: str, excerpt: str) -> bool:
    q = (
        sb.table("opinion_matches_v2")
        .select("id")
        .eq("opinion_id", opinion_id)
        .eq("excerpt", excerpt)
    )
    if term_id:
        q = q.eq("search_term_id", term_id)
    if search_query:
        q = q.eq("search_query", search_query)
    return bool(q.execute().data)


def save_match(
    sb: Client,
    opinion_id: int,
    term_id: int | None,
    search_query: str,
    section: str,
    excerpt: str,
    context: str,
):
    if match_exists(sb, opinion_id, term_id, search_query, excerpt):
        return
    sb.table("opinion_matches_v2").insert({
        "opinion_id":      opinion_id,
        "search_term_id":  term_id,
        "search_query":    search_query,
        "opinion_section": section,
        "excerpt":         excerpt,
        "excerpt_context": context,
    }).execute()


def update_full_text(sb: Client, opinion_id: int, full_text: str):
    sb.table("opinions_v2").update({
        "full_text": full_text,
        "full_text_retrieved_at": datetime.utcnow().isoformat(),
    }).eq("id", opinion_id).execute()


def log_run(sb: Client, term_id: int | None, search_query: str, court: str, after: str, before: str) -> int:
    result = sb.table("ingestion_runs_v2").insert({
        "search_term_id":  term_id,
        "search_query":    search_query,
        "court_filter":    court,
        "date_filed_after":  after or None,
        "date_filed_before": before or None,
        "status": "running",
    }).execute()
    return result.data[0]["id"]


def finish_run(sb: Client, run_id: int, total: int, ingested: int, status: str = "completed", error: str = None):
    sb.table("ingestion_runs_v2").update({
        "total_results":    total,
        "opinions_ingested": ingested,
        "status":           status,
        "error_message":    error,
        "completed_at":     datetime.utcnow().isoformat(),
    }).eq("id", run_id).execute()


# ---------------------------------------------------------------------------
# Core ingestion logic
# ---------------------------------------------------------------------------

def ingest_query(
    term_row: dict,
    search_query: str,
    excerpt_pattern: re.Pattern,
    court: str,
    after: str,
    before: str,
    http: RateLimitedClient,
    sb: Client,
    dry_run: bool = False,
):
    """Run one search query and ingest matching opinions into v2 tables."""
    term_id = term_row["id"]
    print(f"\n{'='*60}", flush=True)
    print(f"Query: '{search_query}' | Court: {court} | After: {after}", flush=True)
    print(f"{'='*60}", flush=True)

    run_id = None if dry_run else log_run(sb, term_id, search_query, court, after, before)

    results = search_opinions(http, search_query, court, after, before)
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

            opinion_id = get_or_create_opinion(sb, cluster_id, result, court_name)
            if not opinion_id:
                print("  [skip] failed to insert opinion", flush=True)
                continue

            # Fetch full text
            all_text_parts = []
            for op in opinions_list:
                op_id   = op.get("id")
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
                snippet = opinions_list[0].get("snippet", "") if opinions_list else ""
                if snippet:
                    all_text_parts = [("snippet", html_to_text(snippet))]

            combined_text = "\n\n".join(text for _, text in all_text_parts)
            if combined_text:
                update_full_text(sb, opinion_id, combined_text)

            # Extract matches using excerpt_pattern (no pre-filter)
            match_count = 0
            for section_type, section_text in all_text_parts:
                hits = extract_excerpt(section_text, excerpt_pattern)
                for hit in hits:
                    save_match(
                        sb, opinion_id, term_id, search_query,
                        section_type, hit["excerpt"], hit["excerpt_context"],
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


def ingest_term(
    term_row: dict,
    court: str,
    after: str,
    before: str,
    http: RateLimitedClient,
    sb: Client,
    dry_run: bool = False,
):
    """
    Dispatch to ingest_query for each query associated with this term.
    Chapter-query terms produce one query per chapter; others produce one query.
    """
    excerpt_pattern = build_search_pattern(term_row)

    if term_row.get("use_chapter_queries"):
        queries = chapter_queries(term_row)
        print(f"\nTerm '{term_row['term']}': {len(queries)} chapter queries", flush=True)
    else:
        queries = [term_row["term"]]

    for query in queries:
        try:
            ingest_query(
                term_row=term_row,
                search_query=query,
                excerpt_pattern=excerpt_pattern,
                court=court,
                after=after,
                before=before,
                http=http,
                sb=sb,
                dry_run=dry_run,
            )
        except Exception as e:
            print(f"[SKIP] query '{query}' failed: {e}", flush=True)
            continue


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Biblical Jurisprudence ingestion pipeline v2")
    parser.add_argument("--court", default="scotus", help="CourtListener court ID, or 'all'")
    parser.add_argument("--term",  help="Single search term (overrides active terms)")
    parser.add_argument("--after",  default="1975-01-01", help="Filed after (YYYY-MM-DD)")
    parser.add_argument("--before", default="",           help="Filed before (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Search only, no writes")
    parser.add_argument("--list-courts", action="store_true")
    parser.add_argument("--list-terms",  action="store_true")
    args = parser.parse_args()

    if not SUPABASE_KEY:
        sys.exit("Error: set SUPABASE_SERVICE_KEY in .env")
    if not COURTLISTENER_API_KEY:
        sys.exit("Error: set COURTLISTENER_API_KEY in .env")

    http = RateLimitedClient(COURTLISTENER_API_KEY)
    sb   = create_client(SUPABASE_URL, SUPABASE_KEY)

    if args.list_courts:
        rows = sb.table("courts").select("*").order("state").execute().data
        for r in rows:
            print(f"  {r['id']:25} {r['name']}")
        return

    if args.list_terms:
        rows = sb.table("search_terms").select("*").order("term").execute().data
        for r in rows:
            chapter_info = f" ({r['max_chapter']} chapters)" if r.get("use_chapter_queries") else ""
            status = "active" if r["active"] else "inactive"
            print(f"  [{status:8}] {r['term']}{chapter_info}")
        return

    # Resolve search terms
    if args.term:
        term_rows = (
            sb.table("search_terms")
            .select("*")
            .eq("term", args.term)
            .execute()
            .data
        )
        if not term_rows:
            sys.exit(f"Error: term '{args.term}' not found in search_terms table")
    else:
        term_rows = (
            sb.table("search_terms")
            .select("*")
            .eq("active", True)
            .execute()
            .data
        )

    if not term_rows:
        sys.exit("No active search terms found.")

    print(f"Running {len(term_rows)} term(s) against court '{args.court}' (after {args.after})", flush=True)

    for term_row in term_rows:
        try:
            ingest_term(
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
