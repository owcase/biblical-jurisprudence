#!/usr/bin/env python3
"""
Biblical Jurisprudence — LLM Classifier v2

Classifies opinion_matches_v2 records as GENUINE judicial Bible citations
or FALSE_POSITIVE using Claude Haiku.

Key difference from llm_classify.py: the v2 dataset includes matches from
common-name Bible books (John, Matthew, Mark, Luke, James, Ruth, Samuel,
Kings, etc.), so the prompt explicitly handles:
  - Verse-citation format (John 3:16) → strong GENUINE signal
  - Common name only (John Doe, Matthew Johnson) → FALSE_POSITIVE

Usage:
    python llm_classify_v2.py --stats
    python llm_classify_v2.py --dry-run --sample 20
    python llm_classify_v2.py --sample 200
    python llm_classify_v2.py
"""

import argparse
import os
import sys
import time

import anthropic
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

SUPABASE_URL       = os.getenv("SUPABASE_URL")
SUPABASE_KEY       = os.getenv("SUPABASE_SERVICE_KEY")
ANTHROPIC_API_KEY  = os.getenv("ANTHROPIC_API_KEY")

MODEL            = "claude-haiku-4-5-20251001"
BATCH_SIZE       = 100
RATE_LIMIT_SLEEP = 0.5

SYSTEM_PROMPT = """You are a research assistant classifying excerpts from US judicial opinions.

Your task: determine whether a JUDGE is citing the Bible as a legal authority or reference in the court's own reasoning.

════════════════════════════════════════════════
VERSE-CITATION RULE (most important)
════════════════════════════════════════════════
If the excerpt contains a pattern like  BookName Chapter:Verse  — for example:
  "John 3:16", "Matthew 5:44", "1 Samuel 17:4", "II Kings 4:1", "James 2:17"
this is strong evidence of a genuine judicial Bible citation.

Mark GENUINE if a verse-citation pattern appears, UNLESS you are confident it is:
  • A case citation where the book name is a party (e.g. "John v. Smith, 42 F.3d 1")
  • A statute or rule citation that happens to contain numbers in that format
  • Clearly spoken by a witness, defendant, or juror (not the judge)

════════════════════════════════════════════════
GENERAL GENUINE CRITERIA
════════════════════════════════════════════════
Mark GENUINE if ANY of the following apply:
  • The judge directly quotes or paraphrases a biblical passage in the opinion's reasoning
  • The judge invokes biblical text to support a legal conclusion, analogy, or moral argument
  • The excerpt contains a bare biblical book reference in judicial prose (not quoted testimony)
  • You are UNCERTAIN whether the judge or another speaker is citing scripture — default to GENUINE

════════════════════════════════════════════════
FALSE_POSITIVE CRITERIA
════════════════════════════════════════════════
Mark FALSE_POSITIVE only when CONFIDENT that:
  • A common name appears only as a person's name with no verse citation
    (e.g. "John Doe", "Matthew Johnson", "Mark the defendant", "James argued that")
  • A party, witness, juror, or attorney (not the judge) is quoting the Bible,
    and this is unmistakably clear from speaker labels or Q/A transcript format
  • "Bible" is a party name in a case citation (e.g. "State v. Bible, 175 Ariz.")
  • The Bible is a physical object being described (found, stolen, held, burned)
  • The term appears only in an organization name (Bible Baptist Church, Watchtower)
  • Common-noun metaphor only ("the bible of accounting", "bible for investors")
  • Religious activity clearly unrelated to judicial reasoning (Bible study class,
    swearing on a Bible, reading the Bible in jail)

════════════════════════════════════════════════
DECISION RULE
════════════════════════════════════════════════
When in doubt, mark GENUINE.

Respond with exactly one word: GENUINE or FALSE_POSITIVE"""


def classify_excerpt_llm(client: anthropic.Anthropic, excerpt: str, context: str,
                          search_query: str = "", retries: int = 5) -> bool:
    """Returns True if false positive, False if genuine."""
    user_content = f"EXCERPT:\n{excerpt}"
    if context and context != excerpt:
        user_content += f"\n\nBROADER CONTEXT:\n{context[:1500]}"
    if search_query:
        user_content += f"\n\nSEARCH QUERY THAT FOUND THIS: {search_query}"

    for attempt in range(retries):
        try:
            message = client.messages.create(
                model=MODEL,
                max_tokens=10,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_content}],
            )
            response = message.content[0].text.strip().upper()
            return response != "GENUINE"
        except Exception as e:
            if attempt == retries - 1:
                raise
            wait = 2 ** attempt
            print(f"  [api retry {attempt+1}/{retries}] {e} — waiting {wait}s", flush=True)
            time.sleep(wait)


def fetch_pending(sb: Client, batch_size: int, offset: int) -> list[dict]:
    return (
        sb.table("opinion_matches_v2")
        .select("id, excerpt, excerpt_context, search_query")
        .eq("verified", False)
        .eq("llm_reviewed", False)
        .range(offset, offset + batch_size - 1)
        .execute()
        .data
    )


def write_with_retry(sb: Client, match_id: int, is_fp: bool, retries: int = 5):
    for attempt in range(retries):
        try:
            sb.table("opinion_matches_v2").update({
                "llm_reviewed":       True,
                "llm_false_positive": is_fp,
            }).eq("id", match_id).execute()
            return
        except Exception as e:
            if attempt == retries - 1:
                raise
            wait = 2 ** attempt
            print(f"  [db retry {attempt+1}/{retries}] {e} — waiting {wait}s", flush=True)
            time.sleep(wait)


def write_results(sb: Client, results: list[tuple[int, bool]]):
    for match_id, is_fp in results:
        write_with_retry(sb, match_id, is_fp)


def show_stats(sb: Client):
    total       = sb.table("opinion_matches_v2").select("id", count="exact").execute().count
    verified    = sb.table("opinion_matches_v2").select("id", count="exact").eq("verified", True).execute().count
    llm_reviewed= sb.table("opinion_matches_v2").select("id", count="exact").eq("llm_reviewed", True).execute().count
    llm_fp      = sb.table("opinion_matches_v2").select("id", count="exact").eq("llm_false_positive", True).execute().count
    llm_genuine = sb.table("opinion_matches_v2").select("id", count="exact").eq("llm_false_positive", False).eq("llm_reviewed", True).execute().count
    pending     = sb.table("opinion_matches_v2").select("id", count="exact").eq("verified", False).eq("llm_reviewed", False).execute().count

    print(f"opinion_matches_v2 total:        {total}")
    print(f"  manually verified (genuine):   {verified}")
    print(f"  LLM reviewed:                  {llm_reviewed}")
    print(f"    LLM false positive:          {llm_fp}")
    print(f"    LLM genuine (for review):    {llm_genuine}")
    print(f"  pending LLM review:            {pending}")


def main():
    parser = argparse.ArgumentParser(description="LLM classifier v2 (opinion_matches_v2)")
    parser.add_argument("--dry-run", action="store_true", help="Print verdicts without writing")
    parser.add_argument("--sample",  type=int, default=0, help="Classify this many then stop")
    parser.add_argument("--stats",   action="store_true", help="Show stats and exit")
    args = parser.parse_args()

    if not SUPABASE_KEY:
        sys.exit("Error: set SUPABASE_SERVICE_KEY in .env")
    if not ANTHROPIC_API_KEY and not args.stats:
        sys.exit("Error: set ANTHROPIC_API_KEY in .env")

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    if args.stats:
        show_stats(sb)
        return

    anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    total_processed = 0
    total_genuine   = 0
    total_fp        = 0
    offset          = 0

    print(f"Starting LLM classification v2 (model: {MODEL})", flush=True)
    if args.dry_run:
        print("[dry run] no writes will be made\n", flush=True)
    if args.sample:
        print(f"[sample mode] will stop after {args.sample} matches\n", flush=True)

    while True:
        fetch_size = BATCH_SIZE
        if args.sample:
            fetch_size = min(BATCH_SIZE, args.sample - total_processed)
            if fetch_size <= 0:
                break

        rows = fetch_pending(sb, fetch_size, offset)
        if not rows:
            break

        results = []
        for row in rows:
            try:
                is_fp = classify_excerpt_llm(
                    anthropic_client,
                    row["excerpt"] or "",
                    row["excerpt_context"] or "",
                    row.get("search_query") or "",
                )
                results.append((row["id"], is_fp))
                total_processed += 1
                if is_fp:
                    total_fp += 1
                else:
                    total_genuine += 1

                if args.dry_run:
                    label = "FALSE_POSITIVE" if is_fp else "GENUINE"
                    query_hint = f" [{row.get('search_query', '')}]"
                    print(f"[{total_processed}] {label}{query_hint}: {(row['excerpt'] or '')[:120]}", flush=True)

                time.sleep(RATE_LIMIT_SLEEP)

            except Exception as e:
                print(f"  [warn] failed to classify id {row['id']}: {e}", flush=True)
                continue

        if not args.dry_run and results:
            write_results(sb, results)

        print(
            f"  Processed {total_processed} | genuine: {total_genuine} | fp: {total_fp}",
            flush=True,
        )

        if args.sample and total_processed >= args.sample:
            break

        if len(rows) < fetch_size:
            break

    print(f"\nDone. {total_processed} classified: {total_genuine} genuine, {total_fp} false positive.")


if __name__ == "__main__":
    main()
