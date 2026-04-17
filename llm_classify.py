#!/usr/bin/env python3
"""
Biblical Jurisprudence — LLM Classifier

Uses Claude Haiku to classify pending opinion_matches as genuine judicial
Bible citations or false positives.

A match is genuine only if the JUDGE is invoking the Bible as an authority
in the court's own reasoning — not summarising counsel's argument, not
describing facts, not quoting witnesses or jurors.

Usage:
    python llm_classify.py --dry-run        # print sample classifications, no writes
    python llm_classify.py --sample 50      # classify 50 matches then stop
    python llm_classify.py                  # classify all pending matches
    python llm_classify.py --stats          # show LLM review queue stats
"""

import argparse
import os
import sys
import time

import anthropic
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

MODEL = "claude-haiku-4-5-20251001"
BATCH_SIZE = 100       # fetch from DB in batches of this size
WRITE_BATCH = 50       # write results back in batches of this size
RATE_LIMIT_SLEEP = 0.5 # seconds between API calls

SYSTEM_PROMPT = """You are a research assistant helping to classify excerpts from US state court judicial opinions.

Your task is to determine whether the JUDGE is citing the Bible as an authority or reference in the court's own legal reasoning.

Mark as GENUINE (judge cites Bible) if:
- The judge directly quotes or references a biblical passage in the court's reasoning or analysis
- The judge invokes biblical text to support a legal conclusion, analogy, or moral argument
- The judge uses biblical language as part of their own written opinion

Mark as FALSE_POSITIVE if:
- The excerpt describes a party, witness, or juror referencing the Bible
- The excerpt is about counsel (prosecutor or defense) quoting or arguing from the Bible
- The excerpt discusses whether counsel's Bible reference was permissible/improper
- The excerpt describes physical Bible objects (found, stolen, held, etc.)
- The excerpt is a trial transcript quote (Q/A format)
- The excerpt involves a case citation where "Bible" is a party name (e.g. "State v. Bible")
- The excerpt describes religious activity (Bible study, swearing on a Bible, etc.)
- The Bible is mentioned only as background fact or character evidence
- The term appears in an organization name (Bible Baptist Church, etc.)
- The term is a common noun metaphor ("the bible of accounting")

Respond with exactly one word: GENUINE or FALSE_POSITIVE"""


def classify_excerpt_llm(client: anthropic.Anthropic, excerpt: str, context: str) -> bool:
    """
    Returns True if false positive, False if genuine.
    """
    user_content = f"EXCERPT:\n{excerpt}"
    if context and context != excerpt:
        user_content += f"\n\nBROADER CONTEXT:\n{context[:1500]}"

    message = client.messages.create(
        model=MODEL,
        max_tokens=10,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_content}],
    )
    response = message.content[0].text.strip().upper()
    return response != "GENUINE"


def fetch_pending(sb: Client, batch_size: int, offset: int) -> list[dict]:
    """Fetch unreviewed, non-false-positive matches not yet LLM-reviewed."""
    return (
        sb.table("opinion_matches")
        .select("id, excerpt, excerpt_context")
        .eq("verified", False)
        .eq("false_positive", False)
        .eq("llm_reviewed", False)
        .range(offset, offset + batch_size - 1)
        .execute()
        .data
    )


def write_results(sb: Client, results: list[tuple[int, bool]]):
    """Write LLM verdicts back to DB."""
    for match_id, is_fp in results:
        sb.table("opinion_matches").update({
            "llm_reviewed": True,
            "llm_false_positive": is_fp,
        }).eq("id", match_id).execute()


def show_stats(sb: Client):
    total = sb.table("opinion_matches").select("id", count="exact").execute().count
    verified = sb.table("opinion_matches").select("id", count="exact").eq("verified", True).execute().count
    regex_fp = sb.table("opinion_matches").select("id", count="exact").eq("false_positive", True).execute().count
    llm_reviewed = sb.table("opinion_matches").select("id", count="exact").eq("llm_reviewed", True).execute().count
    llm_fp = sb.table("opinion_matches").select("id", count="exact").eq("llm_false_positive", True).execute().count
    llm_genuine = sb.table("opinion_matches").select("id", count="exact").eq("llm_false_positive", False).eq("llm_reviewed", True).execute().count
    pending_llm = sb.table("opinion_matches").select("id", count="exact").eq("verified", False).eq("false_positive", False).eq("llm_reviewed", False).execute().count

    print(f"opinion_matches total:           {total}")
    print(f"  manually verified (genuine):   {verified}")
    print(f"  regex false positives:         {regex_fp}")
    print(f"  LLM reviewed:                  {llm_reviewed}")
    print(f"    LLM false positive:          {llm_fp}")
    print(f"    LLM genuine (for review):    {llm_genuine}")
    print(f"  pending LLM review:            {pending_llm}")


def main():
    parser = argparse.ArgumentParser(description="LLM-based false positive classifier")
    parser.add_argument("--dry-run", action="store_true", help="Print verdicts without writing")
    parser.add_argument("--sample", type=int, default=0, help="Only classify this many matches")
    parser.add_argument("--stats", action="store_true", help="Show stats and exit")
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
    total_genuine = 0
    total_fp = 0
    offset = 0

    print(f"Starting LLM classification (model: {MODEL})", flush=True)
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
                )
                results.append((row["id"], is_fp))
                total_processed += 1
                if is_fp:
                    total_fp += 1
                else:
                    total_genuine += 1

                if args.dry_run:
                    label = "FALSE_POSITIVE" if is_fp else "GENUINE"
                    print(f"[{total_processed}] {label}: {(row['excerpt'] or '')[:120]}", flush=True)

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

        # If we got fewer rows than requested, we've reached the end
        if len(rows) < fetch_size:
            break

    print(f"\nDone. {total_processed} classified: {total_genuine} genuine, {total_fp} false positive.")


if __name__ == "__main__":
    main()
