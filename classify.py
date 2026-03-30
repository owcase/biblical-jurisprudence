#!/usr/bin/env python3
"""
Biblical Jurisprudence — Auto-classifier

Applies named regex rules to unreviewed opinion_matches and marks
obvious false positives automatically. Ambiguous matches are left
for manual review.

Usage:
    python classify.py               # classify all unreviewed matches
    python classify.py --dry-run     # preview without writing
    python classify.py --stats       # show current review queue stats
"""

import argparse
import os
import re
import sys

from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

# ---------------------------------------------------------------------------
# Classification rules
# Each rule is (rule_name, compiled_regex).
# A match is auto-classified as false_positive if ANY rule fires on its excerpt.
# Rules are checked in order; first match wins (for the note label).
# ---------------------------------------------------------------------------

RULES: list[tuple[str, re.Pattern]] = [
    # Case citations: "Bible, 175 Ariz." / "See Bible" / "v. Bible" / "Bible v."
    (
        "case_citation_bible",
        re.compile(
            r"(\bSee\s+Bible\b"
            r"|\bBible\s+v\."
            r"|\bv\.\s+Bible\b"
            r"|\bBible,\s+\d+"
            r"|\bBible\s+\d+\s+[A-Z]"
            r")",
            re.IGNORECASE,
        ),
    ),
    # Trial transcript Q/A format
    (
        "transcript_qa",
        re.compile(
            r"(^\s*\"?Q[\s\.]+.{10,}"
            r"|^\s*\"?A[\s\.]+.{10,}"
            r"|\bQ\s+Did\b|\bQ\s+Do\b|\bQ\s+What\b|\bQ\s+Were\b|\bQ\s+Have\b"
            r"|\bQ\s+Are\b|\bQ\s+Is\b|\bQ\s+Was\b|\bQ\s+When\b|\bQ\s+How\b"
            r"|\bA\s+Yes\b|\bA\s+No\b|\bA\s+Yeah\b"
            r")",
            re.IGNORECASE | re.MULTILINE,
        ),
    ),
    # Speaker labels (non-judge speakers)
    (
        "speaker_label",
        re.compile(
            r"(THE\s+DEFENDANT\s*:"
            r"|THE\s+WITNESS\s*:"
            r"|\[?DEFENSE\s+COUNSEL\]?\s*:"
            r"|\[?PROSECUTOR\]?\s*:"
            r"|PROSPECTIVE\s+JUROR\s*:"
            r"|MR\.\s+[A-Z]+\s*:"
            r"|MS\.\s+[A-Z]+\s*:"
            r")",
            re.IGNORECASE,
        ),
    ),
    # Physical Bible as object (not invoked as authority)
    (
        "physical_bible",
        re.compile(
            r"(burned?\s+Bible"
            r"|Bible\s+(out\s+of|from|in)\s+the"
            r"|(read|reading|reads)\s+(aloud\s+)?(from|out\s+of)\s+(a|the|his|her|their)\s+Bible"
            r"|(found|got|left|hold|carried|carrying|had)\s+(a|the|his|her)\s+Bible"
            r"|pocket\s+Bible"
            r"|family\s+Bible"
            r"|hold\s+the\s+Bible"
            r")",
            re.IGNORECASE,
        ),
    ),
    # Bible used as metaphor (common noun)
    (
        "bible_metaphor",
        re.compile(
            r"(the\s+bible\s+of\s+\w+"
            r"|they\s+call\s+the\s+bible\s+of"
            r"|what\s+they\s+call\s+the\s+bible"
            r")",
            re.IGNORECASE,
        ),
    ),
    # Satanic Bible
    (
        "satanic_bible",
        re.compile(r"\bSatanic\s+Bible\b", re.IGNORECASE),
    ),
    # "Degree in Bible" (academic credential)
    (
        "bible_degree",
        re.compile(r"\bdegree\s+in\s+Bible\b", re.IGNORECASE),
    ),
    # Bible study / Bible school / Bible class / Bible institute (activity, not citation)
    (
        "bible_activity",
        re.compile(
            r"\bBible\s+(study|school|class|institute|camp|college\s+course|correspondence\s+course)\b",
            re.IGNORECASE,
        ),
    ),
    # "Bible readings in jury room" / "read Bibles in hotel rooms" (juror misconduct claims)
    (
        "juror_bible_reading",
        re.compile(
            r"(jurors?\s+(read|reading|prayed\s+and\s+read)\s+(from\s+)?(the\s+)?Bibles?"
            r"|Bibles?\s+found\s+in\s+(their\s+)?hotel\s+rooms?"
            r"|Bible\s+readings?\s+(in|during)\s+(the\s+)?jury"
            r"|reading\s+(the\s+)?Bible\s+(and\s+praying\s+)?in\s+the\s+jury"
            r")",
            re.IGNORECASE,
        ),
    ),
    # "Well-versed in the Bible" / "studied the Bible" / "offered him a Bible" (character evidence)
    (
        "character_evidence",
        re.compile(
            r"(well.versed\s+in\s+the\s+Bible"
            r"|(study|studying|studies|studied)\s+the\s+Bible"
            r"|offered\s+(him|her|them)\s+a\s+Bible"
            r"|completed\s+a\s+Bible"
            r")",
            re.IGNORECASE,
        ),
    ),
    # "sworn on the Bible" — witness oath, not judicial citation
    (
        "sworn_on_bible",
        re.compile(r"\bsworn\s+on\s+(the\s+)?(.+\s+)?[Bb]ible\b", re.IGNORECASE),
    ),
    # Bible case / Bible cover as physical objects
    (
        "bible_physical_object",
        re.compile(
            r"(Bible\s+case\b"
            r"|Bible\s+cover\b"
            r"|Bible\s+on\s+(a\s+)?table"
            r"|Bible\s+in\s+(the\s+)?car"
            r"|Bible\s+(was\s+)?found"
            r"|(stole?|stolen|took|taken)\s+(a\s+|the\s+)?Bible"
            r"|(a\s+)?Bible\s+(and\s+a|,\s*)(?!says|teaches|tells|states|describes)"
            r")",
            re.IGNORECASE,
        ),
    ),
    # Judge ruling on whether prosecutor's/counsel's Bible reference was permissible
    # (Bible is the subject of discussion, not what the judge is invoking as authority)
    (
        "prosecutor_bible_ref",
        re.compile(
            r"(prosecutor.{0,30}(reference|references|quoted|quoting|citing|urging|relying).{0,20}Bible"
            r"|references?\s+to\s+the\s+Bible\s+(in|are|is|was|were|had|have|do\s+not|does\s+not)"
            r"|quotes?\s+from\s+the\s+Bible\s+(are|is|in|were|was)"
            r"|Bible.{0,30}(impermissible|improper|allowed|permissible|prohibited)"
            r"|counsel.{0,40}(quoted?|arguing|reference).{0,20}Bible"
            r"|objection.{0,30}Bible"
            r"|This\s+Court\s+has\s+(held|found|previously).{0,60}Bible"
            r")",
            re.IGNORECASE,
        ),
    ),
    # Juror/prospective juror stating personal Bible beliefs (voir dire)
    (
        "juror_bible_belief",
        re.compile(
            r"(juror.{0,50}Bible"
            r"|Bible.{0,50}(juror|deliberat|jury\s+room|hotel\s+room)"
            r"|habit\s+of\s+reading\s+the\s+Bible"
            r"|she\s+(said|stated|indicated).{0,30}Bible"
            r"|he\s+(said|stated|indicated).{0,30}Bible.*kill"
            r")",
            re.IGNORECASE,
        ),
    ),
]


def classify_excerpt(excerpt: str) -> tuple[bool, str | None]:
    """
    Check excerpt against all rules.
    Returns (is_false_positive, rule_name). rule_name is None if not classified.
    """
    for rule_name, pattern in RULES:
        if pattern.search(excerpt):
            return True, rule_name
    return False, None


def run_classifier(sb: Client, dry_run: bool = False) -> dict[str, int]:
    """Fetch unreviewed matches and apply classification rules."""
    rows = (
        sb.table("opinion_matches")
        .select("id, excerpt")
        .eq("verified", False)
        .eq("false_positive", False)
        .execute()
        .data
    )

    print(f"Fetched {len(rows)} unreviewed matches.", flush=True)

    counts: dict[str, int] = {}
    to_update: list[tuple[int, str]] = []

    for row in rows:
        is_fp, rule_name = classify_excerpt(row["excerpt"] or "")
        if is_fp:
            counts[rule_name] = counts.get(rule_name, 0) + 1
            to_update.append((row["id"], rule_name))

    print(f"\nAuto-classified {len(to_update)} of {len(rows)} matches as false positives:")
    for rule, count in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"  {rule:35} {count}")
    print(f"\n  Remaining for manual review: {len(rows) - len(to_update)}")

    if dry_run:
        print("\n[dry run] no changes written.")
        return counts

    # Write in batches of 50
    for i in range(0, len(to_update), 50):
        batch = to_update[i:i + 50]
        ids = [r[0] for r in batch]
        # Update each with its specific rule name in notes
        for match_id, rule_name in batch:
            sb.table("opinion_matches").update({
                "false_positive": True,
                "notes": f"auto-classified: {rule_name}",
            }).eq("id", match_id).execute()

    print(f"\nDone. {len(to_update)} matches marked as false positive.")
    return counts


def show_stats(sb: Client):
    """Print current review queue stats."""
    total = sb.table("opinion_matches").select("id", count="exact").execute().count
    verified = sb.table("opinion_matches").select("id", count="exact").eq("verified", True).execute().count
    fp = sb.table("opinion_matches").select("id", count="exact").eq("false_positive", True).execute().count
    pending = sb.table("opinion_matches").select("id", count="exact").eq("verified", False).eq("false_positive", False).execute().count

    print(f"opinion_matches total:      {total}")
    print(f"  verified (genuine):       {verified}")
    print(f"  false positive:           {fp}")
    print(f"  pending manual review:    {pending}")


def main():
    parser = argparse.ArgumentParser(description="Auto-classify opinion_matches false positives")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    parser.add_argument("--stats", action="store_true", help="Show queue stats and exit")
    args = parser.parse_args()

    if not SUPABASE_KEY:
        sys.exit("Error: set SUPABASE_SERVICE_KEY in .env")

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    if args.stats:
        show_stats(sb)
        return

    run_classifier(sb, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
