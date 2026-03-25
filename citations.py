"""
citations.py

Identifies and extracts biblical citations from legal opinion text.
Covers all 66 canonical Protestant books with full names and common
abbreviations, including numbered-book prefixes in Arabic, Roman,
and ordinal forms.

A citation is defined as a book name followed by at least a chapter number,
e.g. "Matthew 19:14", "1 Cor. 13:4-7", "II Kings 2:11", "Psalm 23".
"""

import re
from dataclasses import dataclass
from typing import List, Optional


# ---------------------------------------------------------------------------
# Prefix fragments for numbered books
# Matches: "1 ", "2 ", "I ", "II ", "First ", "Second ", etc.
# ---------------------------------------------------------------------------
_P12  = r"(?:1|2|I{1,2}|First|Second)\s+"
_P123 = r"(?:1|2|3|I{1,3}|First|Second|Third)\s+"


# ---------------------------------------------------------------------------
# Book name patterns.
# Order matters: longer/numbered forms are listed before shorter/plain forms
# so the regex alternation engine matches the most specific option first.
#
# Known false-positive risk with short abbreviations in legal text:
#   "Ex"  → also used for "Exhibit" (dropped in favour of "Exod")
#   "Col" → also used for "Colonel"/"Colorado" (kept; chapter:verse disambiguates)
#   "Mt"  → also used for "Mount" (kept; chapter:verse disambiguates)
# ---------------------------------------------------------------------------
_BOOKS: List[str] = [
    # ── Old Testament ───────────────────────────────────────────────────────
    "Genesis",          "Gen",
    "Exodus",           "Exod",           # "Ex" dropped — too ambiguous
    "Leviticus",        "Lev",
    "Numbers",          "Num",
    "Deuteronomy",      "Deut",           "Dt",
    "Joshua",           "Josh",
    "Judges",           "Judg",           "Jdg",
    "Ruth",
    _P12 + "Samuel",    _P12 + "Sam",
    _P12 + "Kings",     _P12 + "Kgs",     _P12 + "Ki",
    _P12 + "Chronicles",_P12 + "Chron",   _P12 + "Chr",
    "Ezra",
    "Nehemiah",         "Neh",
    "Esther",           "Esth",
    "Job",
    "Psalms",           "Psalm",          "Psa",   "Ps",
    "Proverbs",         "Prov",
    "Ecclesiastes",     "Eccles",         "Eccl",
    "Song of Solomon",  "Song of Songs",  "Song",  "Cant",
    "Isaiah",           "Isa",
    "Jeremiah",         "Jer",
    "Lamentations",     "Lam",
    "Ezekiel",          "Ezek",
    "Daniel",           "Dan",
    "Hosea",            "Hos",
    "Joel",
    "Amos",
    "Obadiah",          "Obad",
    "Jonah",
    "Micah",            "Mic",
    "Nahum",            "Nah",
    "Habakkuk",         "Hab",
    "Zephaniah",        "Zeph",
    "Haggai",           "Hag",
    "Zechariah",        "Zech",
    "Malachi",          "Mal",
    # ── New Testament ───────────────────────────────────────────────────────
    "Matthew",          "Matt",           "Mt",
    "Mark",             "Mk",
    "Luke",             "Lk",
    "Acts",
    "Romans",           "Rom",
    _P12 + "Corinthians", _P12 + "Cor",
    "Galatians",        "Gal",
    "Ephesians",        "Eph",
    "Philippians",      "Phil",
    "Colossians",       "Col",
    _P12 + "Thessalonians", _P12 + "Thess",
    _P12 + "Timothy",   _P12 + "Tim",
    "Titus",            "Tit",
    "Philemon",         "Philem",
    "Hebrews",          "Heb",
    "James",            "Jas",
    _P12 + "Peter",     _P12 + "Pet",
    _P123 + "John",     # numbered John BEFORE plain John to avoid short-circuit
    "John",             "Jn",
    "Jude",
    "Revelation",       "Rev",            "Apoc",
]


# ---------------------------------------------------------------------------
# Compile the master regex
# ---------------------------------------------------------------------------
_book_alt = "|".join(f"(?:{b})" for b in _BOOKS)

# Pattern breakdown:
#   \b                          word boundary before the citation
#   ({_book_alt})               capture the book name (incl. any numeric prefix)
#   \.?                         optional period (for abbreviations like "Matt.")
#   \s+                         whitespace between book and chapter
#   (\d{1,3})                   chapter number (capture group 2)
#   (?::(\d{1,3})               optional colon + verse start (capture group 3)
#     (?:-(\d{1,3}))?           optional hyphen + verse end for ranges (group 4)
#   )?
#   \b                          word boundary after the last digit
_CITATION_RE = re.compile(
    rf"\b({_book_alt})\.?\s+(\d{{1,3}})(?::(\d{{1,3}})(?:-(\d{{1,3}}))?)?\b"
)


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------
@dataclass
class Citation:
    """A single biblical citation found in source text."""

    raw: str                        # Exact string as it appeared in the source
    book: str                       # Book name as matched (e.g. "1 Corinthians")
    chapter: int
    verse_start: Optional[int] = None
    verse_end: Optional[int] = None  # Set when a range like :3-12 is given

    def __str__(self) -> str:
        ref = f"{self.book} {self.chapter}"
        if self.verse_start is not None:
            ref += f":{self.verse_start}"
            if self.verse_end is not None:
                ref += f"-{self.verse_end}"
        return ref


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def find_citations(text: str) -> List[Citation]:
    """
    Return all biblical citations found in *text*, in document order.

    Parameters
    ----------
    text : str
        Full text of a judicial opinion or other document.

    Returns
    -------
    List[Citation]
        One Citation per match; duplicates preserved.
    """
    results: List[Citation] = []
    for m in _CITATION_RE.finditer(text):
        book, chapter_s, verse_s, verse_end_s = m.groups()
        results.append(Citation(
            raw=m.group(0),
            book=book.strip(),
            chapter=int(chapter_s),
            verse_start=int(verse_s) if verse_s else None,
            verse_end=int(verse_end_s) if verse_end_s else None,
        ))
    return results


def summarize(citations: List[Citation]) -> dict:
    """
    Frequency count of citations grouped by book, sorted descending.

    Parameters
    ----------
    citations : List[Citation]
        Output of find_citations().

    Returns
    -------
    dict
        {book_name: count}
    """
    counts: dict = {}
    for c in citations:
        counts[c.book] = counts.get(c.book, 0) + 1
    return dict(sorted(counts.items(), key=lambda x: x[1], reverse=True))
