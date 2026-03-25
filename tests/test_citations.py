"""
tests/test_citations.py

Basic tests for the citations module.
Run with: pytest tests/
"""

import pytest
from citations import find_citations, summarize, Citation


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def one(text: str) -> Citation:
    """Assert exactly one citation found and return it."""
    results = find_citations(text)
    assert len(results) == 1, f"Expected 1 citation, got {len(results)}: {results}"
    return results[0]


def none(text: str) -> None:
    """Assert no citations found."""
    results = find_citations(text)
    assert len(results) == 0, f"Expected 0 citations, got {len(results)}: {results}"


# ---------------------------------------------------------------------------
# Full book names — chapter:verse
# ---------------------------------------------------------------------------

class TestFullNames:
    def test_matthew(self):
        c = one("The court noted Matthew 19:14 in its analysis.")
        assert c.book == "Matthew"
        assert c.chapter == 19
        assert c.verse_start == 14

    def test_john(self):
        c = one("John 11:35 is the shortest verse in the Bible.")
        assert c.book == "John"
        assert c.chapter == 11
        assert c.verse_start == 35

    def test_genesis(self):
        c = one("Genesis 1:1 describes creation.")
        assert c.book == "Genesis"
        assert c.chapter == 1
        assert c.verse_start == 1

    def test_revelation(self):
        c = one("See Revelation 21:4.")
        assert c.book == "Revelation"
        assert c.chapter == 21
        assert c.verse_start == 4

    def test_song_of_solomon(self):
        c = one("The opinion quoted Song of Solomon 2:1.")
        assert c.book == "Song of Solomon"
        assert c.chapter == 2
        assert c.verse_start == 1

    def test_song_of_songs(self):
        c = one("See Song of Songs 1:1.")
        assert c.book == "Song of Songs"


# ---------------------------------------------------------------------------
# Numbered books — all prefix forms
# ---------------------------------------------------------------------------

class TestNumberedBooks:
    def test_arabic_numeral(self):
        c = one("1 Corinthians 13:4 is cited frequently.")
        assert c.book == "1 Corinthians"
        assert c.chapter == 13
        assert c.verse_start == 4

    def test_roman_numeral(self):
        c = one("The court cited II Kings 2:11.")
        assert c.book == "II Kings"
        assert c.chapter == 2
        assert c.verse_start == 11

    def test_ordinal(self):
        c = one("First Peter 3:15 was invoked.")
        assert c.book == "First Peter"
        assert c.chapter == 3
        assert c.verse_start == 15

    def test_second_ordinal(self):
        c = one("Second Timothy 3:16 is often cited.")
        assert c.book == "Second Timothy"
        assert c.chapter == 3
        assert c.verse_start == 16

    def test_third_john(self):
        c = one("3 John 1:4 was quoted.")
        assert c.book == "3 John"
        assert c.chapter == 1
        assert c.verse_start == 4

    def test_numbered_john_not_confused_with_plain_john(self):
        """'1 John 3:16' should match '1 John', not plain 'John'."""
        c = one("1 John 3:16 is often quoted.")
        assert c.book == "1 John"

    def test_plain_john_still_works(self):
        c = one("John 3:16 is perhaps the most cited verse.")
        assert c.book == "John"


# ---------------------------------------------------------------------------
# Abbreviations
# ---------------------------------------------------------------------------

class TestAbbreviations:
    def test_matt_with_period(self):
        c = one("See Matt. 19:14.")
        assert c.book == "Matt"
        assert c.chapter == 19
        assert c.verse_start == 14

    def test_gen_no_period(self):
        c = one("Gen 1:1")
        assert c.book == "Gen"

    def test_ps(self):
        c = one("Ps 23:1")
        assert c.book == "Ps"

    def test_abbreviated_numbered_book(self):
        c = one("1 Cor. 13:4")
        assert c.book == "1 Cor"
        assert c.chapter == 13

    def test_rev(self):
        c = one("Rev 22:21 closes the canon.")
        assert c.book == "Rev"


# ---------------------------------------------------------------------------
# Verse ranges
# ---------------------------------------------------------------------------

class TestVerseRanges:
    def test_range(self):
        c = one("Matthew 5:3-12 covers the Beatitudes.")
        assert c.book == "Matthew"
        assert c.chapter == 5
        assert c.verse_start == 3
        assert c.verse_end == 12

    def test_range_abbreviated(self):
        c = one("Rom 8:38-39")
        assert c.verse_start == 38
        assert c.verse_end == 39


# ---------------------------------------------------------------------------
# Chapter-only citations
# ---------------------------------------------------------------------------

class TestChapterOnly:
    def test_psalm_23(self):
        c = one("Psalm 23 offers comfort.")
        assert c.book == "Psalm"
        assert c.chapter == 23
        assert c.verse_start is None

    def test_proverbs_chapter(self):
        c = one("The wisdom of Proverbs 3 is invoked.")
        assert c.book == "Proverbs"
        assert c.chapter == 3


# ---------------------------------------------------------------------------
# Multiple citations in one text
# ---------------------------------------------------------------------------

class TestMultiple:
    def test_two_citations(self):
        text = "The opinion cites Matthew 5:17 and Romans 13:1."
        results = find_citations(text)
        assert len(results) == 2
        assert results[0].book == "Matthew"
        assert results[1].book == "Romans"

    def test_summarize(self):
        text = "Matthew 5:1, Matthew 5:2, John 3:16"
        results = find_citations(text)
        summary = summarize(results)
        assert summary["Matthew"] == 2
        assert summary["John"] == 1


# ---------------------------------------------------------------------------
# Should NOT match
# ---------------------------------------------------------------------------

class TestNonMatches:
    def test_no_chapter(self):
        """A bare book name with no chapter should not match (avoids noise)."""
        # "John" alone appears constantly in legal text as a person's name
        none("Justice John wrote the majority opinion.")

    def test_partial_word(self):
        """Should not match book names embedded in longer words."""
        none("The court examined malicious intent.")   # "Mal" inside "malicious"
        none("The statute was amended.")               # "Amos" not present

    def test_exhibit_reference(self):
        """'Ex' was intentionally dropped; 'Exod' required for Exodus."""
        # "Ex. 1" is a common exhibit reference in legal text
        none("See Ex. 1 for the contract.")
