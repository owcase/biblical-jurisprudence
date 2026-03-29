# Biblical Jurisprudence — Project Plan

> A research project to systematically identify and catalogue instances where US state and federal judges reference the Bible in written rulings (post-1980). Output: a journal article.

---

## Status Key
- `[ ]` Not started
- `[x]` Complete
- `[-]` In progress

---

## Phase 0: Infrastructure ✅

- [x] Repository created
- [x] Supabase project created (`pwrtjhvhbofoteiflhna`)
- [x] Supabase MCP configured and live
- [x] CourtListener MCP configured and live
- [x] `.mcp.json` in project root (gitignored)
- [x] Database schema designed and applied
- [x] `search_terms` table seeded with initial vocabulary

---

## Phase 1: Database Schema

### Tables

#### `courts`
Reference table for all US courts.

| Column | Type | Notes |
|---|---|---|
| `id` | `TEXT PK` | CourtListener court ID (e.g. `ala`, `scotus`) |
| `name` | `TEXT` | Full court name |
| `short_name` | `TEXT` | Abbreviated name |
| `jurisdiction` | `TEXT` | `federal` or `state` |
| `level` | `TEXT` | `supreme`, `appellate`, `trial`, `other` |
| `state` | `TEXT` | State name (NULL for federal) |

#### `search_terms`
Expandable vocabulary of terms to search for.

| Column | Type | Notes |
|---|---|---|
| `id` | `SERIAL PK` | |
| `term` | `TEXT UNIQUE` | The search term (e.g. `Bible`, `Matthew`, `Genesis 1:1`) |
| `category` | `TEXT` | `general`, `book_name`, `verse_reference`, `phrase`, `figure`, `concept` |
| `testament` | `TEXT` | `old`, `new`, `both`, `other`, NULL |
| `active` | `BOOLEAN` | Whether to include in current searches |
| `notes` | `TEXT` | Optional context |
| `created_at` | `TIMESTAMPTZ` | |

#### `opinions`
One row per unique judicial opinion.

| Column | Type | Notes |
|---|---|---|
| `id` | `SERIAL PK` | |
| `courtlistener_cluster_id` | `INTEGER UNIQUE` | CourtListener cluster ID |
| `courtlistener_opinion_id` | `INTEGER` | Primary opinion document ID |
| `case_name` | `TEXT` | Short case name |
| `case_name_full` | `TEXT` | Full case name |
| `citation` | `TEXT[]` | Array of citation strings |
| `docket_number` | `TEXT` | |
| `court_id` | `TEXT FK → courts` | |
| `court_name` | `TEXT` | Denormalised for convenience |
| `date_filed` | `DATE` | |
| `judge` | `TEXT` | |
| `case_type` | `TEXT` | `criminal`, `civil`, `constitutional`, `family`, `other` — may require manual coding |
| `outcome` | `TEXT` | Ruling direction — nullable, manual coding later |
| `status` | `TEXT` | `Published` / `Unpublished` |
| `source_url` | `TEXT` | CourtListener opinion URL |
| `full_text` | `TEXT` | NULL now; populated in future phase |
| `full_text_retrieved_at` | `TIMESTAMPTZ` | NULL until full text fetched |
| `created_at` | `TIMESTAMPTZ` | |
| `updated_at` | `TIMESTAMPTZ` | |

#### `opinion_matches`
One row per biblical reference found — the core research record.

| Column | Type | Notes |
|---|---|---|
| `id` | `SERIAL PK` | |
| `opinion_id` | `INTEGER FK → opinions` | |
| `search_term_id` | `INTEGER FK → search_terms` | |
| `opinion_section` | `TEXT` | `lead-opinion`, `dissent`, `concurrence`, `rehearing` |
| `excerpt` | `TEXT` | The passage containing the reference |
| `excerpt_context` | `TEXT` | Slightly wider surrounding context |
| `verified` | `BOOLEAN` | Manually verified as true positive (default: false) |
| `false_positive` | `BOOLEAN` | Flagged as not a genuine biblical citation (default: false) |
| `notes` | `TEXT` | Research notes |
| `created_at` | `TIMESTAMPTZ` | |

Unique constraint: `(opinion_id, search_term_id, excerpt)` — prevents duplicate matches.

#### `ingestion_runs`
Tracks each data collection run for auditability and resumability.

| Column | Type | Notes |
|---|---|---|
| `id` | `SERIAL PK` | |
| `search_term_id` | `INTEGER FK → search_terms` | NULL = multi-term run |
| `court_filter` | `TEXT` | e.g. `ala`, `alacrimapp`, `all` |
| `date_filed_after` | `DATE` | |
| `date_filed_before` | `DATE` | |
| `total_results` | `INTEGER` | Count returned by CourtListener |
| `opinions_ingested` | `INTEGER` | Successfully stored |
| `status` | `TEXT` | `pending`, `running`, `completed`, `failed` |
| `error_message` | `TEXT` | On failure |
| `started_at` | `TIMESTAMPTZ` | |
| `completed_at` | `TIMESTAMPTZ` | |

---

## Phase 2: Search Term Vocabulary

### Initial terms (seeded)

**General**
- `Bible`, `Biblical`, `Scripture`, `Scriptures`, `Holy Scripture`, `Holy Bible`

**Old Testament books**
- `Genesis`, `Exodus`, `Leviticus`, `Numbers`, `Deuteronomy`, `Joshua`, `Judges`, `Ruth`
- `Samuel`, `Kings`, `Chronicles`, `Ezra`, `Nehemiah`, `Esther`, `Job`, `Psalms`, `Proverbs`
- `Ecclesiastes`, `Isaiah`, `Jeremiah`, `Lamentations`, `Ezekiel`, `Daniel`, `Hosea`
- `Joel`, `Amos`, `Obadiah`, `Jonah`, `Micah`, `Nahum`, `Habakkuk`, `Zephaniah`, `Haggai`
- `Zechariah`, `Malachi`

**New Testament books**
- `Matthew`, `Mark`, `Luke`, `John`, `Acts`, `Romans`, `Corinthians`, `Galatians`
- `Ephesians`, `Philippians`, `Colossians`, `Thessalonians`, `Timothy`, `Titus`, `Philemon`
- `Hebrews`, `James`, `Peter`, `Jude`, `Revelation`

**Verse-style references (future expansion)**
- Pattern: `Genesis 1`, `John 3:16`, etc. — to be added iteratively as needed

### Expansion candidates (Phase 4+)
- Named biblical figures: `Moses`, `Solomon`, `Noah`, `David`, `Goliath`, `Cain`, `Abel`
- Common phrases: `eye for an eye`, `Good Samaritan`, `render unto Caesar`, `golden rule`
- Concepts: `Sabbath`, `Ten Commandments`, `Sermon on the Mount`

---

## Phase 3: Data Ingestion Pipeline

- [x] Write `ingest.py` — Python script using CourtListener API
  - Accepts: search term, court filter, date range
  - Fetches paginated results
  - Extracts opinion metadata + full text (HTML fallback chain)
  - Extracts all matching excerpts with ±1000-char context windows
  - Deduplicates against existing records
  - Writes to Supabase via REST API
  - Logs run to `ingestion_runs`
- [x] Test run: Alabama (`ala`) × "Bible" — smoke test passed (3 opinions end-to-end)
- [ ] Validate sample of results manually
- [ ] Expand: all Alabama courts × all active search terms
- [ ] Expand: all 50 states × all active search terms
- [ ] Expand: federal courts × all active search terms

---

## Phase 4: Data Quality & Manual Review

- [ ] Build simple review workflow (mark `verified` / `false_positive`)
- [ ] Audit false positive rate on Alabama sample
- [ ] Assess which book-name terms produce too many false positives (e.g. `Mark`, `Luke`, `John` as names)
- [ ] Add `case_type` coding — manual or rule-based
- [ ] Add `outcome` coding — manual

---

## Phase 5: Analysis (Future)

- [ ] Frequency by court, jurisdiction, time period
- [ ] Frequency by case type
- [ ] Most-cited books
- [ ] Patterns in dissents vs majority opinions
- [ ] Judge-level analysis
- [ ] Time trends (1980–present)

---

## Open Questions

- Which book names create too many false positives? (`Mark`, `Luke`, `John`, `Ruth`, `James` are common names)
- Should `case_type` be coded manually or algorithmically?
- What is the target journal for submission?

---

## Notes

- CourtListener API rate limit: 10 requests per 60 seconds (configured in MCP)
- Alabama test: ~121 results for "Bible" alone across Supreme Court + Criminal Appeals
- `.mcp.json` contains API credentials — gitignored, never commit
- Full text is retrieved during ingestion via HTML fallback chain (`plain_text` → `html_lawbox` → etc.)
- `courts` table seeded with Alabama courts (ala, alacrimapp, alactapp) and federal circuits (scotus, ca1–ca11, cadc)
