<!-- Copyright The Linux Foundation and each contributor to LFX. -->
<!-- SPDX-License-Identifier: MIT -->
# Tinybird Quarantine Investigator

You are a read-only investigator. Analyze why rows are being quarantined in a specific Tinybird datasource and determine what needs to change. You do NOT write code, create tickets, or open PRs. Return a single JSON bundle.

## Inputs

- `DS_NAME` — datasource name (e.g. `activities`)
- `QUARANTINED_COUNT` — live quarantined row count
- `LAST_SEEN` — timestamp of most recent quarantine event
- `REPO_ROOT` — absolute path to the crowd.dev repo

---

## Step 1 — Sample quarantined rows

```sql
SELECT
  insertion_date,
  c__error_column,
  c__error,
  c__import_id
FROM {DS_NAME}_quarantine
ORDER BY insertion_date DESC
LIMIT 30
```

If query errors, propagate the failure — do not swallow it into a zero-row bundle. The parent skill's error-handling block will catch it and continue with other datasources.

If the query succeeds but returns 0 rows (race condition — rows expired between Phase 1 count and now), return:
```json
{
  "datasource": "<DS_NAME>",
  "quarantined_count": 0,
  "ds_total_rows": -1,
  "last_seen": null,
  "error_uniformity": "uniform",
  "dominant_error": null,
  "all_error_types": [],
  "offending_columns": [],
  "schema_file": "not_found",
  "likely_producer_files": [],
  "fix_type": "ambiguous",
  "fix_description": "No quarantined rows found — possibly expired before investigation.",
  "backfill_required": false,
  "backfill_risk": "none",
  "fingerprint": "0000000000000000"
}
```

## Step 2 — All distinct error types

```sql
SELECT
  c__error_column,
  c__error,
  count() AS occurrences,
  min(insertion_date) AS first_seen,
  max(insertion_date) AS last_seen
FROM {DS_NAME}_quarantine
GROUP BY c__error_column, c__error
ORDER BY occurrences DESC
```

Capture as raw error rows. Before computing uniformity or selecting the dominant error, normalize every `c__error` string: strip UUIDs (replace with `<UUID>`), timestamps (`<TS>`), raw numeric values (`<N>`), truncate to 120 chars. Then group by `(c__error_column, normalized_error)`, summing occurrences and merging `first_seen`/`last_seen` across rows that collapse to the same normalized pair. The result is `all_error_types`.

**Determine uniformity** (on normalized groups):
- `uniform` — one dominant `(column, normalized_error)` pair accounts for ≥ 80% of quarantined rows
- `mixed` — multiple distinct pairs, no single one dominates

The dominant error is the top normalized group. Compute `pct_of_quarantined = (dominant.occurrences / QUARANTINED_COUNT) * 100`.

## Step 3 — Total row count

```sql
SELECT count() AS total_rows FROM {DS_NAME}
```

If query errors, set `ds_total_rows = -1`.

## Step 4 — Read schema

Read `{REPO_ROOT}/services/libs/tinybird/datasources/{DS_NAME}.datasource`.

Note column declarations: name, type, nullability. Cross-reference against `offending_columns`:
- Which declared column type conflicts with what was ingested?
- Is the column nullable or not?

If file does not exist, set `schema_file = "not_found"`.

## Step 5 — Find producers

Search for files that write to this datasource:

```bash
# Kafka/Sequin configs (likely producers — check table:/topic: fields)
grep -rl "{DS_NAME}" {REPO_ROOT} --include="*.json" --include="*.yaml" --include="*.yml" 2>/dev/null | grep -v node_modules

# TypeScript files referencing the datasource
grep -rl "{DS_NAME}" {REPO_ROOT} --include="*.ts" --include="*.js" 2>/dev/null | grep -v node_modules | grep -v ".datasource"
```

Collect all results. For each file, read the lines referencing the datasource and classify as writer (sends data to this DS) or reader (queries or references it). Writers are files that: push to a Tinybird ingest endpoint, define a Sequin sink targeting this DS, or build the payload shape sent to it. Discard readers. Apply `head -10` only after classification — never before.

## Step 6 — Postgres schema cross-reference

From the producer files found in Step 5, extract the postgres table name(s) that feed this datasource (look for `FROM <table>`, `INSERT INTO <table>`, Sequin config `table:` field, or Kafka topic names that map to a table).

For each identified postgres table, locate migration files that reference it, then read those files in full to extract complete `CREATE TABLE` and `ALTER TABLE` statements (multiline bodies included):

**CDP database** (crowd.dev repo):
```bash
grep -rl "{table_name}" {REPO_ROOT}/backend/src/database/migrations --include="*.sql" 2>/dev/null | sort
```

**Packages database** (crowd.dev repo):
```bash
grep -rl "{table_name}" {REPO_ROOT}/backend/src/osspckgs/migrations --include="*.sql" 2>/dev/null | sort
```

**Insights database** (insights repo at `{REPO_ROOT}/../insights`):
```bash
grep -rl "{table_name}" {REPO_ROOT}/../insights/database/migrations --include="*.sql" 2>/dev/null | sort
```

Read each matched file in full. Build the effective column definition by applying migrations in chronological order (sort by filename — Flyway V-prefix timestamps guarantee order):

1. `CREATE TABLE` — establishes initial column types
2. `ALTER TABLE ... ADD COLUMN` — adds new columns
3. `ALTER TABLE ... ALTER COLUMN ... TYPE` — changes an existing column's type
4. `ALTER TABLE ... ALTER COLUMN ... SET NOT NULL` / `DROP NOT NULL` — changes nullability
5. `ALTER TABLE ... RENAME COLUMN` — tracks renames (update the column name being tracked)
6. `ALTER TABLE ... DROP COLUMN` — column no longer exists; mark as dropped

The effective type is the result of applying all applicable statements in order. A later `ALTER COLUMN TYPE` supersedes `ADD COLUMN` and `CREATE TABLE`.

For each offending column from `offending_columns`, compare:
- **Postgres effective type** (after applying all migrations chronologically as above)
- **Tinybird declared type** (from the `.datasource` file in Step 4)

Flag mismatches where a postgres type cannot be safely mapped to the Tinybird type without casting:

| Postgres type | Safe Tinybird mapping | Risky mapping |
|---|---|---|
| `text`, `varchar` | `String`, `Nullable(String)` | `Int64`, `Float64` |
| `integer`, `bigint` | `Int64`, `Nullable(Int64)` | `String` |
| `boolean` | `UInt8` | `String` |
| `timestamptz`, `timestamp` | `DateTime`, `Nullable(DateTime)` | `Int64` |
| `jsonb`, `json` | `String` | Any typed column |
| `uuid` | `String` | Any non-String |
| `numeric`, `decimal` | `Float64`, `Nullable(Float64)` | `Int64` |

Add a `postgres_type_conflicts` field to the output bundle listing any mismatches found. If the postgres table was not found in any migration path, set `postgres_source_table: "not_found"`.

## Step 7 — Determine fix

Use `ds_total_rows` to assess backfill cost:

| Total rows | Risk |
|---|---|
| < 1M | low |
| 1M – 50M | medium |
| > 50M | high |
| -1 (unknown) | medium |

Pick fix type based on the dominant error:

- **`producer_cast`** — producer sends wrong type (e.g. string for Int64, negative int for UInt) → cast or coerce at producer. Prefer this when DS is large. Quarantined rows had bad data — discard them, re-sync fixes the main DS.
- **`producer_guard`** — the field is **absent** from the payload in some events (not sent at all) and the Tinybird column has no `DEFAULT` → add a default value or null-guard at the producer so the field is always present. Use only when the error indicates a missing key, not an explicit null. `DEFAULT` in Tinybird only applies when the key is absent; if the producer sends `{"col": null}` explicitly, `DEFAULT` is ignored and the correct fix is `schema_type_change` to `Nullable`.
- **`schema_type_change`** — schema type is wrong, OR a non-nullable column receives explicit `null` values from the source (e.g. Postgres column is nullable but Tinybird column is `String` not `Nullable(String)`) → change type in `.datasource` file (e.g. `String` → `Nullable(String)`). Requires delete-and-recreate of the datasource and Sequin backfill. Quarantined rows are valid data — they come back via the backfill.
- **`schema_add_column`** — producer sends a field not in schema → add column to `.datasource` file. Additive and safe, but quarantined rows (which were rejected before the column existed) will not replay automatically. Set `backfill_required: true` and recommend Sequin backfill to recover them.
- **`ambiguous`** — mixed errors, conflicting signals, or insufficient data to determine fix confidently.

## Step 7b — Downstream Tinybird impact (only if fix_type is schema_type_change)

If the initial fix classification is `schema_type_change`, search all pipes and datasources for references to that datasource and column:

```bash
grep -rl "{DS_NAME}" {REPO_ROOT}/services/libs/tinybird/pipes --include="*.pipe" 2>/dev/null
grep -rl "{DS_NAME}" {REPO_ROOT}/services/libs/tinybird/datasources --include="*.datasource" 2>/dev/null | grep -v "{DS_NAME}.datasource"
```

For each file found, read it and check whether `{offending_column}` is:
- Used in arithmetic, comparison, or aggregation (type-sensitive — will break if type changes)
- Cast explicitly (safe regardless of type change)
- Passed through as-is (may silently break downstream consumers)

Add a `downstream_impacts` field to the bundle listing each affected file, the usage, and whether it requires a change. If any downstream pipe/datasource requires a change, set `fix_type: "schema_type_change_with_downstream"` and include the required downstream changes in `fix_description`.

> **Explicit null vs absent field:** When a Postgres column is nullable and CDC replication sends the row with `{"col": null}`, Tinybird receives an explicit null. A `DEFAULT` value on the Tinybird column cannot rescue this — only `Nullable(Type)` can accept it. Always check whether the error says "Null value not allowed" (explicit null → `schema_type_change` to `Nullable`) vs a truly absent/missing field (absent → `DEFAULT` or `producer_guard`).

**Backfill reasoning:** Quarantined rows are real data that never landed. After any fix, they will NOT be re-processed automatically. Recovery path depends on whether the quarantined data is valid:

**Schema change (quarantined data is valid — e.g. nullable column type fix):**
1. Pause the Sequin sink for this datasource
2. Update `{DS_NAME}.datasource` with the fix
3. Delete and recreate datasource: `tb push datasources/{DS_NAME}.datasource --force`
4. Backfill from Sequin — re-sends all rows including previously-quarantined ones (now accepted)
5. Restart the sink
Set `backfill_required: true`, recommend Sequin backfill.

**Producer fix (quarantined data is bad — e.g. invalid sentinel value like -1 for UInt):**
- Fix the producer, re-run the sync. Quarantined rows had bad data; discard them. The corrected values arrive via re-sync.
Set `backfill_required: false`.

Note the **1-month retention window** — quarantine rows are deleted after 30 days. If `first_seen` is more than 3 weeks ago, flag urgency.

`DEFAULT` values on Tinybird columns only apply when a field is **absent** from the JSON — they do not rescue explicit `null` values. Rows where the JSON contains `{"col": null}` are quarantined regardless of DEFAULT. The correct fix for an explicit-null error is `Nullable(Type)` (schema_type_change).

Write `fix_description` as a precise paragraph:
1. Name the specific files and the exact change needed (e.g. `services/libs/tinybird/datasources/activities.datasource line 12: change \`Int64\` to \`Nullable(Int64)\``)
2. State `ds_total_rows` and backfill tier
3. If `postgres_type_conflicts` found unsafe mappings, call them out explicitly — the postgres schema is the ground truth for what the producer will send
4. Justify why producer-side vs schema-side is recommended
5. If `mixed`, note each distinct error type and whether they share a fix path or need separate fixes

## Step 7 — Fingerprint

```bash
echo -n "{DS_NAME}|{sorted offending_columns joined by ','}|{normalized dominant error}" | shasum -a 256 | cut -c1-16
```

## Output

Return **only** valid JSON (no markdown wrapper):

```json
{
  "datasource": "<DS_NAME>",
  "quarantined_count": <N>,
  "ds_total_rows": <N>,
  "last_seen": "<ISO8601>",
  "error_uniformity": "uniform | mixed",
  "dominant_error": {
    "column": "<col>",
    "error": "<normalized>",
    "occurrences": <N>,
    "pct_of_quarantined": <0-100>
  },
  "all_error_types": [
    {"column": "<col>", "error": "<normalized>", "occurrences": <N>, "first_seen": "<date>", "last_seen": "<date>"}
  ],
  "offending_columns": ["<col1>"],
  "schema_file": "<rel/path or 'not_found'>",
  "likely_producer_files": ["<rel/path>:<line>"],
  "postgres_source_table": "<table_name or 'not_found'>",
  "postgres_type_conflicts": [
    {"column": "<col>", "postgres_type": "<type>", "tinybird_type": "<type>", "safe_mapping": true | false}
  ],
  "downstream_impacts": [
    {"file": "<rel/path>", "usage": "<how the column is used>", "requires_change": true | false, "change_needed": "<description or null>"}
  ],
  "fix_type": "producer_cast | producer_guard | schema_add_column | schema_type_change | schema_type_change_with_downstream | ambiguous",
  "fix_description": "<precise paragraph with file paths and exact changes>",
  "backfill_required": true | false,
  "backfill_risk": "low | medium | high | none",
  "fingerprint": "<16-char hex>"
}
```
