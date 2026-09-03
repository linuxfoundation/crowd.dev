<!-- Copyright The Linux Foundation and each contributor to LFX. -->
<!-- SPDX-License-Identifier: MIT -->
---
name: tb-quarantine-triage
description: >
  On-demand Tinybird quarantine investigator. Detects datasources with live
  quarantined rows, runs per-datasource root-cause analysis, presents a
  diagnosis plan for human review, then creates CM Jira tickets and git
  worktrees for approved datasources. No automation — always requires human
  sign-off before any Jira or Git action.
allowed-tools: Bash, Read, Glob, Grep, Agent, AskUserQuestion, Skill, mcp__tinybird__list_datasources, mcp__tinybird__execute_query, mcp__plugin_context-mode_context-mode__ctx_execute, mcp__mcp-atlassian__createJiraIssue
---

# Tinybird Quarantine Investigator

You are an on-demand quarantine analysis tool. You never auto-create tickets or branches without explicit human approval. You never read or log environment variables containing TOKEN, SECRET, or KEY.

The repo root is the current working directory (`crowd.dev`).

---

## Phase 1 — Detect

Detection is two-pass. Do not include a `FORMAT` clause in any Tinybird query.

> **Important:** Quarantine tables (e.g. `activities_quarantine`) are Tinybird system tables. They never appear in `list_datasources` output. You must build the candidate list from the regular datasource names and then probe each quarantine table directly via SQL.

### Pass A — Candidate list from datasource listing

Call the `list_datasources` MCP tool. The result may be large and saved to a file path rather than returned inline. If a file path is returned, use `ctx_execute` (language: `javascript`) to read and parse the file — never load it raw into conversation. The response is a **JSON array** of objects with a `name` field (not a `{datasources: [...]}` wrapper).

Extract all datasource `name` values. Exclude any where the name:
- starts with `raul_` or `test_`
- ends with `_old` or contains `_backup`
- ends with `_MV` or `_MV_ds` or `_copy_ds`, or matches `*_MV_ds_\d+` (materialized views, numbered MV shards, and copy targets — they do not receive direct ingestion)
- ends with `_sorted` or `_sorted_alt`
- matches `*_bucket_*_ds` or `*_deduplicated_cleaned_bucket_*_ds` or `*_enriched_deduplicated_bucket_*_ds` or `*_collection_bucket_*_ds` or `*_collection_deduplicated_cleaned_bucket_*_ds` (sharded bucket datasources)

These are the candidate base DS names for Pass B.

### Pass B — Live count per quarantine table

For each candidate from Pass A, query (in parallel where possible):

```sql
SELECT count() AS live_quarantined, max(insertion_date) AS last_seen
FROM {DS_NAME}_quarantine
```

Skip datasources where the query errors (many will — quarantine tables that have never received rows may still error). Keep only those with `live_quarantined > 0`.

If no datasources remain after filtering, report:
```
No live quarantined rows found. Nothing to investigate.
```
And stop.

---

## Phase 2 — Diagnose (parallel subagents)

For each affected datasource, spawn one investigator subagent **in parallel**. Read the prompt from `.claude/skills/tb-quarantine-triage/prompts/investigator.md` and pass:

- `DS_NAME` — datasource name
- `QUARANTINED_COUNT` — count from Pass B
- `LAST_SEEN` — timestamp from Pass B
- `REPO_ROOT` — absolute path to the repo (resolve via `pwd`)

Each subagent returns a **diagnosis bundle** (JSON):

```json
{
  "datasource": "<name>",
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
    {"column": "<col>", "postgres_type": "<type>", "tinybird_type": "<type>", "safe_mapping": true}
  ],
  "downstream_impacts": [
    {"file": "<rel/path>", "usage": "<how the column is used>", "requires_change": true, "change_needed": "<description or null>"}
  ],
  "fix_type": "producer_cast | producer_guard | schema_add_column | schema_type_change | schema_type_change_with_downstream | ambiguous",
  "fix_description": "<what specifically needs to change, including file paths and the exact change>",
  "backfill_required": true | false,
  "backfill_risk": "low | medium | high | none",
  "fingerprint": "<sha256(ds|sorted_cols|dominant_error_sig)[:16]>"
}
```

---

## Phase 3 — Present diagnosis plan

After all subagents complete, present a structured report:

### Summary table

```
| Datasource | Quarantined | Uniformity | Fix type | Backfill? |
|------------|-------------|------------|----------|-----------|
| activities |     47      |  uniform   | producer_cast | no  |
| members    |      3      |  mixed     | ambiguous | —        |
```

### Per-datasource detail

For each datasource, print a section:

```
## <datasource_name>

**Quarantined rows:** <N> of <ds_total_rows> total (<pct>%)
**Last seen:** <last_seen>
**Error uniformity:** uniform | mixed

### Errors
| Column | Error | Occurrences | First seen | Last seen |
|--------|-------|-------------|------------|-----------|
| ...    | ...   | ...         | ...        | ...       |

### Root cause
<fix_description — full explanation including file paths and exact changes needed>

### Schema file
<schema_file>

### Producer files
<likely_producer_files — one per line>

### Backfill
<backfill_required> — risk: <backfill_risk>

### Recovery

Recovery path depends on fix type and whether the quarantined data itself is valid:

**Schema type change (`schema_type_change` / `schema_type_change_with_downstream`) — quarantined data is valid:**
1. Pause the Sequin sink for this datasource
2. Update `{DS_NAME}.datasource` with the type fix (and any downstream pipes if `schema_type_change_with_downstream`)
3. Delete and recreate the datasource: `tb push datasources/{DS_NAME}.datasource --force`
4. Backfill from Sequin — re-sends all rows including previously-quarantined ones (now accepted by the fixed schema)
5. Restart the sink

**Add column (`schema_add_column`) — quarantined data is valid:**
1. Add the missing column to `{DS_NAME}.datasource`
2. Push the updated schema: `tb push datasources/{DS_NAME}.datasource` (no delete/recreate needed — additive)
3. Backfill from Sequin — re-sends previously-quarantined rows so the new column is populated

**Producer fix (`producer_cast` / `producer_guard`) — quarantined data is bad:**
1. Fix the producer code (cast or null-guard the offending value)
2. Re-run the sync / worker that feeds this datasource
3. Corrected rows land in `{DS_NAME}` directly; the quarantined rows (which had the bad value) are discarded
4. No explicit backfill needed — the records already exist in the main datasource with their pre-fix values; the re-sync updates them

> ⚠ Quarantine retention is **1 month**. Rows older than 30 days are deleted automatically. Check `first_seen` — if > 3 weeks ago, flag as urgent.
```

If `error_uniformity` is `"mixed"`, add a warning:
> ⚠ Multiple distinct error types. The fix may not resolve all quarantined rows. Review each error type before proceeding.

---

## Phase 4 — Ticket approval gate

Ask the user which datasources to file tickets for:

Use `AskUserQuestion` with one multi-select question listing each affected datasource as an option. Include `"None — diagnose only"` as an option.

Do not proceed to Phase 5 until the user responds.

---

## Phase 5 — Create Jira tickets

For each datasource approved by the user, create one Jira ticket.

Use the Atlassian MCP `createJiraIssue`:

- **project**: `IN`
- **issuetype**: Bug
- **summary**: `Tinybird quarantine: <datasource_name> — <dominant_error.column>: <dominant_error.error[:60]>`
- **description**: structured Jira description (see format below)
- **labels**: `["tinybird", "quarantine", "tb-quarantine-fp-<fingerprint>"]`
- **priority**: High

**Jira description format:**
```
h2. Problem

Rows are being quarantined in the *<datasource_name>* datasource.

*Quarantined rows:* <N> of <ds_total_rows> total
*Last seen:* <last_seen>
*Error uniformity:* <uniform | mixed>

h2. Error breakdown

|| Column || Error || Occurrences || First seen || Last seen ||
| <col> | <error> | <N> | <date> | <date> |

h2. Root cause

<fix_description>

h2. Affected files

*Schema:* <schema_file>
*Producers:*
<likely_producer_files — one per line>
*Downstream pipes/datasources requiring changes:*
<downstream_impacts where requires_change=true — one per line with change_needed description, or "None">

h2. Backfill

Backfill required: <yes | no> — Risk: <backfill_risk>
```

Capture the returned Jira key (e.g. `IN-1234`). After all tickets are created, print a summary:

```
## Tickets created

| Datasource | Ticket | Link |
|------------|--------|------|
| <name>     | <KEY>  | https://crowddev.atlassian.net/browse/<KEY> |
```

---

## Phase 6 — Worktree approval gate

Ask the user which tickets to start working on now:

Use `AskUserQuestion` with one multi-select question listing each created ticket as an option (`<KEY> — <datasource_name>`). Include `"None — tickets only"` as an option.

Do not proceed to Phase 7 until the user responds.

---

## Phase 7 — Create git worktrees

For each ticket approved by the user, create a worktree.

Use the `superpowers:using-git-worktrees` skill:

- **branch**: `fix/<JIRA_KEY>-tb-quarantine-<datasource_name>`
- If the skill is not available, fall back to:
  ```bash
  # Ensure .worktrees is gitignored (add once if missing)
  grep -qxF '.worktrees' .gitignore || echo '.worktrees' >> .gitignore
  git worktree add .worktrees/<JIRA_KEY> -b fix/<JIRA_KEY>-tb-quarantine-<datasource_name> origin/main
  ```

After all worktrees are created, print a summary per datasource:

```
## <datasource_name> → <JIRA_KEY>

Ticket: <JIRA_KEY> (https://crowddev.atlassian.net/browse/<JIRA_KEY>)
Worktree: .worktrees/<JIRA_KEY>
Branch: fix/<JIRA_KEY>-tb-quarantine-<datasource_name>

Next steps:
<fix_description — verbatim from the diagnosis bundle>
```

---

## Error handling

- If a subagent fails for one datasource, report the failure inline and continue with the rest.
- If Jira creation fails, report the error and exclude that datasource from the Phase 6 gate.
- If worktree creation fails (branch already exists, dirty state, etc.), report the error — do not force-push or reset.
