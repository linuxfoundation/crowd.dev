# ADR-0005: PyPI downloads via BigQuery bulk export, scoped in the Postgres merge

**Date**: 2026-07-01
**Status**: accepted
**Deciders**: Anil B

_Consolidated ADR for the PyPI downloads worker — record further PyPI-worker download decisions here rather than opening new ADRs._

## Context

We need PyPI download counts to match the npm shape: daily counts for the **Critical slice**
(`downloads_daily`) and rolling 30-day **Window** counts for all tracked pypi packages
(`downloads_last_30d`, mirrored to `packages.downloads_last_30d`). Unlike npm, **PyPI exposes no
per-package downloads HTTP API** — the only source is the public BigQuery dataset
`bigquery-public-data.pypi.file_downloads` (raw per-download events, timestamp-partitioned). The
worker already has proven deps.dev BigQuery→GCS→staging→merge plumbing and a job monitor keyed on
`osspckgs_ingest_jobs`. Cost is driven by bytes scanned, and a single day of the three columns we
read (`file.project`, `timestamp`, `details.installer.name`) measures ~107 GB (weekend) / ~147 GB
(monthly average), so a 30-day window is ~4.56 TB.

## Decision

Ingest PyPI downloads as two new `bq-dataset-ingest` job kinds (`pypi_downloads_30d`,
`pypi_downloads_daily`) that run one BigQuery aggregate over a date range, export **all** projects to
GCS, load to staging, and **scope to the Critical slice in the Postgres merge** (`JOIN packages …
AND is_critical` for daily) — we never push our package list into BigQuery. The 30d workflow does a
**Latest-window refresh** for all pypi (mirroring the latest **Window**); the daily workflow does a
2-day **Trailing re-scan** for the critical subset. Both are idempotent (`ON CONFLICT DO UPDATE`),
fixed-window, and gap-recovered by manual **Backfill** — they are deliberately **not** self-healing.

## Alternatives Considered

### Alternative 1: npm-style per-package HTTP fetch with watermark due-selection
- **Pros**: reuses the npm downloads model exactly; source is scoped to what's due; naturally self-healing.
- **Cons**: requires a per-package downloads API.
- **Why not**: PyPI has no such API. The BigQuery public dataset is the only source, which forces a bulk-aggregate model.

### Alternative 2: Push the critical package list into BigQuery (inline `IN UNNEST([...])`) to shrink the export
- **Pros**: smaller GCS export and staging load, especially for daily backfills.
- **Cons**: inlines our data into the query text.
- **Why not**: the critical set can grow to tens of thousands+; the inline list blows BigQuery's ~1 MB query-text limit (and Temporal's ~2 MB payload limit for the name list). Merge-scoping is unbounded and matches how every deps.dev job scopes to our data in Postgres, not at the source. A cheap `getCriticalPypiCount` guard skips the scan when there are zero critical packages.

### Alternative 3: Gap-filling self-healing (npm's `computeMissingLast30dWindows` model)
- **Pros**: auto-recovers missed days/months without manual intervention.
- **Cons**: needs per-package due-selection / existing-window diffing, extra state and complexity, and re-scans BigQuery anyway.
- **Why not**: for a bulk-BQ source the simpler fixed-window + idempotent-upsert + manual **Backfill** model is sufficient; deps.dev jobs re-scan on re-run too. The daily 2-day **Trailing re-scan** already corrects a partial most-recent partition.

## Consequences

### Positive
- Reuses the deps.dev BQ→GCS→staging→merge plumbing and the `monitor:osspckgs` cost/row dashboard for free.
- Scoping in the merge scales to any critical-set size; our package identifiers never leave Postgres.
- Idempotent upserts make re-runs and overlapping backfills safe (no duplicate rows).

### Negative
- Re-running a date range re-scans BigQuery and re-bills — there is no "already imported" skip.
- The daily 2-day window re-scans each calendar day ~2×; steady-state cost ≈ $610/yr daily + $311/yr 30d ≈ **~$920/yr** at $6.25/TiB (measured).
- Not self-healing: an outage or missed schedule is recovered only by a manual **Backfill**.
- Daily export carries all ~800k projects even though the merge keeps only the critical subset (larger data movement than a source-filtered approach).

### Risks
- **BigQuery cost / runaway scans** — mitigated by per-kind `BQ_DATASET_INGEST_PYPI_DOWNLOADS_*_MAX_BQ_GB` ceilings enforced via a pre-run dry-run (aborts before billing); defaults set from measured sizes (30d = 6000 GB, daily = 2000 GB).
- **Traffic growth** — the ~4.56 TB/30d figure grows with PyPI traffic; ceilings may need raising over time.
