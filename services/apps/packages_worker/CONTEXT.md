# OSS Packages

Tracks open-source packages across ecosystems (npm, Maven). All packages live in `packages`; criticality scoring ranks them in place and marks the top-N per ecosystem as `is_critical = true`.

## Language

### Tiers

**Critical slice**:
The top-N packages per ecosystem selected by criticality score (~700k rows where `is_critical = true`). Fully enriched with versions, maintainers, daily downloads, advisories.
_Avoid_: tracked packages, watched packages

**Watch list**:
A static hardcoded list of package names used as a stand-in for the critical slice until the deps.dev BQ import populates them. Temporary.
_Avoid_: tracked list

**Critical**:
A package whose impact score puts it in the top-N for its ecosystem. `is_critical = true` on `packages`.
_Avoid_: important, popular

### Identity

**purl**:
Package URL (`pkg:npm/react`, `pkg:maven/org.apache/commons`). The canonical cross-ecosystem identifier. Primary stable key across all packages-domain tables.
_Avoid_: package id (that's the `packages.id` bigserial)

**Ecosystem**:
A package registry namespace — `npm`, `maven`. Lowercase.
_Avoid_: system (deps.dev's term), registry

**Packument**:
npm's term for the full JSON document returned by `registry.npmjs.org/<name>` — all versions, maintainers, dist-tags. npm-specific.

### Downloads

**Window**:
One rolling 30-day span in `downloads_last_30d`, identified by its `end_date` (always the 1st of a calendar month). `start_date = end_date − 30 days`. Fixed-width so counts are comparable across months.
_Avoid_: month, period, snapshot

**Self-healing**:
A workflow that recomputes the full set of expected rows on every run, diffs against what's in the DB, and fills only the gaps. No assumption of continuity between runs.
_Avoid_: backfill (that's the one-time historical fill; self-healing is the ongoing property)

## Relationships

- All packages live in `packages`; `rank_packages()` sets `is_critical = true` on the top-N per ecosystem to define the **Critical slice**.
- A package not in the **Critical slice** has **Window** rows in `downloads_last_30d` but no rows in `downloads_daily`.
- A package promoted into the **Critical slice** starts accumulating `downloads_daily` rows from the promotion date forward; its **Window** history already exists.
- Every **Window** is keyed by **purl** — a stable identifier that does not change when packages are re-ranked.
- The **Watch list** stands in for the **Critical slice** until the deps.dev import is operational.
