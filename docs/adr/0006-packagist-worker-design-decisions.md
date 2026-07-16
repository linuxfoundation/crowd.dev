# ADR-0006: Packagist worker — design decisions (living)

**Date**: 2026-07-13
**Status**: living
**Deciders**: Anil B

_Living, consolidated record for the Packagist (PHP/Composer) worker. Record further
Packagist-worker decisions here as new `### subsections` under Decisions rather than opening a
new ADR per decision — mirrors ADR-0001 (oss-packages living ADR) and the ADR-0005 consolidation
note. Each entry is stamped with its own `**Decided**` date; the Changelog at the bottom tracks
when entries are added or revised._

## Context

Packagist is the primary registry for PHP/Composer. Unlike npm/maven/pypi/etc., it is **not
covered by deps.dev** — there is no `PACKAGIST` system in `bigquery-public-data.deps_dev_v1` — so we
cannot seed it from BigQuery the way ADR-0001's universe ingest does for other ecosystems. We crawl
Packagist directly instead (list.json seed → dynamic stats endpoint → static p2 metadata endpoint).
A consequence that shapes several decisions: Packagist hands us **per-package Composer manifests**
(the raw `require` / `require-dev` maps a package declared), **not a pre-resolved dependency graph**
like the one deps.dev exposes in BigQuery.

## Scope and current status

| Decision area   | Status  |
| --------------- | ------- |
| Dependency model | decided |
| Metadata enrichment scope | decided |
| Lane architecture & cadence | decided |

---

## Decisions

### Dependency model: direct edges + declared constraints, resolved at query time

We store, in `package_dependencies`, **only the direct dependency edges a version declares** — one
row per `(version_id, depends_on_id, dependency_kind)` — and we **resolve concrete versions and
transitive closure at query time**, never at ingest.

Per stored edge (for the critical slice only — dependency edges ride the same p2 metadata fetch as
versions, which we only run for critical packages):

- `require` → `dependency_kind = 'direct'`, `require-dev` → `dependency_kind = 'dev'`.
- `version_constraint` = the **declared** constraint string verbatim (e.g. `^3.0 || ^2.0`).
- `depends_on_id` = the depended-on **package** row (resolved by name; edges whose target isn't a
  known packages row are counted and skipped, not errored).
- `depends_on_version_id` = **NULL** — we do not resolve the concrete satisfying version at ingest.
- Platform requirements (`php`, `hhvm`, `ext-*`, `lib-*`, `composer-*` — anything with no `/`) are
  excluded; they aren't packages.

**Provenance.** The query-time transitive model is a **stated requirement**, quoted verbatim from
the Packagist change brief:

> We store only direct dependencies (the requirements declared in each version's composer.json) and
> walk the tree at query time when transitive resolution is needed. We do not need to compute or
> store the resolved graph at ingest.

Leaving `depends_on_version_id` NULL is the column-level **implementation** of that last sentence —
the resolved concrete version *is* part of "the resolved graph" — locked at the plan-approval gate
and consistent with the pre-existing schema comment (`depends_on_version_id bigint, -- resolved
version; NULL if unknown`). Storing the declared `version_constraint` but not a pinned version means
the two derived views — *which version a constraint resolves to* and *the full transitive tree* —
are both computed on read.

**Why not pin `depends_on_version_id` at ingest (as deps.dev does):** a resolved version is *derived
and volatile*. `^3.0` resolves to whatever `psr/log` patch is newest this week; a stored pin goes
stale the moment a patch ships and would need continuous rewriting. deps.dev's own worker documents
exactly this churn — re-resolving `^4.17.0` every snapshot is what produced its ~555M-row edge
exports and forced a snapshot-diff to suppress it. We sidestep it entirely by storing the immutable
declared constraint and resolving on demand. deps.dev pins because it *already* has a fully-resolved
graph in BigQuery for free; we only get manifests, so resolving eagerly would mean building and
continuously re-running a Composer resolver for no durable benefit.

**Why store `dev` edges when deps.dev doesn't:** deps.dev's resolved graph is runtime-only — every
edge lands as `'direct'`, it has no dev/peer notion. Because we read the raw manifest we can split
`require-dev` into `'dev'`, so Packagist edges are *richer per-edge* (dev deps + declared
constraints) even though they are *shallower* (no resolved target version) and *narrower* (critical
slice only) than deps.dev's.

**Consequences.**

_Positive:_

- Write path is small and touches only immutable declared facts — no re-resolution churn, no
  billion-row resolved-graph materialization for Packagist.
- Query-time resolution and transitive walks always reflect current data (new patches, newly-added
  deep dependencies) with no re-ingest.
- Captures dev-dependencies (`kind='dev'`), a dimension deps.dev-seeded ecosystems lack.
- Schema-compatible with the deps.dev-populated `package_dependencies`: same table, same unique key
  `(version_id, depends_on_id, dependency_kind)`; the `depends_on_id` HASH-partitioning (ADR-0001)
  keeps the upstream "who depends on X?" hot path fast for Packagist edges too.

_Negative / trade-offs:_

- `depends_on_version_id` is NULL for every Packagist edge — a consumer that wants the exact version
  a dependency pulls in must resolve the constraint itself; there is no per-edge pinned version the
  way there is for npm/maven/pypi/cargo seeded from deps.dev.
- Transitive questions ("everything X depends on") pay a recursive walk over direct edges at read
  time rather than a single indexed lookup.
- Dependency edges exist for the **critical slice only** — non-critical Packagist packages have
  none, because the p2 metadata crawl that produces them is critical-scoped.

_Risks:_

- If an LFX product later needs per-edge **resolved** versions for Packagist, that is a
  **requirements change** — it reopens the "do not store the resolved graph at ingest" decision and
  would require standing up a Composer-style resolver (and accepting the re-resolution churn deps.dev
  fights). Flagged here so the trade-off is revisited deliberately, not silently.

**Decided**: 2026-07-13

---

### Metadata enrichment scope: all packages, not the critical slice

The metadata lane (p2 manifests → `versions`, `package_dependencies` edges, and the package-level
aggregates `licenses` / `latest_version` / `versions_count` / `first_release_at` /
`latest_release_at` / `homepage`) runs for **all ~500K Packagist packages**, not only the critical
slice: `CROWD_PACKAGES_PACKAGIST_RUN_ONLY_FOR_CRITICAL` defaults to `false` (setting it `true`
narrows back to the critical slice). The same lane additionally links `repos` /
`package_repos` rows for all packages (`'declared'` @ `0.8`, the manifest-declared convention
shared with npm/pypi/maven/cargo), so the shared github-repos-enricher picks Packagist repos up.

**Provenance.** Requested in the deps.dev-parity review (Joana): deps.dev has zero Packagist data,
so unlike every other ecosystem there is no external source backfilling the non-critical majority —
the tiering left most of the catalog thin for no data-availability reason. The goal is best-effort
parity with what deps.dev populates for its ecosystems.

**Why this deliberately diverges from pypi:** the pypi sibling documents critical-only as the
"intended steady state" with all-packages as a temporary bootstrap mode. That design assumes
deps.dev provides package-level data for the whole ecosystem, so per-package registry enrichment
only needs to deepen the critical slice. Packagist has no such backstop; the registry crawl *is*
the universe source. The p2 endpoint is a static, CDN-served file designed to be mirrored (it is
how Composer clients resolve), with `If-Modified-Since`/304 replay — so the steady-state weekly
cost after the first full pass is dominated by 304s, not re-parses.

**What stays critical-only (deliberate, not parity gaps):** `downloads_daily` and
maintainers. deps.dev carries no downloads at all, and npm/pypi both scope daily downloads and
maintainer enrichment to the critical slice; ungating maintainers would also cost ~3–4M per-row
DB round-trips per weekly cycle (`upsertPackageMaintainers` is 2M+2 queries per package) — a
batching rewrite is a prerequisite before that scope can ever widen.

**Consequences.**

_Positive:_ versions, direct dependency edges, licenses/latest-version/release-date aggregates,
homepage, and repo links exist for the whole catalog — matching deps.dev's coverage shape; ranking
inputs and downstream consumers see a fully-populated ecosystem rather than a thin one.

_Negative / trade-offs:_ the weekly metadata drain walks ~500K purls (~10k keyset batches, ~500
continueAsNew generations) instead of ~56K; `versions` grows by an estimated 5–10M rows and
`package_dependencies` by ~20–40M edges (both well within the tables' 90M+/1.15B+ design points);
the two packagist workers now default to opposite scopes, mitigated by explicit comments at both
flip points.

_Risks:_ the all-packages due-selection abandons the partial `is_critical` index and rides the
`purl` btree keyset walk — late-cycle batches skip progressively more already-scanned rows; if the
tail slows measurably, revisit with an `EXPLAIN` (flagged in the rollout checklist).

**Decided**: 2026-07-16

---

### Lane architecture: seed → chained enrichment, downloads as dedicated lanes

Four workflows instead of the earlier seed/stats/metadata split (the `ingestPackagistStats`
workflow, whose name and mixed responsibilities were unclear, was removed):

1. **`seedPackagistPackages`** — weekly cron. Discovery only; on completion **chain-starts the
   enrichment drain as a child workflow** (`ParentClosePolicy.ABANDON`) instead of a second cron,
   so "after seed" is an event, not a clock offset (same idiom as `bootstrapOsspckgs`).
2. **`ingestPackagistMetadata`** — the merged enrichment lane: per package it fetches **both**
   registry endpoints — dynamic (package info, counters, repo link, maintainers) and p2 (versions,
   dependencies, aggregates, homepage) — under one `metadata_last_run_at` watermark.
3. **`ingestPackagistDownloads30d`** — monthly cron **on the 1st**: captures the observed rolling
   30d value as the month's `downloads_last_30d` window row (mirrored to the packages column),
   npm's breadth pattern (run-scoped cutoff, per-purl watermark). Running on the boundary replaces
   the earlier "first scan on/after the 1st" heuristic. No history/backfill lane — Packagist keeps
   no historical series, so a missed month is unrecoverable by design.
4. **`ingestPackagistDownloadsDaily`** — daily cron, critical slice only.

Each lane owns its own watermark + run result in `packagist_package_state`
(`metadata_* / downloads_30d_* / daily_downloads_*`; the ambiguous `p2_last_modified` renamed to
`metadata_last_modified`). Trade-offs: the enrichment lane makes two HTTP calls per package
(bounded by the dynamic endpoint's 10-concurrency), the dynamic endpoint is fetched by three lanes
at different cadences (~500K extra fetches/month for the 30d lane), and a hard seed failure skips
that week's enrichment (recoverable via the manual trigger).

**Decided**: 2026-07-16

---

## Changelog

- **2026-07-13** — ADR created. First entry: _Dependency model: direct edges + declared constraints,
  resolved at query time_.
- **2026-07-16** — Added _Metadata enrichment scope: all packages, not the critical slice_ (also
  covers repo linking for all packages and the deliberate critical-only carve-outs).
- **2026-07-16** — Added _Lane architecture: seed → chained enrichment, downloads as dedicated
  lanes_ (removes the stats lane; renames `p2_last_modified` → `metadata_last_modified`).
