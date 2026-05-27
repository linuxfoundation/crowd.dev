# ADR-0001: OSS packages — design decisions (living)

**Date**: 2026-05-27
**Status**: living
**Deciders**: CDP/Insights team

## Context

The oss-packages domain is being built inside CDP as a new, independent capability — tracking open-source packages, dependency graphs, repositories, security advisories, and maintainers. The domain is pre-production: schema, ingestion workers, and the data model are all actively evolving. This document is the single living record of design decisions for oss-packages, updated in place as decisions are refined. At first production release the team will decide whether to seal it as `accepted` or split it into per-area ADRs; that decision is recorded in the Changelog at the bottom.

## Scope and current status

| Decision area | Status |
| --- | --- |
| Database placement | decided |
| Worker architecture | decided |
| Universe source and critical-package selection | decided (formula is a placeholder) |
| Write semantics across sub-workers | decided |
| Package → repository provenance | decided |
| OSV as canonical security source | decided (`has_critical_vulnerability` flag deferred) |
| Per-source ingestion strategies | decided (Sonatype API access pending) |
| deps.dev coverage and gaps | decided |
| Downloads timeline by tier | decided |

---

## Decisions

### Database placement

The packages schema has no FK relationships into either existing CDP database (`crowd-web`, `product-db`), and requires partitioned tables sized for 90M+ versions and 1.15B+ dependency rows — a scale profile that would cause resource contention if mixed with CDP's community-activity tables.

We store all packages-domain data in a dedicated physical Postgres instance (`packages-db`, port 5434) with its own Flyway migration path (`backend/src/osspckgs/migrations/`), following the same Dockerfile and migration-script pattern used by `product-db`. Schema and connection code live entirely within the `packages_worker` service. Read/write host split is prepared in env vars (`CROWD_PACKAGES_DB_READ_HOST` / `CROWD_PACKAGES_DB_WRITE_HOST`) but only the write host is wired in `config.ts` today — read routing is deferred.

**Partitioning rationale:**

| Table | Strategy | Buckets | Hot query shape |
| --- | --- | --- | --- |
| `versions` | HASH(`package_id`) | 32 | Lookup by package — lands in one partition; ~2.8M rows each at 90M total |
| `package_dependencies` | HASH(`depends_on_id`) | 64 | "Who depends on vulnerable package X?" — lands in one partition; ~18M rows each at 1.15B total |
| `downloads_daily` | RANGE(`date`) via `pg_partman` | automatic | Time-series; pruning old partitions is straightforward |

**`package_dependencies` query trade-off**: partitioning on `depends_on_id` makes upstream queries fast — "which packages depend on X?" lands in one partition. The inverse — "what does package Y depend on?" (lookup by `package_id`, not `depends_on_id`) — is a cross-partition scan. The upstream direction is the security-critical hot path (vulnerability blast-radius analysis), so this trade-off is intentional.

**Decided**: 2026-05-25

---

### Worker architecture

All packages_worker sub-workers live in a single npm package (`services/apps/packages_worker`) and are built from one Dockerfile (`scripts/services/docker/Dockerfile.packages-worker`). Each sub-worker is a self-contained directory under `services/apps/packages_worker/src/{worker}/` with its own logic, types, and database access. Each is launched as a separate container using a different entry point command, sharing the same image. Config helpers (`requireEnv`, `requireEnvInt`) and the packages-db connection are shared across all entry points.

```
services/apps/packages_worker/
  src/
    bin/
      packages-worker.ts      ← parent / health-check stub
      github-repos-enricher.ts
    enricher/                 ← github-repos-enricher logic
    npm/                      ← npm worker (future)
    deps-dev/                 ← deps.dev worker (future)
    osv/                      ← OSV worker (future)
    config.ts                 ← shared requireEnv / requireEnvInt
    db.ts                     ← shared packages-db connection
```

Adding a new data source means adding `src/{worker}/` and a new compose service entry — no new npm package, no new Dockerfile.

**Temporal integration**: the service is initialized with `temporal: { enabled: true }` and registers a `packages-worker` task queue. Each ingestion sub-worker defines its workflows under `src/{worker}/workflows.ts`, its activities under `src/{worker}/activities.ts`, and its schedule under `src/{worker}/schedule.ts`. Schedules use `ScheduleOverlapPolicy.SKIP` so a slow run does not queue a second execution. Central re-exports live in `src/workflows/index.ts` and `src/activities.ts`. The `github-repos-enricher` currently runs as a direct polling loop rather than a Temporal workflow — migrating it is deferred until the broader ingestion patterns stabilize.

**Decided**: 2026-05-25

---

### Universe source and critical-package selection

Tier 2 enriches a critical slice of the npm and Maven ecosystems — not the full registry. We need a signal-rich, affordable source to rank the ~4–5M packages and decide which top-N to fully enrich.

We use the [deps.dev BigQuery public datasets](https://deps.dev) — specifically `PackageVersionsLatest`, `DependentsLatest`, `PackageVersionToProjectLatest`, and `ProjectsLatest` — filtered to `System IN ('NPM', 'MAVEN')` as the universe input. The BigQuery data is exported to Parquet files and imported into `packages_universe` on a weekly cadence. A scoring + ranking job then promotes the top-N per ecosystem by setting `is_critical = true` and copying `criticality_score` onto the full `packages` table.

The current scoring formula and per-ecosystem critical-package quotas are **not yet finalized** — both are still under discussion. The ranking function takes `critical_top_n_by_ecosystem` as a JSONB parameter and weights as numeric inputs, so thresholds and formula coefficients can be tuned at call time without a schema change.

The BigQuery free tier is approximately 1 TiB/month. Column projection and `System` filtering are mandatory on every query; full-table scans will exhaust the quota.

**Decided**: 2026-05-26

---

### Write semantics across sub-workers

Five sub-workers run concurrently (npm, Maven, OSV, GitHub, Docker Hub), all writing to the same `packages-db` schema. We define per-table write rules that allow concurrent writes without distributed locking:

| Table | Rule |
| --- | --- |
| `packages` | Upsert on `purl`. Each worker only writes columns it owns; ecosystem isolation means column-level conflicts cannot occur in practice. |
| `packages_universe` | Full truncate-and-replace on the weekly rank job. No other worker writes to this table. The truncate + bulk-insert must be wrapped in a single transaction or shadow-table swap to avoid a window of emptiness visible to the promotion query. |
| `versions` | Append-only via `INSERT … ON CONFLICT DO NOTHING`. Yanked/deprecated status is a separate targeted `UPDATE (is_yanked = true) WHERE …`. |
| `repos` | Registry workers (npm, Maven) do **not** write directly to `repos`. They write `package_repos` rows. The GitHub enricher — triggered when `repos.last_synced_at IS NULL` — upserts `repos` with metadata. Docker Hub worker adds `docker_*` columns on top. |
| `package_repos` | Composite PK `(package_id, repo_url)`. Each `source` value ('declared', 'deps_dev', 'heuristic', 'manual') is a separate row — sources do not overwrite each other. |
| `advisories` | Upsert on `osv_id`. OSV is the single source of truth; no other worker writes to this table. |
| `maintainers` / `package_maintainers` | Upsert on `(ecosystem, username)`. Never delete — history is preserved. |
| `downloads_daily` | Append-only time-series. Each `(package_id, date)` row is written once. npm and Maven workers own disjoint rows by ecosystem. Historical timelines are preserved — workers do not overwrite past dates. |
| `downloads_last_30d` | Upsert on `(purl, end_date)`. Written by the weekly ranking worker only. The cached `packages_universe.downloads_last_30d` column must be updated in the same pass. |

The column-ownership rule is a social contract, not enforced by Postgres. Code review must catch cross-ecosystem or cross-source column writes using this table as the reference.

**Decided**: 2026-05-26

---

### Package → repository provenance

A package's source repository is central to Tier 2 analytics (Scorecard, stars, commit activity). Maintainers sometimes publish incorrect URLs, monorepos host many packages, and multiple sources propose URLs at different reliability levels.

The `package_repos` table is one-to-many: one package may map to multiple candidate repo URLs; one repo may host many packages. Each row carries:

- `source`: `'declared'` (from package manifest) | `'deps_dev'` | `'heuristic'` (name-based guess) | `'manual'` (human curation)
- `confidence`: 0.00–1.00 — declared ≈ 0.80, deps_dev ≈ 0.90, heuristic ≈ 0.50, manual = 1.00

All repo URLs are **canonicalized** before insertion: scheme normalized to `https`, host lowercased, trailing `.git` stripped, trailing slash stripped, and path case lowercased for GitHub/GitLab. The canonical URL is the `repos.url` primary key and FK target in `package_repos`. Canonicalization must be a shared utility (not reimplemented per worker) with unit tests covering edge cases before the first worker ships.

The `packages` table retains `declared_repository_url` (raw) and `repository_url` (canonical highest-confidence match) as denormalized copies for quick access.

**Population order**:
1. Registry workers (npm, Maven) write `packages` and `package_repos` rows.
2. The GitHub enricher polls `repos` for rows where `last_synced_at IS NULL` (never enriched) or `last_synced_at < NOW() - INTERVAL '<configurable hours>'` (stale). The re-sync interval is controlled via `ENRICHER_REPO_UPDATE_INTERVAL_HOURS`.
3. The enricher updates those rows with full metadata and sets `last_synced_at`.
4. Subsequent enricher runs pick up new repos added since the last pass.

`repos` rows are current-state only — no historical snapshots; each enricher run overwrites previous metadata values.

**Decided**: 2026-05-26

---

### OSV as canonical security source

Tier 2 exposes a `has_critical_advisory` flag per package. We need a security advisory source covering both npm and Maven, available in bulk, with structured version ranges. The repo has prior art on OSV parsing in `services/apps/git_integration/src/crowdgit/services/vulnerability_scanner/`.

We ingest the OSV bulk ZIP — full download daily, not incremental — into `advisories` and `advisory_affected_ranges`, with no JSONB. Each advisory row captures `osv_id`, `aliases`, `ecosystem`, `package_name`, `severity`, `cvss`, `summary`, `details`, `published_at`, `modified_at`. `is_critical` is a `GENERATED ALWAYS AS (cvss >= 7.0) STORED` column.

**v1 ecosystem allowlist**: only `npm` and `maven` advisories are processed; all others are skipped at parse time.

**Multi-package advisories**: `advisories.package_name` holds the first affected package (primary lookup for single-package advisories, the majority). The full affected list is in `advisory_affected_ranges`, one row per `(osv_id, ecosystem, package_name)`.

**Severity fallback** (many OSV records carry no CVSS vector):

| Qualitative severity | Synthesized cvss |
| --- | --- |
| `CRITICAL` | 9.5 |
| `HIGH` | 7.5 |
| `MEDIUM` | 5.0 |
| `LOW` | 3.0 |

The qualitative `severity` tag is stored alongside the synthesized value so consumers can distinguish real CVSS from approximations.

**Ecosystem normalization**:

| OSV raw value | Stored as |
| --- | --- |
| `npm` | `npm` |
| `Maven` | `maven` |

**Derivation cadence**: `deriveCriticalFlag` runs at the end of each OSV sync pass in the same worker loop — no separate scheduler needed.

**`has_critical_vulnerability` flag**: deferred — semantics not yet decided (see Open Questions). Callers join `advisory_packages` directly in the interim.

The ingest worker must stream-parse the bulk ZIP rather than loading it into memory.

**Decided**: 2026-05-26

---

### Per-source ingestion strategies

#### npm

**Strategy**: daily delta poll via the CouchDB changes feed, not a full sync.

1. Call `replicate.npmjs.com/_changes?since=<last_seq>&limit=<batch>` to get package names changed in the last 24h.
2. For each changed name, fetch the full document from `registry.npmjs.com/<package>`.
3. Normalize into `packages`, `versions`, `maintainers`, and `package_maintainers` using the write rules above.
4. Downloads (timeline): call `api.npmjs.org/downloads/range/<start>:<end>/<package>` in batches of **128 packages per call** against the critical list. Write per-day rows to `downloads_daily`. Update `packages_universe.downloads_last_30d` at the end of each pass.

The `_changes` `seq` cursor is persisted between runs (in a `worker_state` row or env var) so each poll starts where the last one ended. If the cursor is lost the worker falls back to a full re-sync of the critical list.

#### Maven

**Strategy**: deps.dev artifact list as the universe source; TypeScript POM fetcher for critical packages.

1. Use the deps.dev BigQuery/Parquet export as the source of truth for the Maven universe. Write rows to `packages_universe`.
2. For artifacts flagged `is_critical = true`, fetch the POM from `repo1.maven.org/maven2/<groupId-path>/<artifactId>/<version>/<artifactId>-<version>.pom`. Extract `<developers>`, `<contributors>`, license, description, and SCM URL.
3. Sonatype Central Stats for groupId-level monthly downloads (pending API access — see Open Questions).

Expected maintainer coverage: 30–50% of Maven POMs have meaningful `<developers>` blocks. The gap is expected; week-3+ work cross-references GitHub collaborators via `package_repos` provenance.

A standalone Java app parsing the Maven Central Lucene index is being built in parallel as a diagnostic tool to cross-check deps.dev coverage. It is not the primary ingestion path.

#### GitHub

**Strategy**: token-pooled GraphQL via the existing GitHub App, batches of 100 repos.

The existing `github-repos-enricher` worker (`services/apps/packages_worker/src/enricher/`) is the template. Token sourcing uses the GitHub App already integrated through Nango — hundreds of installation tokens are available there, so no PATs are needed. Key parameters:

- **Nango GitHub App tokens** in rotation for rate-limit headroom (not PATs — see open questions).
- GraphQL `repository(owner, name)` query per repo — one call per repo, not batched.
- Writes `primary_language`, `topics`, `watchers`, `last_commit_at`, `archived`, `disabled`, `is_fork`, `created_at` to `repos`. Does not provide Scorecard — that comes from deps.dev `ProjectsLatest`.

#### Docker Hub

**Strategy**: opportunistic enrichment, coverage ~5–10%.

For any `repos` row where GitHub metadata or Dockerfile content declares a known Docker image name, call `hub.docker.com/v2/repositories/<image>` and write `docker_image_name`, `docker_pulls`, and `docker_stars` to the `repos` row.

Docker Hub runs after registry workers are stable. Docker dependents (which images use a given image as a base layer) are **explicitly out of scope for v1**.

**Decided**: 2026-05-26

---

### deps.dev coverage and gaps

deps.dev is the primary source for package identity, dependents, advisories, and `ProjectsLatest`-derived repo metadata. The table below is the canonical reference for which sub-worker owns which column. Nullable columns in the schema reflect these boundaries.

#### packages

| Column | From deps.dev? | Source if not |
| --- | --- | --- |
| `purl`, `ecosystem`, `namespace`, `name` | yes | — |
| `description`, `licenses`, `latest_version` | yes | — |
| `declared_repository_url`, `homepage` | yes | — |
| `first_release_at`, `latest_release_at`, `versions_count` | yes | — |
| `dependent_packages_count` | yes (via `DependentsLatest`) | — |
| `registry_url` | no | npm registry / crates.io / PyPI |
| `status` | no | npm registry (deprecated/unpublished flag; no deps.dev equivalent) |
| `licenses_raw` | no | npm registry / crates.io (raw SPDX before normalization) |
| `keywords` | no | npm registry / PyPI / crates.io |
| `dist_tags_latest`, `_next`, `_beta` | no | npm registry only |
| `dependent_repos_count` | no | derived in Postgres: `COUNT(DISTINCT repo_id)` via `package_repos` |
| `criticality_score`, `is_critical` | no | internal — `rank_packages_universe()` |

#### versions

| Column | From deps.dev? | Source if not |
| --- | --- | --- |
| `number`, `published_at` | yes | — |
| `is_prerelease` | yes (derived: `NOT VersionInfo.IsRelease`) | — |
| `license` | yes (per-version, `PackageVersionsLatest`) | — |
| `is_latest` | no | derived: `number = packages.latest_version` |
| `is_yanked` | no | npm registry (deprecated flag per version); crates.io API |

#### repos

| Column | From deps.dev? | Source if not |
| --- | --- | --- |
| `url`, `host`, `owner`, `name` | yes (`ProjectsLatest`) | — |
| `description`, `homepage`, `stars`, `forks`, `open_issues` | yes | — |
| `raw_project_type`, `raw_project_name` | yes | — |
| `primary_language`, `topics`, `watchers` | no | GitHub API (`github-repos-enricher`) |
| `last_commit_at`, `archived`, `disabled`, `is_fork`, `created_at` | no | GitHub API |
| `scorecard_score`, `scorecard_last_run_at` | no | `bigquery-public-data.openssf.scorecardcron_v2_latest` |

#### advisories

| Column | From deps.dev? | Source if not |
| --- | --- | --- |
| `osv_id`, `source`, `source_url`, `summary`, `details` | yes (`AdvisoriesLatest`) | — |
| `cvss`, `severity`, `aliases`, `published_at` | yes | — |
| `modified_at` | no | tracked in-house on each re-sync |

#### advisory_affected_ranges

| Column | From deps.dev? | Source if not |
| --- | --- | --- |
| `range_raw`, `unaffected_raw` | yes | — |
| `introduced_version`, `fixed_version`, `last_affected` | no | future range-parsing workstream (parses `range_raw`) |

#### Tables with zero deps.dev coverage

| Table | Owning source |
| --- | --- |
| `repo_scorecard_checks` | `bigquery-public-data.openssf.scorecardcron_v2_latest` |
| `repo_docker` | Docker Hub API / GHCR |
| `maintainers`, `package_maintainers` | npm registry, crates.io, PyPI |
| `package_funding_links` | npm registry (`funding` field) |
| `package_name_history` | internal CDP tracking only |
| `downloads_daily`, `downloads_last_30d` | registry APIs / BigQuery — see Downloads section |

Any new column added to the schema or new deps.dev table exposed requires an amendment to this section. If a sub-worker discovers coverage drift (deps.dev adds or removes fields), the change in ownership must be made explicit here — do not silently fill what was previously listed as a gap.

**Decided**: 2026-05-27

---

### Downloads timeline by tier

Downloads is the strongest criticality signal, but the ~4–5M packages in `packages_universe` (Tier 3) and the ~600k critical packages in `packages` (Tier 2) have very different cost profiles for historical download data.

Tier 2 (`packages`) downloads are stored in `downloads_daily` — one row per `(package_id, date)`, consumers sum over any window they need. Tier 3 (`packages_universe`) downloads are stored in `downloads_last_30d` — one row per `(purl, end_date)` capturing a rolling 30-day window — with the latest window's count also cached on `packages_universe.downloads_last_30d bigint` for direct use by `rank_packages_universe()` without a join.

`downloads_last_30d` is keyed by `purl` (not `packages_universe.id`) so rows survive the weekly truncation of `packages_universe`.

| Table | Tier | Grain | Unique key | PK | Partitioning |
| --- | --- | --- | --- | --- | --- |
| `downloads_daily` | 2 (`packages`) | one row per package per day | `(package_id, date)` | `(id, date)` | RANGE on `date` via pg_partman |
| `downloads_last_30d` | 3 (`packages_universe`) | one row per purl per rolling 30-day window | `(purl, end_date)` | `(id, end_date)` | RANGE on `end_date` |

Upsert pattern for `downloads_last_30d`:

```sql
INSERT INTO downloads_last_30d (purl, start_date, end_date, count)
VALUES (...)
ON CONFLICT (purl, end_date) DO UPDATE
SET count      = EXCLUDED.count,
    start_date = EXCLUDED.start_date;
```

The weekly ranking worker must write both the `downloads_last_30d` row and the cached `packages_universe.downloads_last_30d` column in the same pass — if only one write happens, the ranking function silently uses a stale value. Code review must enforce this.

A package promoted from Tier 3 to Tier 2 (becomes critical) will have rolling-window history but no daily history; the daily timeline starts from the promotion date forward.

**Decided**: 2026-05-27

---

## Open questions / in-flight

- **Sonatype Central Stats API access** — not confirmed as of 2026-05-27. If unavailable by day 5, Maven download counts will be absent from the week-2 demo (`downloads_last_month` NULL for Maven rows; disclose to stakeholders).
- **`has_critical_vulnerability` flag** — currently deferred; the column is commented out in the schema. The open question is what it should mean: (a) any critical advisory exists for this package name regardless of version, or (b) the package's latest version actually falls inside an active affected range. Option (b) requires evaluating OSV's `introduced` / `fixed` / `last_affected` range fields using semver (npm) or Maven's own version comparator — non-trivial work. Until resolved, callers join `advisory_packages` directly instead of reading a flag.
- **criticality_score formula** — the placeholder formula (`X * downloadsCount + Y * dependentCount`) has not been validated against known critical packages. Final formula is yet to be defined.
- **pg_partman + pg_cron setup** — must be confirmed active in the OCI environment before download workers start; `downloads_daily` and `downloads_last_30_days` inserts will fail if monthly partitions are not pre-created.

---

## Changelog

- 2026-05-27 — initial record

---

## Note on promotion to production

At first production release of oss-packages, the team decides whether to:
- seal ADR-0001 as `accepted` (frozen) and write new ADRs for any post-release changes, or
- split it into per-area ADRs for clearer long-term ownership.

The decision is recorded in the Changelog above.
