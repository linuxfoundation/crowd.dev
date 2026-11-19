---
type: system
slug: sys-packages
name: OSS packages data
repo: crowd.dev
owner: CDP team
depends_on: [sys-cdp]
used_by: []
sources:
  - services/apps/packages_worker/
  - backend/src/osspckgs/
  - backend/src/db/packagesDb.ts
  - services/libs/data-access-layer/src/osspckgs/
  - backend/src/api/public/v1/index.ts
  - backend/src/api/public/v1/packages/
  - backend/src/api/public/v1/akrites/
  - backend/src/api/public/v1/akrites-external/
  - backend/src/api/public/v1/ossprey/
  - backend/src/api/public/v1/stewardships/
  - services/libs/tinybird/datasources/ossPackages.datasource
---

# OSS packages data

CDP keeps a separate dataset about open source packages: which packages exist in each registry,
their versions, dependencies, downloads and maintainers, the advisories that affect them, the
repository each one comes from, and signals about those repositories (Scorecard, security
contacts, activity). On top of that it runs criticality ranking, stewardship tracking, blast
radius analysis and member contributor reports. The main consumer is Akrites; Insights reads part
of it through Tinybird.

The diagram of how the pieces connect is in [`docs/design/packages.md`](../design/packages.md).
Entry points, task queues, schedules and how to run them are in
[`services/apps/packages_worker/README.md`](../../services/apps/packages_worker/README.md).

## Parts

| Part              | Where                                                 | Role                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| ----------------- | ----------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `packages_worker` | `services/apps/packages_worker/`                      | One package, one image, 15 long-running entry points: the deps.dev and BigQuery ingest, one worker per registry (npm, PyPI, Maven, Cargo, Go, NuGet, RubyGems, Packagist), OSV, Docker Hub, the GitHub repo enricher, security contacts and member contributors, criticality and blast radius                                                                                                                                                                                                                                                                                                                                                        |
| `packages-db`     | schema in `backend/src/osspckgs/migrations/` (Flyway) | Dedicated Postgres instance, separate from the CDP database. About 50 tables, with `versions` and `package_dependencies` hash-partitioned and `downloads_daily` partitioned by date                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| Data access       | `services/libs/data-access-layer/src/osspckgs/`       | Queries the API handlers use: packages, versions, downloads, maintainers, repos, stewardships, advisories, member contributors, identity affiliations                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| Public API        | `backend/src/api/public/v1/`                          | `/akrites` and `/akrites-external`, plus the older `/packages`, `/stewardships` and `/ossprey` routes. Connects through `backend/src/db/packagesDb.ts`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| Tinybird          | `services/libs/tinybird/`                             | `ossPackages`, `packageRepos`, `packageDependencies`, `packageMaintainers`, `advisories`, `advisoryPackages`, `advisoryAffectedRanges` are replicated from `packages-db`. `packageDownloads` is not: it is replicated from the Insights Postgres database, where the Insights package downloads worker writes it. `ossPackages_enriched_ds` is built inside Tinybird by the `ossPackages_enriched` copy pipe (daily, 02:30 UTC), and `ossPackages_enriched_sink` exports its health and lifecycle fields to Kafka (daily, 04:00 UTC) so a Kafka Connect JDBC sink writes them back to `packages` rows, for critical packages (`isCritical = 1`) only |

## What is in packages-db

| Group                 | Main tables                                                                                                                                                       |
| --------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Universe and metadata | `packages` (which also holds the universe signals), `versions`, `package_name_history`, `package_funding_links`                                                   |
| Dependency graph      | `package_dependencies`                                                                                                                                            |
| Usage                 | `downloads_daily` and the per-registry download tables, `repo_docker`, `repo_docker_pulls_daily`                                                                  |
| People                | `maintainers`, `package_maintainers`, `repo_contributors`, `identity_affiliations`                                                                                |
| Security              | `advisories`, `advisory_packages`, `advisory_affected_ranges`, `repo_scorecard_checks`, `security_contacts`, `security_policy_parses`, `repo_reporting_protocols` |
| Repositories          | `repos`, `package_repos` (package→repository links with their confidence), `repo_activity_snapshot`, `repo_well_known_files`                                      |
| Ranking               | `package_criticality_spotlight` and the impact scores written by `rank_packages_chunked()`                                                                        |
| Stewardship           | `stewardships`, `stewards`, `stewardship_assessments`, `stewardship_findings`, `stewardship_remediation_actions`, `stewardship_activity`                          |
| Blast radius          | `blast_radius_analyses`, `blast_radius_dependents`, `blast_radius_verdicts`, `blast_radius_stage_runs`, `blast_radius_symbol_specs`                               |
| Jobs and state        | `osspckgs_ingest_jobs`, `member_contributor_jobs`, and the npm, PyPI and Packagist state tables (other registry workers have none)                                |

## Public API

All routes sit under the CDP public API and use the Auth0 OAuth2 middleware with scopes.

- **`/akrites`** (15 paths): package list, scatter and metrics, package detail, advisories and
  history, batch stewardship, and the stewardship workflow (my packages and activity, open,
  assign, escalate, status). Spec: `v1/akrites/openapi.yaml`.
- **`/akrites-external`** (16 paths): package, advisory and contact detail, each with a batch
  form; project profiling; member contributor jobs; blast radius jobs; and one write path,
  `contacts/ingest`. Spec: `v1/akrites-external/openapi.yaml`.
- **`/packages`, `/stewardships`, `/ossprey`**: the earlier routes. `v1/index.ts` marks them as
  superseded by `/akrites`, to be removed once consumers have moved.

Blast radius and member contributor reports are asynchronous: the API inserts a pending job
row, starts a workflow and the client polls the job. The packages workers run in their own
Temporal namespace, so the API starts these workflows with a separate client
(`backend/src/db/packagesTemporal.ts`, `CROWD_PACKAGES_TEMPORAL_NAMESPACE`).

## Links to the rest of CDP

- Insights reads packages data through Tinybird: the v2 health score pipes use `packageRepos`
  (lifecycle, development), `ossPackages_enriched_ds`
  joined through `packageRepos` (impact), and the
  advisory and dependency datasources (security); `project_insights_impact_breakdown_copy` uses
  `packageRepos` and `ossPackages_enriched_ds`. `packageRepos` is what ties a package to a
  repository an Insights project tracks.
- Member contributor reports work out each contributor's organization from CDP's affiliations
  (read from the CDP database), the email domain and the GitHub company field, keep the best match,
  and cache it per identity in `packages-db` (`identity_affiliations`, a derived cache, not CDP
  data). `osspckgs/akritesMembers.ts` holds the hard-coded list of Akrites
  member organizations, by CDP organization ID, that those reports filter on.

## Decisions

[ADR-0001](../adr/0001-oss-packages-design-decisions.md) is the living record for the packages
domain (database placement, worker architecture, universe selection, criticality scoring, OSV,
per-source ingestion). Narrower decisions:

| Topic                                    | ADR                                                                                                                                                                         |
| ---------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| deps.dev BigQuery tables                 | [0003](../adr/0003-deps-bq-table-selection.md)                                                                                                                              |
| Go and NuGet transitive dependent counts | [0004](../adr/0004-go-nuget-transitive-dependent-counts.md)                                                                                                                 |
| PyPI downloads from BigQuery             | [0005](../adr/0005-pypi-downloads-bigquery-merge-scoping.md)                                                                                                                |
| Packagist worker                         | [0009](../adr/0009-packagist-worker-design-decisions.md)                                                                                                                    |
| Security contacts                        | [0010](../adr/0010-security-contacts-worker.md)                                                                                                                             |
| Akrites authentication (proposed)        | [0016](../adr/0016-akrites-cdp-public-api-authentication.md)                                                                                                                |
| Blast radius pipeline                    | [0017](../adr/0017-blast-radius-pipeline-architecture.md)                                                                                                                   |
| Package→repository confidence            | [0020](../adr/0020-package-repo-confidence-scoring.md), [0021](../adr/0021-secondary-manifest-repository-signal.md), [0022](../adr/0022-package-repo-ownership-evidence.md) |
