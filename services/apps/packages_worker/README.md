# packages_worker

Collects open source package data (registries, deps.dev, OSV, OpenSSF Scorecard, GitHub) into
`packages-db` and runs the pipelines built on it: package→repository mapping, security contacts,
criticality, stewardship, blast radius and member contributors. Everything in this folder is one
npm package and one Docker image (`scripts/services/docker/Dockerfile.packages-worker`); each
entry point under `src/bin/` runs as its own process.

Where this fits in CDP, and what reads the data, is in
[`docs/knowledge/sys-packages.md`](../../../docs/knowledge/sys-packages.md) and
[`docs/design/packages.md`](../../../docs/design/packages.md). Why it is built this way is in
[ADR-0001](../../../docs/adr/0001-oss-packages-design-decisions.md) and the other packages ADRs
listed there. Terms are defined in [`CONTEXT.md`](CONTEXT.md).

## Entry points

Each script below is `pnpm start:<name>` (production) or `pnpm dev:<name>:local` (local, loads
`backend/.env.*.local`). A Temporal entry point registers its schedules on start and then serves
its task queue. These workers use their own Temporal namespace, separate from the rest of CDP;
the public API reaches it through `backend/src/db/packagesTemporal.ts`.

| Entry point                | Task queue                 | Code                                         | Schedules it registers (UTC)                                                                                                                                                                                                 |
| -------------------------- | -------------------------- | -------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bq-dataset-ingest`        | `bq-dataset-ingest`        | `deps-dev/`, `scorecard/`, `package-repos/`  | `osspckgs-bootstrap-weekly` Mon 02:00, `pypi-downloads-daily` 06:00, `pypi-downloads-30d-monthly` 4th 06:00, `package-repo-confidence-sweep-daily` 05:00                                                                     |
| `npm-worker`               | `npm-worker`               | `npm/`                                       | `npm-registry-ingest` 03:15, `npm-daily-downloads-backfill` 03:30, `npm-last-30d-history-backfill` 04:30, `npm-last-30d-latest-refresh` 1st 04:00                                                                            |
| `pypi-worker`              | `pypi-worker`              | `pypi/`                                      | `pypi-registry-ingest` 05:00                                                                                                                                                                                                 |
| `maven-worker`             | `packages-worker`          | `maven/`                                     | `maven-ingestion` 09:00                                                                                                                                                                                                      |
| `cargo-worker`             | `cargo-worker`             | `cargo/`                                     | `cargo-registry-sync` 06:00                                                                                                                                                                                                  |
| `go-worker`                | `go-worker`                | `go/`                                        | `go-version-enrich` 02:00, `go-status-enrich` 05:00                                                                                                                                                                          |
| `nuget-worker`             | `nuget-worker`             | `nuget/`                                     | `nuget-registry-ingest` 07:00                                                                                                                                                                                                |
| `rubygems-worker`          | `rubygems-worker`          | `rubygems/`                                  | `rubygems-registry-ingest` 07:00, `rubygems-critical-ingest` 09:00                                                                                                                                                           |
| `packagist-worker`         | `packagist-worker`         | `packagist/`                                 | `packagist-seed`, `packagist-downloads-30d`, `packagist-downloads-daily`, `packagist-transitive-backstop` (crons in `packagist/`)                                                                                            |
| `osv-worker`               | `osv-worker`               | `osv/`                                       | `osv-advisories-sync` 03:30                                                                                                                                                                                                  |
| `security-contacts-worker` | `security-contacts-worker` | `security-contacts/`, `member-contributors/` | `security-contacts-ingestion` 06:00, `reporting-protocol-ingestion` 07:00, `governance-file-contributors-sync` 05:00, `git-activity-contributors-{full,incremental}-sync` 06:00, `github-contributors-weekly-sync` Sun 07:00 |
| `blast-radius-worker`      | `blast-radius-worker`      | `blast-radius/`                              | None: jobs start from the public API (`submitBlastRadiusJob`)                                                                                                                                                                |
| `criticality-worker`       | `packages-worker`          | `criticality/`                               | None: a long-running process; PageRank runs with `pnpm dev:pagerank`, impact scores with the `rank_packages()` SQL function                                                                                                  |
| `github-repos-enricher`    | none                       | `enricher/`                                  | None: a continuous loop that enriches GitHub repos (activity snapshot, well-known files)                                                                                                                                     |
| `dockerhub-sync`           | none                       | `dockerhub/`                                 | None: a long-running sync of Docker Hub repositories and pulls                                                                                                                                                               |

One-off backfills, run by hand: `maven-backfill`, `maven-repo-url-backfill` and
`stewardship-backfill` (`src/bin/`).

## Sources

| Source                     | Used by                                                                                                                                                               |
| -------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| deps.dev BigQuery datasets | `bq-dataset-ingest` (the package universe, versions, dependencies, package→project links); see [`deps-dev/README.md`](src/deps-dev/README.md)                         |
| Registry APIs              | the npm, PyPI, Maven, Cargo, Go, NuGet, RubyGems and Packagist workers; see [`npm/README.md`](src/npm/README.md) and [`packagist/README.md`](src/packagist/README.md) |
| OSV                        | `osv-worker` (advisories and affected ranges)                                                                                                                         |
| OpenSSF Scorecard API      | the `scorecard` job kind in `bq-dataset-ingest`                                                                                                                       |
| GitHub (App)               | `github-repos-enricher`, `dockerhub-sync` (finds Dockerfiles), `security-contacts-worker`                                                                             |
| Docker Hub                 | `dockerhub-sync`                                                                                                                                                      |
| An LLM on Amazon Bedrock   | security contacts, member contributors and blast radius                                                                                                               |

## State

All entry points write to `packages-db`, a dedicated Postgres instance whose schema lives in
`backend/src/osspckgs/migrations` (Flyway). Connection settings come from `PACKAGES_DB_*`
(`src/db.ts`). Each registry worker keeps its own progress in a `<registry>_package_state` or
`<registry>_worker_state` table; bootstrap runs are tracked in `osspckgs_ingest_jobs`.

## Running locally

Use the `packages-worker-setup` skill to start `packages-db`, apply migrations and run a worker,
and `packages-worker-add-entrypoint` to add a new one. Useful scripts:

- `pnpm trigger-bootstrap:local [full|incremental] [ECOSYSTEMS]` starts a deps.dev bootstrap.
- `pnpm monitor:osspckgs:local` shows active and recent bootstrap jobs.
