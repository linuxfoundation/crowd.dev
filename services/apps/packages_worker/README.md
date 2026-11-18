# packages_worker

Collects open source package data (registries, deps.dev, OSV, OpenSSF Scorecard, GitHub) into
`packages-db` and runs the pipelines built on it: package→repository mapping, security contacts,
criticality, stewardship, blast radius and member contributors. Everything in this folder is one
npm package and one Docker image (`scripts/services/docker/Dockerfile.packages`); each
entry point under `src/bin/` runs as its own process.

Where this fits in CDP, and what reads the data, is in
[`docs/knowledge/sys-packages.md`](../../../docs/knowledge/sys-packages.md) and
[`docs/design/packages.md`](../../../docs/design/packages.md). Why it is built this way is in
[ADR-0001](../../../docs/adr/0001-oss-packages-design-decisions.md) and the other packages ADRs
listed there. Terms are defined in [`CONTEXT.md`](CONTEXT.md).

## Entry points

Each script below is `pnpm start:<name>` (production) or `pnpm dev:<name>:local` (local, loads
`backend/.env.*.local`). A Temporal entry point registers its schedules on start and then serves
its task queue. These workers use their own Temporal namespace, separate from the rest of CDP:
each one reads `CROWD_TEMPORAL_NAMESPACE`, which the service files in `scripts/services/` set to
`CROWD_PACKAGES_TEMPORAL_NAMESPACE`. The public API starts jobs in
`CROWD_PACKAGES_TEMPORAL_NAMESPACE` through `backend/src/db/packagesTemporal.ts`, so the two must
match or the jobs are never picked up.

| Entry point                | Task queue                 | Code                                         | Schedules it registers (UTC)                                                                                                                                                                                                       |
| -------------------------- | -------------------------- | -------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bq-dataset-ingest`        | `bq-dataset-ingest`        | `deps-dev/`, `scorecard/`, `package-repos/`  | `osspckgs-bootstrap-weekly` Mon 02:00, `pypi-downloads-daily` 06:00, `pypi-downloads-30d-monthly` 4th 06:00, `package-repo-confidence-sweep-daily` 05:00                                                                           |
| `npm-worker`               | `npm-worker`               | `npm/`                                       | `npm-registry-ingest` 03:15, `npm-daily-downloads-backfill` 03:30, `npm-last-30d-history-backfill` 04:30, `npm-last-30d-latest-refresh` 1st 04:00                                                                                  |
| `pypi-worker`              | `pypi-worker`              | `pypi/`                                      | `pypi-registry-ingest` 05:00                                                                                                                                                                                                       |
| `maven-worker`             | `packages-worker`          | `maven/`                                     | `maven-ingestion` 09:00                                                                                                                                                                                                            |
| `cargo-worker`             | `cargo-worker`             | `cargo/`                                     | `cargo-registry-sync` 06:00                                                                                                                                                                                                        |
| `go-worker`                | `go-worker`                | `go/`                                        | `go-version-enrich` 02:00, `go-status-enrich` 05:00                                                                                                                                                                                |
| `nuget-worker`             | `nuget-worker`             | `nuget/`                                     | `nuget-registry-ingest` 07:00                                                                                                                                                                                                      |
| `rubygems-worker`          | `rubygems-worker`          | `rubygems/`                                  | `rubygems-registry-ingest` 07:00, `rubygems-critical-ingest` 09:00                                                                                                                                                                 |
| `packagist-worker`         | `packagist-worker`         | `packagist/`                                 | `packagist-seed`, `packagist-downloads-30d`, `packagist-downloads-daily`, `packagist-transitive-backstop` (crons in `packagist/`)                                                                                                  |
| `osv-worker`               | `osv-worker`               | `osv/`                                       | `osv-advisories-sync` 03:30                                                                                                                                                                                                        |
| `security-contacts-worker` | `security-contacts-worker` | `security-contacts/`, `member-contributors/` | `security-contacts-ingestion` 06:00, `reporting-protocol-ingestion` 07:00, `governance-file-contributors-sync` 05:00, `git-activity-contributors-{full,incremental}-sync` 06:00, `github-contributors-weekly-sync` Sun 07:00       |
| `blast-radius-worker`      | `blast-radius-worker`      | `blast-radius/`                              | None: jobs start from the public API (`submitBlastRadiusJob`)                                                                                                                                                                      |
| `criticality-worker`       | none                       | `criticality/`                               | None: a long-running process that holds a `packages-db` connection. Impact ranking runs in the weekly deps.dev bootstrap or with `pnpm run:impact`, both calling `rank_packages_chunked()`; PageRank runs with `pnpm dev:pagerank` |
| `github-repos-enricher`    | none                       | `enricher/`                                  | None: a continuous loop that enriches GitHub repos (activity snapshot, well-known files)                                                                                                                                           |
| `dockerhub-sync`           | none                       | `dockerhub/`                                 | None: a long-running sync of Docker Hub repositories and pulls                                                                                                                                                                     |

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
| Tinybird                   | the git activity contributor sync in `security-contacts-worker` (commit contributor snapshot, `CROWD_PACKAGES_TB_TOKEN`)                                              |

## State

All entry points write to `packages-db`, a dedicated Postgres instance whose schema lives in
`backend/src/osspckgs/migrations` (Flyway). Connection settings come from `CROWD_PACKAGES_DB_WRITE_HOST`, `CROWD_PACKAGES_DB_PORT`,
`CROWD_PACKAGES_DB_DATABASE`, `CROWD_PACKAGES_DB_USERNAME` and `CROWD_PACKAGES_DB_PASSWORD`
(`getPackagesDbConfig()` in `src/config.ts`). `bq-dataset-ingest` and `security-contacts-worker`
also connect to the CDP database. Each registry worker keeps its own progress in a `<registry>_package_state` or
`<registry>_worker_state` table; bootstrap runs are tracked in `osspckgs_ingest_jobs`.

## Running locally

1. Start `packages-db` (port 5434) and Temporal with the rest of the local infrastructure:
   `./scripts/cli scaffold up`. Every entry point except `github-repos-enricher`, `dockerhub-sync` and
   `criticality-worker` needs Temporal.
2. Apply the `packages-db` migrations with the Flyway image in `backend/src/osspckgs/`; the
   `packages-worker-setup` skill has the exact commands.
3. Start an entry point, either in Docker with `DEV=1 ./scripts/cli service <entry-point> up`
   (each one has `scripts/services/<entry-point>.yaml`, except `criticality-worker`), or directly
   with `pnpm dev:<entry-point>:local` from this folder. The `pnpm` scripts load
   `backend/.env.dist.local` and `backend/.env.override.local` after your shell, so a
   `CROWD_TEMPORAL_NAMESPACE` set in the shell is overwritten and the worker polls the namespace
   those files set. Workers connect with `CROWD_TEMPORAL_SERVER_URL` and
   `CROWD_TEMPORAL_NAMESPACE`; the API starts on-demand jobs with
   `CROWD_PACKAGES_TEMPORAL_SERVER_URL` and `CROWD_PACKAGES_TEMPORAL_NAMESPACE`. For those jobs to
   reach a worker started this way, point the API at the same Temporal instance and namespace:
   set `CROWD_PACKAGES_TEMPORAL_NAMESPACE` in `backend/.env.override.local`, and set
   `CROWD_PACKAGES_TEMPORAL_SERVER_URL` to the address the API can reach, `localhost:7233` when
   the API runs on the host or `temporal:7233` in `backend/.env.override.composed` when it runs in
   Docker (the Docker API loads that file last). Do not change the `CROWD_TEMPORAL_*` values: every
   local CDP service reads them. `pnpm trigger-bootstrap:local`
   connects through `CROWD_TEMPORAL_*`, like the workers, so it needs no change.

Use the `packages-worker-add-entrypoint` skill to add a new entry point. Useful scripts:

- `pnpm trigger-bootstrap:local [full|incremental] [ECOSYSTEMS]` starts a deps.dev bootstrap.
- `pnpm monitor:osspckgs:local` shows active and recent bootstrap jobs.
