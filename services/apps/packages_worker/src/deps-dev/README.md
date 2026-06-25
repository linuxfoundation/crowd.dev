# bq-dataset-ingest — BQ → GCS → Postgres pipeline

Ingests public BigQuery datasets (deps.dev, OpenSSF Scorecard) into the
packages database via a BQ export → GCS Parquet → staging → merge pipeline.
Runs on the `bq-dataset-ingest` Temporal task queue.

## How to run

```bash
# from services/apps/packages_worker

# local dev (hot-reload, loads backend/.env.*.local)
pnpm dev:bq-dataset-ingest:local

# prod-style start
pnpm start:bq-dataset-ingest

# trigger a bootstrap run manually
pnpm trigger-bootstrap:local [full|incremental] [ECOSYSTEMS] [options]
pnpm trigger-bootstrap       [full|incremental] [ECOSYSTEMS] [options]

# monitor active and recent jobs
pnpm monitor:osspckgs:local
pnpm monitor:osspckgs
```

See `src/scripts/triggerBootstrap.ts --help` for full options.

## BQ byte-ceiling overrides

Each job kind has a hardcoded `maxBytesGb` ceiling checked via a BQ dry-run
before the real export fires. If the dry-run exceeds the ceiling the activity
fails immediately (no BQ cost incurred). Script-mode kinds (the GO/NUGET reverse
transitive closure — a multi-statement `WHILE` loop a dry-run cannot price) skip
the dry-run and instead enforce the ceiling server-side via `maximumBytesBilled`;
there the ceiling is a runaway cap set above expected spend, not a tight gate.

Every ceiling can be overridden at runtime via an env variable — useful when a
table grows past the default ceiling without requiring a code deploy:

```
BQ_DATASET_INGEST_<KIND>_MAX_BQ_GB=<number>           # applies to all sync modes
BQ_DATASET_INGEST_<KIND>_<SYNCMODE>_MAX_BQ_GB=<number> # mode-specific, takes precedence
```

where `<KIND>` is the job kind in `UPPER_SNAKE_CASE` and `<SYNCMODE>` is `FULL` or `INCREMENTAL`.
The mode-specific key takes precedence over the generic key. Value must be a positive finite number.

**When adding a new job kind, add a row to this table.**

| Env var override                                     |            Default (GB) | Job kind                 | Notes                                                                                                                                                                                                                                                                                                                                             |
| ---------------------------------------------------- | ----------------------: | ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `BQ_DATASET_INGEST_PACKAGES_FULL_MAX_BQ_GB`          |                    6000 | `packages`               | Full only (set in `ingestPackages.ts`)                                                                                                                                                                                                                                                                                                            |
| `BQ_DATASET_INGEST_PACKAGES_INCREMENTAL_MAX_BQ_GB`   |                     400 | `packages`               | Incremental only (set in `ingestPackages.ts`)                                                                                                                                                                                                                                                                                                     |
| `BQ_DATASET_INGEST_VERSIONS_MAX_BQ_GB`               |                     400 | `versions`               |                                                                                                                                                                                                                                                                                                                                                   |
| `BQ_DATASET_INGEST_PACKAGE_DEPENDENCIES_MAX_BQ_GB`   | 25000 full / 10000 incr | `package_dependencies`   | Full scans `*Latest`. Incremental is a snapshot edge-diff (today vs watermark partitions of `DependencyGraphEdges` + `GoRequirements` + `NuGetRequirements`), matched on `(root, to_name)` excluding the resolved `to_version` to drop re-resolution churn (~4.1TB, Option A). Mode-specific `…_FULL_…` / `…_INCREMENTAL_…` keys take precedence. |
| `BQ_DATASET_INGEST_REPOS_MAX_BQ_GB`                  |                    2000 | `repos`                  |                                                                                                                                                                                                                                                                                                                                                   |
| `BQ_DATASET_INGEST_PACKAGE_REPOS_MAX_BQ_GB`          |                    2000 | `package_repos`          |                                                                                                                                                                                                                                                                                                                                                   |
| `BQ_DATASET_INGEST_ADVISORIES_MAX_BQ_GB`             |                      10 | `advisories`             |                                                                                                                                                                                                                                                                                                                                                   |
| `BQ_DATASET_INGEST_ADVISORY_PACKAGES_MAX_BQ_GB`      |                    1500 | `advisory_packages`      |                                                                                                                                                                                                                                                                                                                                                   |
| `BQ_DATASET_INGEST_DEPENDENT_COUNTS_MAX_BQ_GB`       |                    2000 | `dependent_counts`       | Edges only (NPM/MAVEN/PYPI/CARGO) from the `Dependents` reverse index. GO/NUGET are absent from `Dependents` and run as separate kinds below.                                                                                                                                                                                                     |
| `BQ_DATASET_INGEST_DEPENDENT_COUNTS_GO_MAX_BQ_GB`    |                    5000 | `dependent_counts_go`    | GO exact reverse transitive closure over `GoRequirementsLatest` (script mode). All 3 count columns. Ceiling is a `maximumBytesBilled` runaway cap above the validated full-pipeline spend (2.31 TB incl. repos aggregation), not a dry-run gate.                                                                                                  |
| `BQ_DATASET_INGEST_DEPENDENT_COUNTS_NUGET_MAX_BQ_GB` |                     200 | `dependent_counts_nuget` | NUGET exact reverse transitive closure over `NuGetRequirementsLatest` (script mode). All 3 count columns. `maximumBytesBilled` runaway cap above the measured ~32 GB.                                                                                                                                                                             |
| `BQ_DATASET_INGEST_SCORECARD_REPOS_MAX_BQ_GB`        |                      50 | `scorecard_repos`        |                                                                                                                                                                                                                                                                                                                                                   |
| `BQ_DATASET_INGEST_SCORECARD_CHECKS_MAX_BQ_GB`       |                     500 | `scorecard_checks`       |                                                                                                                                                                                                                                                                                                                                                   |

The override logic lives in `src/deps-dev/activities/bqExportToGcs.ts`.

## Environment variables

| Variable                       | Required  | Purpose                                                                                            |
| ------------------------------ | --------- | -------------------------------------------------------------------------------------------------- |
| `OSSPCKGS_GCP_PROJECT`         | yes       | GCP project ID for BQ and GCS                                                                      |
| `OSSPCKGS_GCS_BUCKET`          | yes       | GCS bucket for Parquet exports                                                                     |
| `OSSPCKGS_GCP_CREDENTIALS_B64` | yes       | Base64-encoded GCP service account JSON                                                            |
| `OSSPCKGS_DEPS_TABLE`          | no        | Set to `B` to use `DependenciesLatest` (ADR-0003 Option B) instead of `DependencyGraphEdgesLatest` |
| `CROWD_TEMPORAL_SERVER_URL`    | yes       | Temporal server address                                                                            |
| `CROWD_TEMPORAL_NAMESPACE`     | yes       | Temporal namespace (overrides `backend-config` default)                                            |
| `CROWD_TEMPORAL_CERTIFICATE`   | prod only | Base64-encoded mTLS client certificate                                                             |
| `CROWD_TEMPORAL_PRIVATE_KEY`   | prod only | Base64-encoded mTLS private key                                                                    |
