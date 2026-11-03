# Snowflake connectors worker

`snowflake-connectors-worker` exports source data from Snowflake to S3 as Parquet, transforms each row into CDP
activities, and sends them to `data_sink_worker`. It runs as a Temporal worker on the `snowflakeConnectors` task queue
and also runs a long-lived consumer loop in the same process.

For where this service sits in the ingestion pipeline, see
[`docs/design/activity-pipeline.md`](../../../docs/design/activity-pipeline.md). Shared Snowflake, S3 and job-table code
lives in [`services/libs/snowflake`](../../libs/snowflake).

## Components

| Component            | Code                                                                    | What it does                                                                                           |
| -------------------- | ----------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| Export workflow      | `src/workflows/exportWorkflow.ts`, `src/activities/exportActivity.ts`   | Runs `COPY INTO` in Snowflake to write Parquet files to S3, one child workflow per platform and source |
| Transformer consumer | `src/consumer/transformerConsumer.ts`                                   | Claims pending export jobs, reads the Parquet files, transforms rows and emits activities              |
| Cleanup workflow     | `src/workflows/cleanupWorkflow.ts`, `src/activities/cleanupActivity.ts` | Deletes S3 files for jobs that finished cleanly                                                        |
| Integration resolver | `src/core/integrationResolver.ts`                                       | Maps a row's project to a CDP segment and integration                                                  |
| Platform registry    | `src/integrations/index.ts`, `src/integrations/types.ts`                | Lists each platform's data sources, with a query builder and transformer per source                    |

`src/index.ts` starts the worker: it registers both Temporal schedules, starts the consumer, and then starts the
Temporal worker. On `SIGINT` or `SIGTERM` the consumer finishes its current job before stopping; the process force-exits
after 2 hours.

## Sources

A platform has one or more data sources. Each source maps to a Snowflake table, or a joined set of tables, and has its
own `buildSourceQuery.ts` and `transformer.ts` under `src/integrations/<platform>/<source>/`. Each source is exported,
tracked and transformed on its own.

| Platform     | Source                | Activity types                                 |
| ------------ | --------------------- | ---------------------------------------------- |
| `cvent`      | `event-registrations` | `registered-event`, `attended-event`           |
| `tnc`        | `enrollments`         | `enrolled-certification`, `enrolled-training`  |
| `tnc`        | `certificates`        | `issued-certification`                         |
| `tnc`        | `courses`             | `attempted-course`, `attempted-exam`           |
| `meetings`   | `meeting-attendance`  | `invited-meeting`, `attended-meeting`          |
| `committees` | `committees`          | `added-to-committee`, `removed-from-committee` |

A platform runs only when it is listed in `CROWD_SNOWFLAKE_ENABLED_PLATFORMS`. Code for a platform that is not listed
is never exported or transformed.

To add a platform or a source, use the
[`scaffold-snowflake-connector`](../../../.claude/skills/scaffold-snowflake-connector/SKILL.md) skill. It lists every
touch point, including the `PlatformType` and activity type enums, the activity types migration and the registry entry.

## How it works

### 1. Export

The `snowflake-s3-export` schedule starts `snowflakeS3ExportScheduler` every day at 00:20. For each enabled platform it
starts one `exportSnowflakeData` child workflow per source.

Each child runs `executeExport(platform, sourceName)`:

1. Reads the latest `exportStartedAt` among this source's jobs that completed without an error. With no such job, the
   export is a full export.
2. Calls the source's `buildSourceQuery(sinceTimestamp)`. With a timestamp, the query returns rows updated since then,
   plus every row for projects whose segment was created since then.
3. Runs `COPY INTO` in batches of 10,000 rows. Each batch writes one Parquet file under
   `<CROWD_SNOWFLAKE_S3_BUCKET_PATH>/<platform>/<source>/<yyyy>/<mm>/<dd>/`.
4. Inserts one row per file into `integration."snowflakeExportJobs"`.

Source queries keep only rows whose project matches a CDP segment. Outside production (`NODE_ENV` is not
`production`), each query also limits itself to a single project.

### 2. Transform

The consumer loop claims the oldest unclaimed job with `FOR UPDATE SKIP LOCKED`, so several consumers never take the
same job. For each job it:

1. Looks up the source's transformer from the job's `platform` and `sourceName`.
2. Streams the Parquet rows from S3 and calls `safeTransformRow()` on each. A transformer returns one activity, several
   activities, or `null` to skip the row. A transformer that throws is logged and counted as a skip.
3. Resolves each activity's segment and integration, then sends the activity to `data_sink_worker` in the queue message
   through `DataSinkWorkerEmitter`.
4. Marks the job completed with its metrics, or failed with the error.

When no job is pending, the loop sleeps for 10 seconds and doubles the wait on each empty poll, up to 30 minutes. A
claimed job resets the wait.

### 3. Resolve segment and integration

Every transformed row carries a segment reference built from the row's `PROJECT_SLUG` and `PROJECT_ID` columns. The
resolver matches it, case-insensitively, against a `subproject` segment's `slug` and `sourceId`. Rows with no matching
segment are skipped.

If the segment has no integration for the platform, the resolver creates one with status `done`. A Redis lock stops two
consumers from creating the same integration. Results are cached in Redis for 24 hours, and the cache is warmed at
startup for all enabled platforms.

### 4. Clean up

The `snowflake-s3-cleanup` schedule starts `snowflakeS3CleanupScheduler` every day at 02:00. It deletes the S3 file and
sets `cleanedAt` for each job that completed more than 24 hours ago with no error and zero skipped rows. Files for
failed jobs and jobs with skipped rows stay in S3 for inspection.

### Shared rules in transformers

`src/core/transformerBase.ts` and `src/core/personalEmail.ts` hold rules that every transformer uses:

- `cleanIdentityValue()` treats empty strings and placeholders such as `none`, `null`, `n/a` and `-` as missing.
- `buildMemberIdentities()` adds a verified email identity, plus username identities from the platform username, the
  LFID username or, when neither exists, the email.
- `cleanPersonalEmail()` drops known shared meeting-account emails, so they never become member identities.

## Tables

`integration."snowflakeExportJobs"` holds one row per exported Parquet file.

| Column                     | Meaning                                                                                                                                                                |
| -------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `platform`, `"sourceName"` | Which source produced the file                                                                                                                                         |
| `s3_path`                  | Parquet file location (unique)                                                                                                                                         |
| `"exportStartedAt"`        | When the export run started; the next incremental export starts here                                                                                                   |
| `"processingStartedAt"`    | Set when a consumer claims the job                                                                                                                                     |
| `"completedAt"`            | Set when processing finishes, whether it succeeded or failed                                                                                                           |
| `error`                    | Error message for a failed job                                                                                                                                         |
| `"cleanedAt"`              | When the S3 file was deleted                                                                                                                                           |
| `metrics`                  | `exportedRows` and `exportedBytes` from the export, then `transformedCount`, `skippedCount`, `transformSkippedCount`, `resolveSkippedCount` and `processingDurationMs` |

The table is created in `V1771344764__addSnowflakeExportTable.sql` and `V1772548920__addSourceNameToSnowflakeExportJobs.sql`
under `backend/src/database/migrations/`.

The resolver also reads `segments` and reads and writes `integrations`.

## Configuration

| Variable                               | Purpose                                                                          |
| -------------------------------------- | -------------------------------------------------------------------------------- |
| `CROWD_SNOWFLAKE_ENABLED_PLATFORMS`    | Comma-separated platforms to run. Empty means nothing is exported or transformed |
| `CROWD_SNOWFLAKE_ACCOUNT`              | Snowflake account                                                                |
| `CROWD_SNOWFLAKE_USERNAME`             | Snowflake user                                                                   |
| `CROWD_SNOWFLAKE_TOKEN`                | Programmatic access token. When set, it is used instead of the private key       |
| `CROWD_SNOWFLAKE_PRIVATE_KEY`          | Private key for key-pair authentication                                          |
| `CROWD_SNOWFLAKE_DATABASE`             | Snowflake database                                                               |
| `CROWD_SNOWFLAKE_WAREHOUSE`            | Snowflake warehouse                                                              |
| `CROWD_SNOWFLAKE_ROLE`                 | Snowflake role                                                                   |
| `CROWD_SNOWFLAKE_STORAGE_INTEGRATION`  | Snowflake storage integration that `COPY INTO` writes through                    |
| `CROWD_SNOWFLAKE_S3_BUCKET_PATH`       | S3 path prefix for exported files                                                |
| `CROWD_SNOWFLAKE_S3_REGION`            | S3 region                                                                        |
| `CROWD_SNOWFLAKE_S3_ACCESS_KEY_ID`     | S3 credentials used to read and delete files                                     |
| `CROWD_SNOWFLAKE_S3_SECRET_ACCESS_KEY` | S3 credentials used to read and delete files                                     |

The worker also uses the standard CDP Postgres, Redis, Kafka queue and Temporal settings. The four
`CROWD_SNOWFLAKE_S3_*` variables and `CROWD_SNOWFLAKE_STORAGE_INTEGRATION` are required at startup.

## Running locally

The `dev:local` script loads `backend/.env.dist.local` and `backend/.env.override.local`, then runs the worker with
reload on changes in `src/` and `services/libs/`:

```bash
pnpm run dev:local
```

To run an export or a cleanup outside the schedule:

```bash
pnpm run trigger-export
pnpm run trigger-cleanup
```

Both scripts start the workflow on the `snowflakeConnectors` task queue, so a worker must be running to pick it up.

To run the worker in Docker with the rest of the local stack, use `./cli service snowflake-connectors-worker up`.

## Troubleshooting

- **Nothing is exported.** Check that the platform is in `CROWD_SNOWFLAKE_ENABLED_PLATFORMS`. The schedules are
  created once; if `snowflake-s3-export` already exists in Temporal, a change to its schedule in code does not apply
  until the schedule is updated or deleted there.
- **Activities are missing for a project.** Look at `resolveSkippedCount` in the job's `metrics`. A non-zero count means
  rows had no matching `subproject` segment for their `PROJECT_SLUG` and `PROJECT_ID`.
- **Rows are skipped by the transformer.** Look at `transformSkippedCount` and the `Failed to transform row, skipping`
  log lines, which list the row's keys.
- **A job failed.** Its `error` column holds the message. The S3 file stays in place, but the job is not claimed again.
- **A new source got a full export.** That is expected: a source with no completed job starts without a timestamp.
