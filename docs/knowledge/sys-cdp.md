---
type: system
slug: sys-cdp
name: CDP services
repo: crowd.dev
owner: CDP team
depends_on: []
used_by: [sys-insights]
sources:
  - services/apps/
  - services/libs/queue/src/vendors/kafka/config.ts
  - scripts/cli
---

# CDP services

`services/apps/` holds 34 services. Most are Temporal workers built on
`@crowd/archetype-worker`; a few are Kafka queue consumers or HTTP services. Shared code lives in
`services/libs/`, and database queries in `services/libs/data-access-layer`.

To find a Temporal worker's task queue, look at `CROWD_TEMPORAL_TASKQUEUE` in its
`package.json` scripts. To find what it runs, look at `src/workflows.ts`, or `src/workflows/index.ts` in workers that
split them into a directory (`packages_worker`, `pcc_sync_worker`, `snowflake_connectors`).

## Ingestion

How source data gets in. [`data-flow.md`](data-flow.md) shows how these connect.

| Service                     | Kind           | Role                                                                                                                                                                                                                                                         |
| --------------------------- | -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `webhook_api`               | HTTP           | Receives platform webhooks and hands them to `integration_stream_worker`                                                                                                                                                                                     |
| `integration_run_worker`    | Kafka consumer | Runs an integration sync and splits it into streams                                                                                                                                                                                                          |
| `integration_stream_worker` | Kafka consumer | Fetches and processes each stream, emits results to `data_sink_worker`                                                                                                                                                                                       |
| `data_sink_worker`          | Kafka consumer | Resolves members and organizations, writes `activityRelations` to Postgres, sends activity content to the `activities` Kafka topic for Tinybird, and queues an OpenSearch sync                                                                               |
| `connectors_worker`         | Temporal       | Connector framework v2 runtime: dispatches and runs syncs for the connectors registered in `src/main.ts`, currently GitHub and Octolens, plus a dummy connector in development. `services/libs/connectors` also holds Credly code that is not registered yet |
| `nango_worker`              | Temporal       | GitHub sync through Nango: repo syncs, webhook processing, connection cleanup                                                                                                                                                                                |
| `nango_webhook_api`         | HTTP           | Receives Nango webhooks and starts `nango_worker` workflows                                                                                                                                                                                                  |
| `snowflake_connectors`      | Temporal       | Exports source data from Snowflake through S3, transforms it, emits to `data_sink_worker`                                                                                                                                                                    |
| `git_integration`           | Python         | Clones repositories, extracts commits and maintainers, sends them to Kafka                                                                                                                                                                                   |
| `mailing_list_integration`  | Python         | Ingests mailing list archives (`crowdmail`)                                                                                                                                                                                                                  |

## Members, organizations and identity

Background on the data model is in [`member-identity.md`](member-identity.md).

| Service                           | Kind     | Role                                                                                                                         |
| --------------------------------- | -------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `members_enrichment_worker`       | Temporal | Enriches members from external sources under `src/sources/`: Clearbit, Crustdata, LFID, ProgAI and SERP                      |
| `organizations_enrichment_worker` | Temporal | Enriches organizations from external sources                                                                                 |
| `merge_suggestions_worker`        | Temporal | Generates member and organization merge suggestions, including LLM-checked ones                                              |
| `entity_merging_worker`           | Temporal | Finishes member and organization merges and unmerges once the records are combined: moves dependent data and deletes orphans |
| `profiles_worker`                 | Temporal | Recomputes member and organization profiles and per-segment aggregates                                                       |
| `script_executor_worker`          | Temporal | One-off and recurring data fixes, such as merging identities that differ only in capitalization                              |

## Projects

| Service                               | Kind     | Role                                                                                  |
| ------------------------------------- | -------- | ------------------------------------------------------------------------------------- |
| `automatic_projects_discovery_worker` | Temporal | Discovers candidate projects                                                          |
| `projects_evaluation_worker`          | Temporal | Evaluates candidates and hands accepted ones to onboarding                            |
| `automatic_onboarding_worker`         | Temporal | Onboards projects                                                                     |
| `categorization_worker`               | Temporal | Assigns projects to categories                                                        |
| `pcc_sync_worker`                     | Temporal | Imports PCC project data: exports it to S3 on a schedule, then parses and consumes it |

## Signals and scoring

| Service                          | Kind     | Role                                                                                                                                                                                                                      |
| -------------------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `packages_worker`                | Temporal | Package ecosystem data. One package, many entry points under `src/bin/`: npm, PyPI, Maven, Cargo, Go, NuGet, RubyGems, Packagist, OSV, Docker Hub, BigQuery ingest, criticality, blast radius, security contacts and more |
| `security_best_practices_worker` | Temporal | OSPS Baseline security checks. Insights starts `upsertOSPSBaselineSecurityInsights` here                                                                                                                                  |
| `docs_readiness_worker`          | Temporal | Documentation readiness scoring. Insights starts `processProjectDocsReadiness` here                                                                                                                                       |
| `star_snapshot_worker`           | Temporal | Captures and backfills repository star history                                                                                                                                                                            |

The two workflows Insights starts are part of the cross-repo contract; see the
[downstream consumer section in `AGENTS.md`](../../AGENTS.md#downstream-consumer-lfx-insights).

## Search, caching and exports

| Service              | Kind           | Role                                                             |
| -------------------- | -------------- | ---------------------------------------------------------------- |
| `search_sync_worker` | Kafka consumer | Copies members and organizations from Postgres to OpenSearch     |
| `search_sync_api`    | HTTP           | API for triggering OpenSearch syncs of members and organizations |
| `cache_worker`       | Temporal       | Refreshes dashboard caches                                       |
| `exports_worker`     | Temporal       | CSV exports of members and organizations                         |

## Operations

| Service                                | Kind      | Role                                                                                                                                     |
| -------------------------------------- | --------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `cron_service`                         | Scheduler | Scheduled jobs under `src/jobs/`: integration health checks, Nango monitoring, materialized view refreshes, queue monitoring and similar |
| `kafka_connect_monitor`                | HTTP      | `/connector-health` endpoint reporting Kafka Connect connector state                                                                     |
| `temporal_codec_server`                | HTTP      | `/encode` and `/decode` endpoints so the Temporal UI can read encrypted payloads                                                         |
| `template_worker`, `template_consumer` | Scaffolds | Starting points for a new Temporal worker or Kafka consumer; copy them rather than running them                                          |

## Running it locally

`README.md` at the repository root covers the development environment: `cd scripts && ./cli start`,
or `./cli clean-start-dev` for hot reloading. Individual services have their own `dev` scripts in
`package.json`; Temporal workers read `CROWD_TEMPORAL_TASKQUEUE` from those scripts.
