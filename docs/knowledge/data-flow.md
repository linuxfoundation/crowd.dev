---
type: data-flow
slug: flow-activities
name: How an activity reaches Tinybird and LFX Insights
repo: crowd.dev
owner: CDP team
depends_on: [sys-cdp, model-member-identity]
used_by: [sys-insights]
sources:
  - services/libs/queue/src/vendors/kafka/config.ts
  - services/apps/data_sink_worker/src/main.ts
  - services/libs/data-access-layer/src/activities/sql.ts
  - services/libs/data-access-layer/src/activities/insert.ts
  - services/libs/tinybird/dataflow.md
  - services/libs/tinybird/lambda-architecture.md
  - services/libs/tinybird/bucketing-architecture.md
---

# How an activity reaches Tinybird and LFX Insights

An activity is something a person did on a platform: a commit, a pull request review, a mailing
list post. This note is the end-to-end map. The Tinybird half is documented in depth in
`services/libs/tinybird/`; this note covers the half before it and points into those docs.

Each activity is split in two at the data sink, and the halves reach Tinybird by different
routes.

The diagrams of the whole path, from ingestion to LFX Insights, are in
[`docs/design/activity-pipeline.md`](../design/activity-pipeline.md).

## 1. Ingestion paths

Every path ends at the `data_sink_worker` Kafka queue, except where noted.

| Path            | Services                                                             | Notes                                                                                                                                           |
| --------------- | -------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| Integrations v1 | `webhook_api`, `integration_run_worker`, `integration_stream_worker` | Queue chain on Kafka: a run splits into streams, each stream emits results. Queue settings in `services/libs/queue/src/vendors/kafka/config.ts` |
| Connectors v2   | `connectors_worker`                                                  | Temporal-based framework in `services/libs/connectors`                                                                                          |
| Nango           | `nango_webhook_api`, `nango_worker`                                  | GitHub through Nango                                                                                                                            |
| Snowflake       | `snowflake_connectors`                                               | Exports through S3, transforms, emits to the data sink                                                                                          |
| Git             | `git_integration`                                                    | Python; sends processed commits to Kafka                                                                                                        |
| Mailing lists   | `mailing_list_integration`                                           | Python (`crowdmail`)                                                                                                                            |

## 2. The data sink

`data_sink_worker` is where an activity is stored. For each result, in this order, it:

1. resolves the member from the activity's identities, creating or merging as needed (see
   [`member-identity.md`](member-identity.md));
2. resolves the organization to credit, with `findAffiliation`;
3. sends the **full activity**, body and attributes included, through `insertActivities` to the
   Kafka topic `activities` (`services/libs/data-access-layer/src/activities/insert.ts`);
4. writes the **relation row** through `createOrUpdateRelations` into Postgres `activityRelations`
   (`services/libs/data-access-layer/src/activities/sql.ts`);
5. queues an OpenSearch sync through `search_sync_worker`.

Because the Kafka send comes first, a failure between steps 3 and 4 can leave an activity in
Tinybird's `activities` with no relation row in Postgres yet.

The relation row is not just foreign keys. Besides `memberId`, `organizationId`, `segmentId` and
the other links, it keeps the activity's `type`, `timestamp`, `platform`, `channel`, `sourceId`,
`sourceParentId`, `sentimentScore`, `score`, `gitInsertions`, `gitDeletions`, `isContribution`
and `pullRequestReviewState`. What Postgres does **not** keep is the rest of the payload: the
body, the attributes and other free-form fields. Those exist only in Tinybird.

Relations live in Postgres, which is why member merges, affiliation changes and bot flags can be
corrected after the fact: they update `activityRelations`, and the change replicates to Tinybird.
When an activity is re-ingested, the data sink reuses the existing `activityId` and lets
Tinybird's deduplication keys resolve the upsert, rather than reading the old content back from
Tinybird.

## 3. Into Tinybird

**Relations.** Sequin reads Postgres logical replication and publishes every row change to a Kafka
topic named after the table. Kafka Connect forwards those messages to the Tinybird Events API,
landing in the `activityRelations` raw datasource.

**Full activity.** Kafka Connect forwards the `activities` topic the same way, into the `activities`
datasource. Sequin is not involved.

Tinybird then deduplicates, enriches and buckets the data through materialized views and copy
pipes. Pipes read whichever datasource a query needs and join them where it needs both.

Read these rather than restating them here:

- [`services/libs/tinybird/dataflow.md`](../../services/libs/tinybird/dataflow.md): system diagram,
  including the Snowflake branch
- [`services/libs/tinybird/README.md`](../../services/libs/tinybird/README.md): overview and the two
  parallel `activityRelations` architectures
- [`lambda-architecture.md`](../../services/libs/tinybird/lambda-architecture.md) and
  [`bucketing-architecture.md`](../../services/libs/tinybird/bucketing-architecture.md): how each
  works
- [`sequin.md`](../../services/libs/tinybird/sequin.md): Sequin configuration

`kafka_connect_monitor` reports connector health at `/connector-health`.

## 4. Consumers

LFX Insights reads Tinybird pipes from its Nuxt server, its public API and one worker, and writes
community mentions into the `mentions` datasource. Its side of the contract, including every pipe
and datasource it relies on, is in
[`docs/knowledge/cross-repo-contract.md`](https://github.com/linuxfoundation/insights/blob/main/docs/knowledge/cross-repo-contract.md)
in the Insights repository.

## Where to look when numbers look wrong

- **Activity missing entirely**: check the ingestion path's own health first (`cron_service` runs
  `integrationResultsCheck`), then whether `data_sink_worker` wrote the relation to
  `activityRelations`.
- **Relation in Postgres but not in Tinybird**: Sequin or Kafka Connect; start with
  `kafka_connect_monitor`.
- **Relation present but the body or attributes are missing**: the `activities` topic or its Kafka
  Connect sink. Postgres has no copy of those fields to recover them from.
- **Row in Tinybird but not in a pipe's result**: the Tinybird layer, usually deduplication,
  bucketing or a filter such as bot status or a disabled repository.
- **Wrong person or organization credited**: identity resolution or affiliation; see
  [`member-identity.md`](member-identity.md).
