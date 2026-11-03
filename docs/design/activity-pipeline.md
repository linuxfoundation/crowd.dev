---
type: design
slug: design-activity-pipeline
name: Activity pipeline
repo: crowd.dev
owner: CDP team
depends_on: [sys-cdp, model-member-identity]
used_by: [flow-activities, sys-cdp]
sources:
  - services/libs/queue/src/vendors/kafka/config.ts
  - services/libs/queue/src/types.ts
  - services/libs/common_services/src/services/emitters/dataSinkWorker.emitter.ts
  - services/apps/webhook_api/src/main.ts
  - services/apps/integration_run_worker/src/service/integrationRunService.ts
  - services/apps/integration_stream_worker/src/service/integrationDataService.ts
  - services/libs/connectors/src/emit.ts
  - services/apps/nango_webhook_api/src/main.ts
  - services/apps/nango_worker/src/activities/nangoActivities.ts
  - services/apps/snowflake_connectors/src/consumer/transformerConsumer.ts
  - services/libs/snowflake/src/snowflakeExporter.ts
  - services/apps/git_integration/src/crowdgit/services/commit/commit_service.py
  - services/apps/mailing_list_integration/src/crowdmail/
  - services/apps/cron_service/src/jobs/integrationResultsCheck.job.ts
  - services/apps/data_sink_worker/src/main.ts
  - services/apps/data_sink_worker/src/service/activity.service.ts
  - services/libs/data-access-layer/src/activities/sql.ts
  - services/libs/data-access-layer/src/activities/insert.ts
  - scripts/scaffold/sequin.yml
  - scripts/scaffold/kafka-connect/tinybird-local-sink.properties
  - services/libs/tinybird/dataflow.md
  - services/libs/tinybird/lambda-architecture.md
  - services/libs/tinybird/bucketing-architecture.md
---

# Activity pipeline

How an activity travels from a source platform to Tinybird and LFX Insights. The text map,
with the code paths and troubleshooting steps, is in
[`docs/knowledge/data-flow.md`](../knowledge/data-flow.md). This doc holds the pictures.

## End to end

Every ingestion path hands its results to `data_sink_worker`. The data sink splits each activity
in two, and the halves reach Tinybird by different routes: the full activity goes straight to a
Kafka topic, and the relation row goes through Postgres and Sequin. Kafka Connect forwards both
topics to the Tinybird Events API, which lands each topic in the datasource of the same name.

```mermaid
flowchart LR
    src["Source platform"] --> ing["Ingestion service"]
    ing --> dsw["data_sink_worker"]

    dsw -->|"full activity"| kAct[["Kafka: activities"]]
    dsw -->|"relation row"| pg[("Postgres: activityRelations")]
    pg --> sequin["Sequin"]
    sequin --> kRel[["Kafka: activityRelations"]]

    kAct --> kc["Kafka Connect"]
    kRel --> kc
    kc -->|"Events API"| tbAct[("Tinybird: activities")]
    kc -->|"Events API"| tbRel[("Tinybird: activityRelations")]

    tbAct --> pipes["Tinybird pipes"]
    tbRel --> pipes
    pipes --> insights["LFX Insights"]
    pipes --> other["Other consumers"]
```

Sequin replicates other Postgres tables the same way, such as `members`, `organizations` and
`insightsProjects`. What happens inside Tinybird, the deduplication, enrichment and bucketing, is
library-internal and documented in `services/libs/tinybird/`:

- [`dataflow.md`](../../services/libs/tinybird/dataflow.md): system diagram, including the
  Snowflake branch
- [`lambda-architecture.md`](../../services/libs/tinybird/lambda-architecture.md) and
  [`bucketing-architecture.md`](../../services/libs/tinybird/bucketing-architecture.md): the two
  `activityRelations` architectures

## Ingestion paths

Each path reaches the data sink through the `data-sink-worker` Kafka queue. Most paths first
write the raw result to the Postgres table `integration.results` and send only its ID;
`data_sink_worker` reads the result back from that table. `snowflake_connectors` sends the
activity inside the queue message instead.

```mermaid
flowchart LR
    subgraph v1["Integrations v1"]
        webhook["webhook_api"] --> qStream[["integration-stream-worker queue"]]
        run["integration_run_worker"] --> qStream
        qStream --> stream["integration_stream_worker"]
    end

    subgraph v2["Connectors v2"]
        conn["connectors_worker"]
    end

    subgraph nango["Nango"]
        nangoApi["nango_webhook_api"] -->|"Temporal workflow"| nangoW["nango_worker"]
    end

    subgraph python["Python services"]
        git["git_integration"]
        mail["mailing_list_integration"]
    end

    subgraph sf["Snowflake"]
        snow[("Snowflake")] -->|"COPY INTO"| s3[("S3 export")]
        s3 --> sfc["snowflake_connectors"]
    end

    results[("Postgres: integration.results")]
    qSink[["data-sink-worker queue"]]

    stream --> results
    conn --> results
    nangoW --> results
    git --> results
    mail --> results

    stream -->|"result ID"| qSink
    conn -->|"result ID"| qSink
    nangoW -->|"result ID"| qSink
    git -->|"result ID"| qSink
    mail -->|"result ID"| qSink
    sfc -->|"activity in message"| qSink

    qSink --> dsw["data_sink_worker"]
    results -.->|"read by ID"| dsw
```

In production, `cron_service` runs `integrationResultsCheck` every 10 minutes. When the
`data-sink-worker` queue runs low and `integration.results` still holds pending results, it
re-queues the pending result IDs.

## Inside the data sink

For each batch, `data_sink_worker` resolves members and organizations before it writes anything,
then writes the full activity to Kafka before it writes the relation row to Postgres. A failure
between those two writes leaves an activity in Tinybird's `activities` with no relation row in
Postgres yet.

```mermaid
sequenceDiagram
    participant Q as data-sink-worker queue
    participant DS as data_sink_worker
    participant PG as Postgres
    participant K as Kafka
    participant SS as search_sync_worker

    Q->>DS: result
    DS->>PG: resolve member (create or merge)
    DS->>PG: findAffiliation
    DS->>K: insertActivities to "activities" topic
    DS->>PG: createOrUpdateRelations into activityRelations
    DS->>SS: triggerMembersSync
```

`search_sync_worker` then updates the member in OpenSearch.
