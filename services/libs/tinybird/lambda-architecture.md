# Lambda Architecture for Tinybird Data Pipelines

## Table of Contents

- [Overview](#overview)
- [Main Activity Relations Pipeline](#main-activity-relations-pipeline-lambda-architecture---produces-unfiltered-data)
- [Downstream Consumers](#downstream-consumers-of-activityrelations_enriched_deduplicated_bucket_union)
- [Timeline View](#timeline-view-hourly-execution)
- [Snapshot-Based Deduplication](#snapshot-based-deduplication-strategy)
- [Pull Requests Pipeline](#pull-requests-pipeline-primary-lambda-architecture-use-case)
- [Query Patterns](#query-patterns)
- [Initial Snapshot Pipes](#initial-snapshot-pipes-bootstrap)
- [Troubleshooting](#troubleshooting)

**Related Documentation:**
- [Main README](./README.md) - Overview and getting started
- [Bucketing Architecture](./bucketing-architecture.md) - Parallel architecture for filtered data
- [Data Flow Diagram](./dataflow) - Visual system overview

---

## Overview

This document explains the **Lambda Architecture** implementation used in our Tinybird data pipelines. Lambda Architecture is a data processing design pattern that combines:

- **Enrichment Layer (Real-time)**: Materialized Views (MVs) that process new data immediately as it arrives
- **Merge Layer (Scheduled)**: Copy pipes that merge real-time snapshots with historical data on a schedule
- **Serving Layer**: Snapshot-based datasources that provide deduplicated views via query-time filtering

> **Note**: This is one of two parallel architectures processing activityRelations data. See the [Main README](./README.md#two-parallel-architectures) for a comparison with the Bucketing Architecture.

---

## Main Activity Relations Pipeline (Lambda Architecture - Produces Unfiltered Data)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    MAIN ACTIVITY RELATIONS PIPELINE                         │
└─────────────────────────────────────────────────────────────────────────────┘

[1] Postgres tables
    ┌────────────────────────────────────────┐
    │  activityRelations                     │  
    └────────────────────────────────────────┘
                     ↓
    • Replication using logical replication slots


[2] Replication Slot Processing
    ┌────────────────────────────────────────┐
    │  Sequin                                │
    └────────────────────────────────────────┘
                     ↓
    • Each row creation and update is published as kafka queue message
    • Each message is published to its own topic
    • Messages have full row data
    • Topic names are same as table names (ie: activityRelations row changes are published to activityRelations topic)


[3] Processing Kafka Messages
    ┌────────────────────────────────────────┐
    │  Kafka Connect                         │
    └────────────────────────────────────────┘
                     ↓
    • Used to process messages published by Sequin
    • We use [lenses.io HTTP sink](https://docs.lenses.io/latest/connectors/kafka-connectors/sinks/http)
    • The sink uses [Tinybird Events API](https://www.tinybird.co/docs/api-reference/events-api) to forward messages to Tinybird
    • There are 5 tasks in tinybird sink to enable concurrency
    • Each task can process a partition in a topic independently


[4] Data Lands on Tinybird Raw Datasources
    ┌────────────────────────────────────────┐
    │  activityRelations.datasource          │
    └────────────────────────────────────────┘
                     ↓
    • TYPE: ReplacingMergeTree
    • Partitioned by: toYear(createdAt)
    • Sorting Key: segmentId, timestamp, type, platform, channel, sourceId
    • Sorting key is the deduplication key. ReplacingMergeTree engine gets rid of duplicates asnychronously


[2] Enrichment Layer - Real-time Materialized View
    ┌────────────────────────────────────────┐
    │  activityRelations_enrich              │
    │  _snapshot_MV                          │
    │  (TYPE: MATERIALIZED)                  │
    └────────────────────────────────────────┘
                     ↓ (triggers on INSERT to activityRelations)

    What it does:
    ├─ Enriches: country codes, org names, gitChangedLines, buckets
    ├─ Does NOT filter: includes ALL activities (bots, disabled repos, etc.)
    ├─ Attaches snapshot IDs to rows: toStartOfInterval(updatedAt, 1 hour) + 1 hour
    └─ Runs: Immediately on new data


[3] Enrichment Layer Output
    ┌────────────────────────────────────────┐
    │  activityRelations_enrich              │
    │  _snapshot_MV_ds                       │
    └────────────────────────────────────────┘
                     ↓
    • TYPE: ReplacingMergeTree
    • Partitioned by: toYYYYMM(snapshotId)
    • TTL: snapshotId + 1 day
    • Purpose: Temporary real-time enriched snapshots
                     │
                     │ (branches to specialized MVs)
                     │
        ┌────────────┴────────────┐
        │                         │
        ▼                         ▼
    [Pull Requests]        [Other specialized MVs]
    (see below)            (future: issues, commits, etc.)


[4] Merge Layer - Bucketed Snapshot Merger Copy Pipes
    ┌────────────────────────────────────────┐
    │  activityRelations_snapshot_           │
    │  merger_copy_0 .. _2                   │
    │  (TYPE: COPY, daily, staggered)        │
    └────────────────────────────────────────┘
                     ↓

    What each bucket pipe does (bucketing key: cityHash64(segmentId) % 3):
    ├─ Fetches: NEW deltas for its bucket from MV_ds (snapshotId >= bucket's last
    │   snapshot — inclusive, so rows arriving late for an already-merged day are
    │   replayed on the next run; the dedup below makes the replay idempotent)
    ├─ Collapses multi-day deltas: ORDER BY updatedAt DESC LIMIT 1 BY dedup key (no FINAL)
    ├─ Fetches: OLD data from its own bucket datasource (carry-forward, NOT IN delta keys)
    ├─ Merges: UNION ALL → one fresh snapshot for the bucket
    ├─ Mode: replace (atomic — a failed run leaves the previous bucket data intact)
    └─ Schedule: daily, staggered at 01:30/01:34/01:38 UTC

    Why replace-mode buckets (vs the old single append pipe + TTL):
    ├─ Old: one 380GB+ copy per day, running at 40-75% of the 3600s copy timeout,
    │        with a memory-heavy NOT IN over the full realtime snapshot
    ├─ Old: TTL (snapshotId + 2 days) expired the last good snapshot one hour BEFORE
    │        the next scheduled run — a single failed run emptied the datasource, and
    │        the empty state self-perpetuated (max(snapshotId) = epoch matches no MV
    │        rows), requiring a manual full re-bootstrap
    └─ New: 1/3rd-sized copies far from timeout/memory limits; a failure loses
             nothing; a bucket self-heals for up to ~2 missed days (MV delta
             retention is 3 days); longer gaps → run that bucket's initial snapshot


[5] Final Datasources + Union
    ┌────────────────────────────────────────┐
    │  activityRelations_enriched            │
    │  _deduplicated_bucket_0_ds .. _2_ds    │
    │  (UNFILTERED - used by CDP, monitoring)│
    └────────────────────────────────────────┘
                     ↓
    • TYPE: MergeTree (each bucket)
    • Partitioned by: toYear(timestamp)
    • Sorting Key: segmentId, timestamp, type, platform, memberId, organizationId
    • No TTL — replace-mode copies keep exactly ONE snapshot per bucket
    • Query via: activityRelations_enriched_deduplicated_bucket_union (UNION ALL of 3)
    • Bucket count: 3 for now — increasing it later remaps cityHash64(segmentId) % N,
      so add the new datasources/pipes, update the union, and re-run ALL initial
      snapshot pipes afterwards (replace mode makes the re-bootstrap safe)

    ⚠️  Do NOT filter by max(snapshotId) across the union: buckets may legitimately
        sit on different snapshot days after a partial failure, and a global max
        filter would silently drop the lagging buckets. Each bucket already holds
        exactly one snapshot, so no snapshot filtering is needed at all.
```

---

## Downstream Consumers of activityRelations_enriched_deduplicated_bucket_union

The unfiltered lambda architecture output is consumed through the
`activityRelations_enriched_deduplicated_bucket_union` pipe (never by querying
bucket datasources directly) by:

### 1. Pull Requests Pipeline (Primary consumer - detailed section below)
- Analyzes PR lifecycle: opened → reviewed → approved → merged
- Requires complete activity history for accurate PR state tracking
- Output: `pull_requests_analyzed` datasource

### 2. CDP Pipes
- **`activities_relations_filtered.pipe`**: Provides filtered views for CDP operations
- **`activities_filtered.pipe`**: Main activity filtering endpoint
- **`activities_filtered_historical_cutoff.pipe`**: Historical data with cutoff dates
- **`activities_filtered_retention.pipe`**: Retention-based activity filtering
- **`activities_daily_counts.pipe`**: Aggregates daily activity metrics for reporting
- Requires complete dataset for accurate historical counts and trend analysis

### 3. Monitoring Pipes
- **`monitoring_entities.pipe`**: Tracks entity health and data quality metrics
- **`monitoring_copy_pipe_executions.pipe`**: Monitors copy pipe execution status
- **`monitoring_copy_pipes_spread_info.pipe`**: Tracks copy pipe distribution and load
- **`monitoring_long_running_endpoints.pipe`**: Detects slow query performance
- Needs unfiltered data to monitor complete ingestion pipeline
- Detects anomalies in data flow and processing

**Why unfiltered?** These consumers require the complete, unfiltered dataset (including bot activities and disabled repositories) for PR analysis, CDP operations, and system monitoring. The bucketing architecture filters data for Insights queries, but these operational pipelines need the raw, complete view.

---

## Timeline View (Hourly Execution)

**Example: Pull Requests Pipeline**

```
═══════════════════════════════════════════════════════════════════════════════
                         HOURLY EXECUTION TIMELINE
═══════════════════════════════════════════════════════════════════════════════

Time:   :00              :30                    :59        Next Hour :00
        │                │                      │          │
        │                │                      │          │
Step 1: │ PR events      │                      │          │
        │ arrive (opened,│                      │          │
        │ reviewed,      │                      │          │
        │ approved,      │                      │          │
        │ merged)        │                      │          │
        ↓                │                      │          │
        │                │                      │          │
Step 2: MV triggers      │                      │          │
        immediately      │                      │          │
        • Enriches PR    │                      │          │
        • Calculates     │                      │          │
        •  metrics       │                      │          │
        • Adds snapshot  │                      │          │
        ↓                │                      │          │
        │                │                      │          │
Step 3: Writes to        │                      │          │
        pull_request_    │                      │          │
        analyzed_MV_ds   │                      │          │
        with snapshotId  │                      │          │
        = (next hour)    │                      │          │
                         ↓                      │          │
                         │                      │          │
Step 4:                  Copy pipe runs         │          │
                         (at :30)               │          │
                         • Fetch NEW PRs        │          │
                         •  (from MV_ds)        │          │
                         • Fetch OLD PRs        │          │
                         •  (from serving)      │          │
                         • UNION ALL            │          │
                         • New snapshotId       │          │
                         ↓                      │          │
                         │                      │          │
Step 5:                  Appends to             │          │
                         pull_requests_         │          │
                         analyzed               │          │
                         datasource             │          │
                                          ↓          ↓
                                    [Continues   [Next cycle]
                                     processing]

Queries:                 Always filter by max(snapshotId) to get latest data
                         ↓
                         Only sees deduplicated view (latest snapshot)
```

---

## Snapshot-Based Deduplication Strategy

### How It Works

Instead of using FINAL in copy pipes or query time, our approach uses **snapshot-based logical deduplication**:

1. **Physical Storage**: Multiple copies of the same record exist across different snapshots
2. **Logical View**: Queries filter by `max(snapshotId)` to see only the latest version
3. **Automatic Cleanup**: TTL removes old snapshots

> **Note**: This applies to append-mode pipelines. The unfiltered activityRelations
> serving layer now uses replace-mode bucket copies instead — each bucket holds exactly
> one snapshot, so neither TTL cleanup nor query-time snapshot filtering is involved.

### Example

```sql
-- Physical data in datasource:
┌─────────┬─────────────┬──────────────────┐
│ id      │ value       │ snapshotId       │
├─────────┼─────────────┼──────────────────┤
│ 1       │ old_value   │ 2024-01-01 14:00 │  ← Old snapshot
│ 1       │ new_value   │ 2024-01-01 15:00 │  ← New snapshot (same record)
│ 2       │ some_value  │ 2024-01-01 15:00 │
└─────────┴─────────────┴──────────────────┘

-- Query with snapshot filter (generic pattern — substitute your append-mode,
-- snapshot-stamped datasource):
SELECT *
FROM your_snapshot_ds
WHERE snapshotId = (SELECT max(snapshotId) FROM your_snapshot_ds)

-- Result (deduplicated logical view):
┌─────────┬─────────────┬──────────────────┐
│ id      │ value       │ snapshotId       │
├─────────┼─────────────┼──────────────────┤
│ 1       │ new_value   │ 2024-01-01 15:00 │  ← Latest only
│ 2       │ some_value  │ 2024-01-01 15:00 │
└─────────┴─────────────┴──────────────────┘
```

### Why This Approach?

**FINAL is costly**: We get memory issues with FINAL

**Fast filtering**: Filter by snapshotId is highly efficient (using it as partition key)

**Fast copy operations**: Append mode copys are much lightweight and fast then replace mode copys

**Reliable**: TTL automatically manages storagge

---


## Pull Requests Pipeline
The Pull Requests pipeline demonstrates how to **branch from the main pipeline** for specialized, real-time analytics.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    PULL REQUESTS SPECIALIZED PIPELINE                       │
│                  (Branches from activityRelations MV output)                │
└─────────────────────────────────────────────────────────────────────────────┘

[From Step 3 of Main Pipeline]
    activityRelations_enrich_snapshot_MV_ds
    ↓ (filters for PR-related activity types only)
    │
    ├─ pull_request-opened, merge_request-opened, changeset-created
    ├─ pull_request-assigned, pull_request-reviewed, pull_request-approved
    └─ pull_request-closed, pull_request-merged, etc.


[4*] Enrichment Layer - PR Analysis MV (Baseline-Merge Strategy)
    ┌────────────────────────────────────────┐
    │  pull_request_analysis_MV              │
    │  _baseline_merge                       │
    │  (TYPE: MATERIALIZED)                  │
    └────────────────────────────────────────┘
                     ↓ (triggers on PR-related activities)

    Node 1: new_pull_request_related_activity
    └─ Filters PR events from latest snapshot

    Node 2: new_events_aggregated
    ├─ Aggregates NEW delta events by prSourceId
    ├─ Uses argMinIf/minIf for lifecycle timestamps
    ├─ Handles opened events (sourceId) + child events (sourceParentId)
    └─ Groups by normalized PR identifier

    Node 3: pull_request_analysis_results_merged
    ├─ LEFT JOIN: new_events → pull_requests_analyzed (BASELINE)
    ├─ Merge Strategy:
    │   • Strings: if(existing != '', existing, new)
    │   • Timestamps: COALESCE + least() for earliest
    │   • Handles new PRs + updated existing PRs
    └─ Result: Merged PR records


[5*] Enrichment Layer Output
    ┌────────────────────────────────────────┐
    │  pull_request_analyzed_MV_ds           │
    └────────────────────────────────────────┘
                     ↓
    • TYPE: ReplacingMergeTree
    • Purpose: Temporary MV output


[6*] Merge Layer - PR Snapshot Merger
    ┌────────────────────────────────────────┐
    │  pull_request_analysis_snapshot        │
    │  _merger_copy                          │
    │  (TYPE: COPY, every hour at :00)       │
    └────────────────────────────────────────┘
                     ↓

    What it does:
    ├─ Fetches: NEW data from MV_ds (latest snapshot + 1 hour)
    ├─ Fetches: OLD data from pull_requests_analyzed (max snapshot)
    ├─ Merges: UNION ALL → creates new snapshot
    ├─ Mode: replace
    └─ Schedule: 0 * * * * (hourly at minute 0)


[Final] Serving Layer - PR Analytics
    ┌────────────────────────────────────────┐
    │  pull_requests_analyzed                │
    │  (queried by PR widgets)               │
    └────────────────────────────────────────┘
                     ↓
    • TYPE: MergeTree
    • Partitioned by: toYear(openedAt)
    • Sorting Key: segmentId, channel, openedAt, gitChangedLinesBucket, sourceId, id
    • No TTL (permanent analytics data)
    • Query Pattern: WHERE snapshotId = max(snapshotId)

    Schema:
    ├─ Core: id, sourceId, openedAt, segmentId, channel, memberId, organizationId
    ├─ Lifecycle: assignedAt, reviewRequestedAt, reviewedAt, approvedAt,
    │             closedAt, mergedAt, resolvedAt (all Nullable)
    └─ Metrics: assignedInSeconds, reviewedInSeconds, mergedInSeconds, etc.
```

---

## Baseline-Merge Strategy (Pull Requests)

The PR MV uses a **baseline-merge** pattern to efficiently update PR records:

### The Problem

- PRs have **one opened event** (creates the record)
- PRs have **many lifecycle events** that arrive over time (assigned, reviewed, merged)
- We need to merge new events with existing PR data without recomputing everything

### The Solution

```
┌──────────────────────────────────────────────────────────────────┐
│  BASELINE (Existing)        +        DELTA (New Events)          │
│  pull_requests_analyzed              MV latest snapshot          │
│  ────────────────────                ─────────────────           │
│  PR #123                             PR #123                     │
│  • opened: 2024-01-01                • reviewed: 2024-01-05      │
│  • assigned: 2024-01-02              (NEW lifecycle event)       │
│  • reviewed: NULL                                                │
│  • merged: NULL                                                  │
│                                                                  │
│                    ↓  MERGE LOGIC  ↓                             │
│                                                                  │
│  RESULT (New Snapshot):                                          │
│  PR #123                                                         │
│  • opened: 2024-01-01    (from baseline)                         │
│  • assigned: 2024-01-02  (from baseline)                         │
│  • reviewed: 2024-01-05  (from delta - MERGED)                   │
│  • merged: NULL          (from baseline)                         │
│  • snapshotId: 2024-01-05 16:00 (new snapshot)                   │
└──────────────────────────────────────────────────────────────────┘
```

### Merge Logic

**For String Fields** (ClickHouse returns `''` not `NULL`):
```sql
if(existing.id != '', existing.id, new.id)
```

**For DateTime Fields** (take earliest):
```sql
COALESCE(
    if(new.assignedAt IS NOT NULL AND new.assignedAt != toDateTime(0),
       least(existing.assignedAt, new.assignedAt),  -- Earlier of both
       existing.assignedAt                          -- Keep existing if no new
    ),
    new.assignedAt  -- Use new if no existing (new PR)
)
```

### Scenarios

1. **New PR**: `existing` is empty → uses all `new` data
2. **Updated PR**: Merges timestamps (takes earliest), preserves metadata
3. **Unchanged PR**: Not in new delta → not processed
4. **Replace mode copying**: Since data is currently much less than activities, we can afford replace mode copies here. Result data will always be replaced with the freshest snapshot and there'll be only one snapshot available, so that we don't have to filter by the latest snapshot for PRs.

---

## Query Patterns

### Always Filter by Latest Snapshot

**Important**: All analytics queries to  activityRelations_deduplicated_cleaned_ds **MUST** filter by `max(snapshotId)` to get deduplicated data:

- We only need this filter when we store more than one snapshots via append mode copys.
- When merging in replace mode (such as PRs) since there'll be only one snapshot available, this filtering is unnecessary.

```sql
-- ✅ CORRECT: Gets latest snapshot (deduplicated)
SELECT *
FROM activityRelations_deduplicated_cleaned_ds
WHERE snapshotId = (SELECT max(snapshotId) FROM activityRelations_deduplicated_cleaned_ds)
  AND segmentId = 'your-segment-id'
  AND timestamp >= '2024-01-01'

-- ❌ WRONG: Gets ALL snapshots (duplicates, old data)
SELECT *
FROM activityRelations_deduplicated_cleaned_ds
WHERE segmentId = 'your-segment-id'
  AND timestamp >= '2024-01-01'
```

### Why This Works

- **Partition pruning**: `snapshotId` is the partition key → fast filtering
- **Small result set**: Only 1 snapshot returned (latest data)
- **No duplicates**: Logical deduplication via snapshot filtering

---

## Initial Snapshot Pipes (Bootstrap)

Before the Lambda Architecture can run continuously, we need to **create the first snapshot** in each serving datasource. This is done via **initial snapshot copy pipes** that run **@on-demand** (manually triggered).

### Purpose

Initial snapshot pipes:
- Create the baseline/first snapshot in serving datasources
- Run once at system startup or when resetting the pipeline
- Use `COPY_MODE: replace` to overwrite the entire target datasource
- Process current data to create a deterministic starting point
- Enable subsequent merger copy pipes to work incrementally

### Examples

#### 1. activityRelations_enrich_initial_snapshot_0 .. _2 (per bucket)

```
Files: activityRelations_enrich_initial_snapshot_0.pipe .. _2.pipe

TYPE: COPY
COPY_MODE: replace
COPY_SCHEDULE: @on-demand
TARGET_DATASOURCE: activityRelations_enriched_deduplicated_bucket_<N>_ds

What each one does:
├─ Reads raw activityRelations (base table), filtered to cityHash64(segmentId) % 3 = N
├─ Enriches with country codes, org names, gitChangedLines buckets
├─ Creates snapshotId: toStartOfInterval(now(), INTERVAL 1 day)
└─ Replaces bucket N of the serving layer

Usage: Run all 3 to bootstrap the serving layer, or a single one to rebuild a
bucket that fell behind further than the MV delta retention (3 days). Replace
mode makes them safe to re-run.
```

#### 2. pull_request_analysis_initial_snapshot (run bucketed, not single-shot)

**The single-shot invocation is deprecated** — running the whole pipe in one `tb pipe copy run pull_request_analysis_initial_snapshot --wait` call against the full dataset does not reliably finish (times out on the full scan). The pipe itself is still needed for a full bootstrap/rebuild of `pull_requests_analyzed` and has **not** been removed.

Instead, run it **bucketed**: the pipe's own SQL supports `bucket_id`/`num_buckets` params (`cityHash64(segmentId) % num_buckets = bucket_id`, `COPY_MODE: append`) so a full run can be split into N smaller pieces:

```bash
for N in $(seq 0 9); do
  tb pipe copy run pull_request_analysis_initial_snapshot --param bucket_id=$N --param num_buckets=10 --mode append
done
```

This is the same subbucketing mechanism used by the generic `activityRelations_bucket_MV_snapshot_*` family described in `bucketing-architecture.md`, applied here directly inside `pull_request_analysis_initial_snapshot.pipe` rather than via a separate set of pipe files.

This is separate from the **hourly PR Merger** (`pull_request_analysis_baseline_merge_MV.pipe` + `pull_request_analysis_snapshot_merger_copy.pipe`, `COPY_MODE: replace`, `COPY_SCHEDULE: 0 * * * *`), which handles ongoing incremental dedup of `pull_requests_analyzed` once it's populated — it does not perform the initial bootstrap and does not replace the need to run `pull_request_analysis_initial_snapshot` for a full rebuild.

#### 3. segmentId_aggregates_initial_snapshot

```
File: segmentId_aggregates_initial_snapshot.pipe

TYPE: COPY
COPY_MODE: replace
COPY_SCHEDULE: @on-demand
TARGET_DATASOURCE: segmentsAggregatedMV

What it does:
├─ Reads from activityRelations_deduplicated_cleaned_ds (latest snapshot)
├─ Groups by segmentId
├─ Counts distinct contributors (memberId) and organizations (organizationId)
└─ Writes initial segment aggregates

Usage: Run once to bootstrap segment-level metrics
```

### When to Run Initial Snapshots

**Run initial snapshot pipes when:**
1. **First time setup**: Deploying the Lambda Architecture for the first time
2. **Pipeline reset**: Need to rebuild serving datasources from scratch
3. **Data corruption**: Serving datasource has bad data and needs complete refresh
4. **Schema changes**: Major changes to enrichment logic or serving datasource schema
5. **TTL deleted data abruptly**: TTL deleted data and incremental copies didn't run for some reason

**How to run:**
```bash
# Via Tinybird CLI (assuming you have tb CLI configured)
for N in 0 1 2; do tb pipe copy run activityRelations_enrich_initial_snapshot_$N --wait; done
tb pipe copy run segmentId_aggregates_initial_snapshot --wait
```

### Comparison: Initial vs Merger Copy Pipes

| Aspect | Initial Snapshot | Bucket Merger (activityRelations) | PR Merger |
|--------|------------------|-----------------------------------|-----------|
| **Status** | Active — **run bucketed only**, single-shot deprecated | Active | Active |
| **Schedule** | @on-demand (manual) | Daily (01:30/01:34/01:38 UTC) | Hourly (0 * * * *) |
| **Mode** | append¹ / replace² | replace (atomic per-bucket swap) | replace |
| **Purpose** | Bootstrap/reset (full rebuild only, run bucketed) | Incremental merge of MV deltas | Incremental merge of PR events |
| **Source** | Base tables | MV output + own bucket (carry-forward) | MV output + own target |
| **Frequency** | Once (or rarely), split across `num_buckets` runs | Continuous (daily) | Continuous (hourly) |
| **Snapshot Strategy** | Create first snapshot, one `bucket_id` at a time | One snapshot per bucket, re-stamped each run | Single snapshot, replaced each run |

¹ `pull_request_analysis_initial_snapshot`: `COPY_MODE append` (bootstrap by appending)  
² `activityRelations_enrich_initial_snapshot_*` and `segmentId_aggregates_initial_snapshot`: `COPY_MODE replace` (overwrite entire target)

---

## Troubleshooting

### Data Not Updating

**Symptom**: Queries return stale data

**Check**:
1. Is the MV running? `SELECT * FROM activityRelations_enrich_snapshot_MV_ds ORDER BY snapshotId DESC LIMIT 10`
2. Is the copy pipe scheduled? Check `COPY_SCHEDULE` in pipe definition
3. Check Tinybird logs for errors

### Duplicate Records

**Symptom**: Same activity appears multiple times

**Fix**: Ensure you're filtering by `max(snapshotId)`:
```sql
WHERE snapshotId = (SELECT max(snapshotId) FROM datasource_name)
```

### Missing Data

**Symptom**: Recent data not appearing

**Possible Causes**:
1. **TTL deleted MV deltas**: MV output retention is 3 days — a bucket that missed more
   than 2 daily runs can no longer catch up incrementally and needs its initial
   snapshot pipe re-run (the bucket keeps serving its last good data meanwhile)
2. **MV behind**: Check latest `snapshotId` in MV_ds
3. **Copy pipe not run**: Bucket mergers run daily at 01:30-01:38 UTC; PR merger hourly at :00
4. **Filtering**: Check member/repo/segment filters in MV
5. **A bucket is lagging**: Compare `max(snapshotId)` per bucket datasource — with
   replace-mode buckets a failed run leaves old (not missing) data, so look for
   buckets whose snapshotId is older than the others

## TLDR

This architecture moves heavyweight operations (enrichment, filtering, joins) to ingestion time via Materialized Views. Copy pipes only handle snapshot merging (UNION ALL of new + old data). For the unfiltered activityRelations serving layer the merge is split across 3 hash buckets with replace-mode copies — each bucket holds exactly one snapshot, so a failed copy leaves the previous data intact and queries need no snapshot filter (read the bucket union). Append-mode pipelines (e.g. historical patterns described above) deduplicate at query time by filtering `WHERE snapshotId = max(snapshotId)`, not in copy pipes or with FINAL. This keeps scheduled copies fast and lightweight, while queries remain fast by reading pre-processed snapshots.