# Bucketing Architecture for ActivityRelations

## Table of Contents

- [Overview](#overview)
- [Why Bucketing?](#why-bucketing)
- [Complete Data Flow](#complete-data-flow)
- [Query Layer](#query-layer)
- [Hash-Based Bucketing Strategy](#hash-based-bucketing-strategy)
- [Bootstrap/Initial Load](#bootstrapinitial-load)
- [Query Patterns](#query-patterns)
- [Monitoring & Maintenance](#monitoring--maintenance)
- [Adding New Buckets](#adding-new-buckets)

**Related Documentation:**
- [Main README](./README.md) - Overview and getting started
- [Lambda Architecture](./lambda-architecture.md) - Parallel architecture for unfiltered data
- [Data Flow Diagram](./dataflow) - Visual system overview

---

## Overview

The **bucketing architecture** is a distributed data processing pattern implemented for the activityRelations pipeline in Tinybird. This architecture partitions incoming data into 10 parallel buckets using consistent hash-based routing, enabling parallel copy pipe processing and requests routing to smaller buckets instead of the full data

## Why Bucketing?

- **Parallel Processing**: 10 independent buckets process data concurrently
- **Better Resource Utilization**: Copy pipes can run in parallel, and parallelization can be increased by adding more copy workers
- **Scaling to 1B+ activities**: In the future we have the option to have more buckets, spreading data even more


## But What's Wrong with Lambda Architecture?

The bucketing architecture differs from the lambda architecture used for other pipelines (like pull requests). The problem with using lambda architecture for activityRelations processing is that, the result dataset depends on more than one table:

- activityRelations for changing activity data
- members for marking members as bot or non-bot
- enabling/disabling repositories in the repositories

These operations will change the result dataset and with each change new rows can be added or removed from the resulting set. However lambda architecture works on single-table insert triggers (the initial MV) so we can't listen to all these events at the same time.

That's the main reason we can't get away with append-only copies and creating snapshots using new data triggered by materialized views. Instead we need replace copy operations hourly, where we check again if members are bots or not, repositories are enabled or disabled etc.


## Complete Data Flow

### [1] Source: activityRelations Datasource

The pipeline starts with the `activityRelations.datasource`, which receives data replicated from PostgreSQL.

**Upstream Data Flow:**
```
PostgreSQL activityRelations table
  ↓ (logical replication slot)
Sequin (replication processor)
  ↓ (publishes row changes)
Kafka Topic: activityRelations
  ↓ (HTTP sink connector)
Kafka Connect
  ↓ (HTTP POST to Events API)
Tinybird Events API
  ↓ (ingests JSON)
activityRelations.datasource
```

**Datasource Configuration:**
- **File**: `datasources/activityRelations.datasource`
- **Engine**: ReplacingMergeTree
- **Version Column**: ENGINE_VER "updatedAt"
- **Partitioning**: toYear(createdAt)
- **Sorting Key**: segmentId, timestamp, type, platform, channel, sourceId

### [2] Bucketing Layer: 10 Parallel Materialized Views

Data flows from the source datasource into 10 parallel materialized views that split records by segment.

**Materialized View Pipes:**
- `pipes/activityRelations_bucket_MV_0.pipe` through `activityRelations_bucket_MV_9.pipe`

**Bucketing Logic:**
Each MV filters data using a hash-based partitioning strategy:
```sql
SELECT * FROM activityRelations
WHERE cityHash64(segmentId) % 10 = {bucket_number}
```

**Characteristics:**
- **Type**: MATERIALIZED (triggers immediately on INSERT to activityRelations)
- **Purpose**: Distribute incoming data into 10 partitions
- **Distribution**: Each bucket receives ~10% of total data
- **Consistency**: Same segmentId always routes to same bucket (deterministic hashing)

**Note on bucket sizes:** It's normal for some buckets to contain slightly more data than others, since different segments generate different volumes of activities. The hash function ensures distribution on segments, because project specific insights pages will always filter by projects (segmentIds). 

### [3] Raw Bucket Datasources

Each materialized view writes to its corresponding raw bucket datasource.

**Datasources:**
- `datasources/activityRelations_bucket_MV_ds_0.datasource` through `activityRelations_bucket_MV_ds_9.datasource`

**Configuration:**
- **Engine**: ReplacingMergeTree
- **Version Column**: ENGINE_VER "updatedAt"
- **Partitioning**: toYear(createdAt)
- **Sorting Key**: segmentId, timestamp, type, platform, channel, sourceId

These datasources hold the raw, unenriched data for each bucket.

### [4] Enrichment + Cleaning Layer: 10 Parallel Copy Pipes

Hourly scheduled copy pipes enrich and filter the data in each bucket.

**Copy Pipes:**
- `pipes/activityRelations_bucket_clean_enrich_copy_pipe_0.pipe` through `activityRelations_bucket_clean_enrich_copy_pipe_9.pipe`

**Schedule (Staggered):**
The pipes run on staggered schedules to distribute load:
- **Pipes 0-1**: `10 * * * *` (every hour at minute :10)
- **Pipes 2-3**: `14 * * * *` (every hour at minute :14)
- **Pipes 4-5**: `18 * * * *` (every hour at minute :18)
- **Pipes 6-7**: `22 * * * *` (every hour at minute :22)
- **Pipes 8-9**: `26 * * * *` (every hour at minute :26)

**Configuration:**
- **COPY_MODE**: replace (overwrites entire bucket each run)
- **COPY_SCHEDULE**: Hourly with 4-minute stagger between groups

**Operations Performed:**

These copy pipes perform three distinct operations that transform raw bucket data into production-ready analytics data:

1. **Enrichment** (Adding computed fields and metadata):
   - Calculates `gitChangedLines` (gitInsertions + gitDeletions)
   - Categorizes into `gitChangedLinesBucket` (1-9, 10-59, 60-99, 100-499, 500+)
   - Adds `organizationCountryCode` via country mapping
   - Adds `organizationName` from organizations table
   - Generates `snapshotId` (toStartOfInterval(now(), INTERVAL 1 hour))

2. **Cleaning** (Filtering out invalid/unwanted data):
   - Filters by valid members: `memberId IN (SELECT id FROM members_sorted)` (removes bots)
   - Filters by valid repositories for git platforms: `excluded = false AND enabled = true` (removes disabled/excluded repos)
   - Filters by valid segments via the `repositories` table
   - This is why bucketing output is "cleaned" - invalid data is removed

3. **Deduplication** (Ensuring data consistency):
   - Uses FINAL modifier on source to deduplicate by ReplacingMergeTree version
   - Joins with organizations table using FINAL
   - Ensures only the latest version of each activity is included

### [5] Cleaned Bucket Datasources

The enrichment copy pipes write to cleaned bucket datasources.

**Datasources:**
- `datasources/activityRelations_deduplicated_cleaned_bucket_0_ds.datasource` through `activityRelations_deduplicated_cleaned_bucket_9_ds.datasource`

**Configuration:**
- **Engine**: MergeTree (not ReplacingMergeTree - data is already deduplicated)
- **Partitioning**: toYear(timestamp)
- **Sorting Key**: segmentId, timestamp, type, platform, memberId, organizationId

These datasources serve as the final, queryable data layer for each bucket.

### [6] Query Layer

Two query patterns are provided for accessing bucketed data:

#### Union Pipe (Cross-Bucket Queries)

**Pipe**: `pipes/activityRelations_deduplicated_cleaned_bucket_union.pipe`

**Purpose**: Queries across all buckets when segment is unknown or query spans multiple segments

**SQL Pattern**:
```sql
SELECT * FROM activityRelations_deduplicated_cleaned_bucket_0_ds
UNION ALL
SELECT * FROM activityRelations_deduplicated_cleaned_bucket_1_ds
UNION ALL
...
UNION ALL
SELECT * FROM activityRelations_deduplicated_cleaned_bucket_9_ds
```

**Use Cases**:
- Multi-segment analytics
- Global aggregations
- Queries without segment filter

#### Routing Pipe (Single-Bucket Queries)

**Pipe**: `pipes/activityRelations_bucket_routing.pipe`

**Purpose**: Routes query to specific bucket for faster single-segment queries

**Parameters**:
- `bucketId` (Int8): The bucket number (0-9) to query

**SQL Pattern**:
```sql
SELECT * FROM activityRelations_deduplicated_cleaned_bucket_{{ bucketId }}_ds
```

**Use Cases**:
- Single segment queries
- Queries where segment is known
- Performance-critical lookups

**BucketId Resolution**:

The bucketId can be obtained in several ways:

1. **Using the project_buckets pipe**: Query the `project_buckets` pipe with a segmentId to get its bucket assignment:
   ```sql
   SELECT bucketId FROM project_buckets WHERE segmentId = 'your-segment-id'
   ```

3. **Insights**: When using the Insights API, bucketId is automatically injected when a `project` parameter is present. No additional bucketId parameter is required - the API handles routing transparently.

## Hash-Based Bucketing Strategy

### Algorithm

The bucketing uses ClickHouse's `cityHash64` function with modulo 10:

```sql
cityHash64(segmentId) % 10 = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
```

## Bootstrap/Initial Load

For initial deployment or bucket recreation, snapshot pipes populate buckets from existing data.

### Snapshot Pipes

**Pipes**: `pipes/activityRelations_bucket_MV_snapshot_0.pipe` through `activityRelations_bucket_MV_snapshot_9.pipe`

**Configuration**:
- **Type**: COPY
- **Schedule**: @on-demand (manual execution)
- **COPY_MODE**: append
- **Source**: activityRelations datasource
- **Target**: activityRelations_bucket_MV_ds_0 through ds_9

**SQL Pattern**:
```sql
SELECT * FROM activityRelations
WHERE cityHash64(segmentId) % 10 = {bucket_number}
```

### Bootstrap Procedure

1. **Prepare**: Ensure all bucket datasources exist
2. **Execute Snapshots**: Run all 10 snapshot pipes manually
3. **Verify**: Check each bucket has ~10% of total records
4. **Enable Enrichment**: Allow scheduled copy pipes to run
5. **Monitor**: Watch for snapshotId updates in cleaned buckets

## Query Patterns

### Single Segment Query (Use Routing)

```sql
-- Calculate bucket
SET bucketId = cityHash64('segment-123') % 10;

-- Query specific bucket
SELECT * FROM activityRelations_bucket_routing
WHERE segmentId = 'segment-123'
  AND timestamp >= '2024-01-01'
```

**Performance**: ✓ Fast (scans only 1 bucket)

### Multi-Segment Query (Use Union)

```sql
SELECT segmentId, COUNT(*) as activity_count
FROM activityRelations_deduplicated_cleaned_bucket_union
WHERE timestamp >= '2024-01-01'
  AND segmentId IN ('segment-1', 'segment-2', 'segment-3')
GROUP BY segmentId
```

**Performance**: ~ Moderate (scans up to 3 buckets, filtered by WHERE)

### Global Aggregation (Use Union)

```sql
SELECT DATE(timestamp) as date, COUNT(*) as total_activities
FROM activityRelations_deduplicated_cleaned_bucket_union
WHERE timestamp >= '2024-01-01'
GROUP BY date
```

**Performance**: ✗ Slower but in ok range (scans all 10 buckets)

## Monitoring & Maintenance

### Common Maintenance Tasks

**Manually Trigger Enrichment**:
If a bucket is stale, manually trigger its copy pipe:
```bash
tb pipe copy run activityRelations_bucket_clean_enrich_copy_pipe_3 --wait
```

**Rebuild Single Bucket**:
1. Truncate cleaned bucket: `TRUNCATE TABLE activityRelations_deduplicated_cleaned_bucket_3_ds`
2. Run enrichment pipe manually

**Rebuild All Buckets**:
1. Run all snapshot pipes to repopulate raw buckets
2. Run all enrichment pipes to populate cleaned buckets

### Adding New Buckets
> ****Cost Considerations****
> - More buckets = more concurrent copy pipes = higher compute costs
> - Balance between parallelism and resource utilization

As data volume grows, you can scale from 10 buckets to 20, 50, or 100 buckets by following these steps:




**1. Plan the New Bucket Count**
- Choose a new modulo divisor (e.g., 20, 50, or 100)
- Ensure it's a multiple of 10 for easier migration (10 → 20 → 40, etc.)
- Consider resource capacity: more buckets = more concurrent copy pipes

**2. Create New Bucket Resources**

For each new bucket number (e.g., 10-19 for 20-bucket system):

a. **Create MV pipe**: `activityRelations_bucket_MV_{N}.pipe`
   ```sql
   SELECT * FROM activityRelations
   WHERE cityHash64(segmentId) % 20 = {N}
   ```

b. **Create raw bucket datasource**: `activityRelations_bucket_MV_ds_{N}.datasource`
   - Use same schema as existing buckets (0-9)
   - ReplacingMergeTree with ENGINE_VER "updatedAt"

c. **Create enrichment copy pipe**: `activityRelations_bucket_clean_enrich_copy_pipe_{N}.pipe`
   - Copy structure from existing enrichment pipes
   - Assign staggered schedule (continue the pattern)
   - Update to reference new bucket numbers

d. **Create cleaned bucket datasource**: `activityRelations_deduplicated_cleaned_bucket_{N}_ds.datasource`
   - Use same schema as existing cleaned buckets
   - MergeTree engine with same sorting key

**3. Update Existing Buckets**

Modify all existing bucket MVs (0-9) to use the new modulo:
```sql
-- Old: WHERE cityHash64(segmentId) % 10 = 0
-- New: WHERE cityHash64(segmentId) % 20 = 0
```

**4. Update Query Layer**

a. **Update union pipe**: Add new buckets to the UNION ALL chain:
   ```sql
   SELECT * FROM activityRelations_deduplicated_cleaned_bucket_0_ds
   UNION ALL
   ...
   UNION ALL
   SELECT * FROM activityRelations_deduplicated_cleaned_bucket_19_ds
   ```

b. **Update routing pipe**: No changes needed (works with any bucketId)

c. **Update project_buckets pipe**: Update modulo calculation:
   ```sql
   SELECT segmentId, cityHash64(segmentId) % 20 as bucketId
   ```

**5. Bootstrap New Buckets**

Create and run snapshot pipes for new buckets (10-19):
```sql
SELECT * FROM activityRelations
WHERE cityHash64(segmentId) % 20 = {N}
```

**6. Migration Strategy**

**Option A: Clean cutover (requires downtime)**
1. Stop data ingestion temporarily
2. Deploy all new bucket configurations
3. Run all snapshot pipes (0-19) to repopulate
4. Run all enrichment pipes
5. Resume data ingestion

**Option B: Gradual migration (no downtime)**
1. Deploy new buckets (10-19) alongside existing ones
2. Update MVs to use new modulo (this redistributes data)
3. Let MVs accumulate new data naturally
4. Run snapshot pipes for backfill
5. Verify data completeness before removing old buckets

**7. Verification**

After migration, verify bucket distribution:
```sql
SELECT
  0 as bucket, COUNT(*) as count FROM activityRelations_bucket_MV_ds_0
UNION ALL
SELECT 1, COUNT(*) FROM activityRelations_bucket_MV_ds_1
-- ... for all buckets
```

Each bucket should have approximately `total_rows / bucket_count` records.

**8. Update Documentation**

Update the documentation to reflect the new number of buckets.