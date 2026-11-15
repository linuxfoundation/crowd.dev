# ADR-0039: Keep activityRelations as a single unpartitioned table for now

**Date**: 2026-11-15
**Status**: accepted
**Deciders**: Uroš Marolt, CDP team

## Context

`activityRelations` in Postgres holds about 1.02B rows (293 GB heap, 448 GB indexes). Epic linuxfoundation/lfx-cdp-insights-ops#821 asked for a storage approach that keeps ingestion and queries scaling, with partitioning or archiving as candidates. Much of the load turned out to come from duplicate data and never-run maintenance, not the size itself: 98.8M duplicate NULL-channel rows (linuxfoundation/lfx-cdp-insights-ops#852), now prevented by a partial unique index (linuxfoundation/lfx-cdp-insights-ops#1114), and a table that had never been vacuumed (174M dead row versions). data-sink-worker's per-batch work is also much cheaper after the epic's speedups (linuxfoundation/lfx-cdp-insights-ops#916, linuxfoundation/lfx-cdp-insights-ops#917, linuxfoundation/lfx-cdp-insights-ops#919, linuxfoundation/lfx-cdp-insights-ops#959, linuxfoundation/lfx-cdp-insights-ops#969 and linuxfoundation/lfx-cdp-insights-ops#971). Since March 2026 the table has grown by about 12–41M rows a month (about 25M on average), roughly 300M rows and 210 GB including indexes a year.

## Decision

Keep `activityRelations` as one unpartitioned table and don't start a partitioning or archiving migration now. Revisit when the growth or the measured query and ingestion latency changes materially (see Risks).

## Alternatives Considered

### Alternative 1: Partition by time (activity `timestamp` or `createdAt`)

- **Pros**: smaller per-partition indexes and cheaper vacuum per partition; old partitions could be detached or moved to cheaper storage.
- **Cons**: the `activityId` primary key would have to include the partition column, so `activityId` alone would no longer be enforced unique; partitioning by `createdAt` would also need it added to both dedup unique indexes (`ix_unique_activity_relations_dedup_key` and the NULL-channel partial index), which already contain `timestamp`; lookups by member, organization or segment span every partition; the migration of a 740 GB live table needs a dual-write or copy-and-switch period, with Sequin, Snowflake and Tinybird depending on the table.
- **Why not**: the measured problems were solved without it, and the migration risk and effort are high for no current gain.

### Alternative 2: Partition or shard by segment

- **Pros**: matches the most common filter (project/segment) and Tinybird's segment bucketing.
- **Cons**: very uneven segments (a few projects hold most rows); member- and organization-wide queries and the affiliation refresh span all partitions; the dedup indexes already contain `segmentId`, but the `activityId` primary key would still need it, and the migration issues are the same as above.
- **Why not**: skewed data and cross-segment access patterns remove most of the benefit.

### Alternative 3: Archive old rows to cold storage

- **Pros**: keeps the hot table small.
- **Cons**: relations are still read and rewritten for old activities (member merges, affiliation refresh, re-sends), and Tinybird and Snowflake are fed from this table's change stream.
- **Why not**: no clear cold subset exists today.

## Consequences

### Positive

- No migration risk, and no change for Sequin, Snowflake, Tinybird or any reader.
- The space freed by the November 2026 `VACUUM` (about 174M row versions) is reused by new rows, so the files grow slowly for months.

### Negative

- Index sizes keep growing with the table (the dedup key index alone is 214 GB), and any future change to a large index needs a long concurrent build.
- Vacuum and index maintenance stay whole-table operations.

### Risks

- **Autovacuum falling behind again** on a table this size (it had never run on it before): watch dead tuples and transaction ID age, and run a manual `VACUUM` with a large `maintenance_work_mem` before a forced anti-wraparound vacuum.
- **Growth speeding up** (new integrations, large onboardings): revisit this decision if growth stays above about 50M rows a month or if data-sink-worker or query latency on the table regresses.
- **Unused or redundant indexes** add write cost on every insert (for example the 76 GB `updatedAt` index tracked in linuxfoundation/lfx-cdp-insights-ops#926): review them before considering partitioning.
