# ADR-0034: Salesforce data from Snowflake reaches Insights through CDP

**Date**: 2026-10-01
**Status**: proposed
**Deciders**: Joana Maia

## Context

The [2024-10 Insights target architecture](https://github.com/linuxfoundation/lfx-architecture-scratch/tree/main/2024-10%20Insights%20Architecture#target-architecture) planned a Snowflake → Tinybird connector for data that lives in Snowflake but not in CDP, with Salesforce organization data as the main case. That connector was never built. CDP now needs the same Salesforce data itself: the CDP org ↔ Salesforce account mapping for Open Source Stacks and org slugs (ADR-0033), and LF memberships, which today are loaded by hand from a CSV (`import-lfx-memberships.ts`). CDP already pulls Snowflake data with Temporal workers that export to S3 and consume the files (`pcc_sync_worker`, `snowflake_connectors`), and Sequin already replicates CDP Postgres tables to Tinybird.

## Decision

Salesforce data from Snowflake is ingested into CDP Postgres by a Temporal worker following the [approved `pcc_sync_worker` pattern](https://docs.google.com/document/d/1t6HyZdHGM9TA47fyJ5jRQ96z4O5esmnCDu03yNS4lwI/edit?tab=t.0#heading=h.n2bz17vu9svn), and reaches Tinybird through Sequin like any other CDP table. This replaces the planned Snowflake → Tinybird connector for organization data. A direct connector stays possible for future data that CDP has no use for.

## Alternatives Considered

### Alternative 1: Snowflake → Tinybird connector, as in the 2024-10 target architecture

- **Pros**: no CDP tables; Tinybird reads warehouse models directly.
- **Cons**: CDP still needs the same data for the memberships import, org merges and the stacks API, so it would be loaded twice through two pipelines; the merge workflow could not repoint mappings in Tinybird.
- **Why not**: CDP is a consumer of this data, not only Insights.

### Alternative 2: Both pipelines, Snowflake → CDP and Snowflake → Tinybird

- **Pros**: Tinybird does not depend on CDP for Salesforce data.
- **Cons**: two copies that can disagree, two schedules, and CDP-side fixes (merges, manual overrides) never reach Tinybird.
- **Why not**: Sequin already gives Tinybird a copy of CDP data with seconds of lag.

### Alternative 3: Extend `snowflake_connectors`

- **Pros**: one worker for every Snowflake source.
- **Cons**: its transformers produce activities (`TransformerBase` returns `IActivityData`); accounts and memberships are entities that need upserts.
- **Why not**: `pcc_sync_worker` already handles entity upserts from a Snowflake export.

## Consequences

### Positive

- One copy of Salesforce data, shared by CDP and Insights.
- The LF memberships import can be automated on the same worker instead of a manual CSV run.
- CDP-side changes such as org merges show up in Tinybird through the existing replication.

### Negative

- A new Temporal worker, or new workflows, to run and monitor.
- Every new Salesforce field Insights needs is a CDP migration plus a Sequin publication change, not only a Tinybird datasource.
- Insights depends on CDP Postgres being up to date for Salesforce data.

### Risks

- The warehouse export fails and CDP keeps stale data. Mitigated by `syncedAt` on each row and an alert when the last successful sync is older than the schedule allows.
- LF memberships move from a manual import to a sync, which changes their key from `accountName` to the Salesforce account and project IDs. Mitigated by matching existing rows on account name once and checking per-project counts against the warehouse before switching off the CSV import.
