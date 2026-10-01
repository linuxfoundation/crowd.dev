# ADR-0032: Explicit `collections.type` discriminator

**Date**: 2026-09-29
**Status**: proposed
**Deciders**: Joana Maia

## Context

Collections are either curated (created by CDP admins) or community (owned by an Insights SSO user). There is no explicit type: "curated" is inferred from `ssoUserId IS NULL` in Tinybird pipes (`collections_oss_index`, `collections_filtered`, `categories_oss_index`, `category_groups_oss_index`, `collection_buckets`, `activityRelations_collection_bucket_MV_*`, and search via `collections_filtered`) and in the Insights repo. Open Source stacks introduce a third kind, owned by an organization, with no `ssoUserId`. Under the current inference every stack row would be treated as curated and leak into curated surfaces.

## Decision

We add `collections.type` (`curated | community | stack`), backfilled from `ssoUserId`, and switch every curated/community check in Tinybird and Insights to `type` before the first stack row is written. New collection kinds extend the enum instead of adding nullable-column inference.

## Alternatives Considered

### Alternative 1: Keep inference, add a stack-specific exclusion

- **Pros**: no backfill; smaller diff.
- **Cons**: every curated filter becomes `isNull(ssoUserId) AND isNull(ownerAccountId)`; each new kind adds another clause; a missed filter silently leaks rows.
- **Why not**: the failure mode is silent and grows with each new kind.

### Alternative 2: Separate `stacks` table

- **Pros**: no change to existing collection queries.
- **Cons**: duplicates collection pages, search, project links and Tinybird replication for a near-identical entity.
- **Why not**: a stack is a collection with an org owner; splitting it doubles the read path.

## Consequences

### Positive

- Collection kind is explicit and indexable; filters are self-describing.
- Adding a future kind is an enum value plus the surfaces that should show it.

### Negative

- One-off backfill migration and a coordinated change across several Tinybird pipes and the Insights repo.
- Tinybird `collections` datasource schema change.

### Risks

- A stack row written before every filter is migrated leaks into curated surfaces. Mitigated by shipping the `type` column and filter swap first, and gating the stack write API behind that release.
- Backfill mismatch between Postgres and Tinybird. Mitigated by replicating `type` via CDC and verifying counts per type in both stores before enabling stacks.
