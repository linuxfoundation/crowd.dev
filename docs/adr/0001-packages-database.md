# ADR-0001: Separate physical database for the packages domain

**Date**: 2026-05-25
**Status**: accepted
**Deciders**: CDP/Insights team

## Context

The packages domain (tracking open-source packages, dependency graphs, repositories, security advisories, and maintainers) is being built as a new capability inside the CDP platform. The main CDP database (`crowd-web`) and the existing `product-db` already exist as separate physical Postgres instances, each owned by a distinct domain. The packages schema has no foreign-key relationships into either existing database, and requires partitioned tables sized for 90M+ versions and 1.15B+ dependency rows — a scale profile that would create resource contention if mixed with CDP's community-activity tables.

## Decision

We store all packages-domain data in a dedicated physical Postgres instance (`packages-db`, port 5434) with its own Flyway migration path (`backend/src/osspckgs/migrations/`), following the same Dockerfile and migration-script pattern used by `product-db`. Schema and connection code live entirely within the `packages_worker` service.

## Alternatives Considered

### Alternative 1: Add tables to the main CDP db (`crowd-web`)

- **Pros**: No new database to manage; existing Flyway setup and pg-promise helpers already target it.
- **Cons**: Packages tables have a completely different shape and scale from community-activity tables. Resource contention at scale is a real risk, and schema coupling makes independent evolution harder.
- **Why not**: The packages schema has zero FK dependencies on CDP tables. Co-locating independent domains in one database couples their lifecycle, backup strategy, and performance headroom for no benefit.

## Consequences

### Positive

- Clear domain boundary: packages-db has no FK relationships outside its own schema.
- Independent scaling, backup, and maintenance.
- Follows existing precedent; `product-db` demonstrates the pattern works.
- Schema decisions (partitioning, GDPR) are isolated to one database and one migration path.

### Negative

- A third Postgres instance to operate, monitor, and back up.
- Read/write host split is prepared in env vars (`CROWD_PACKAGES_DB_READ_HOST` / `CROWD_PACKAGES_DB_WRITE_HOST`) but only the write host is wired in `config.ts` today — read routing is deferred.

---

**Partitioning rationale (captured here to avoid re-litigating per-table):**

| Table | Strategy | Buckets | Hot query shape |
|---|---|---|---|
| `versions` | HASH(`package_id`) | 32 | Lookup by package — lands in one partition; ~2.8M rows each at 90M total |
| `package_dependencies` | HASH(`depends_on_id`) | 64 | "Who depends on vulnerable package X?" — lands in one partition; ~18M rows each at 1.15B total |
| `downloads_daily` | RANGE(`date`) via `pg_partman` | automatic | Time-series; pruning old partitions is straightforward |
