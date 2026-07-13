# ADR-0006: Database schema types as the source of truth

**Date**: 2026-07-09
**Status**: accepted
**Deciders**: Yeganathan S

## Context

Working with entity types in CDP is messy. The same concepts appear as overlapping shapes across `@crowd/types`, DAL inputs, merge helpers, and API/domain models — often incomplete relative to the database, with inconsistent nullability and no single source of truth. That makes it hard to change data access safely, reuse types across packages, and keep tests aligned with production.

We need a durable way to model persisted data that the rest of the codebase can build on, without inventing yet another parallel type family per feature.

## Decision

Treat the database schema as the source of truth for persisted entity shapes. Introduce a dedicated `db/` area under `@crowd/types` for schema-aligned row types, and derive narrower types (create/update payloads, factory options, domain views) from those via composition (`Pick`, `Partial`, `Omit`, intersections) rather than hand-maintaining duplicates.

These types live in `@crowd/types`, not in the data-access layer: they are shared vocabulary for backend, workers, libraries, and test utilities. DAL remains responsible for queries and persistence; it consumes and returns these types instead of owning the canonical shapes.

This applies to entities generally (members, identities, organizations, and others as we touch them), not only the first tables we migrate.

## Conventions

These rules exist so people (and tools) do not confuse the row type with write payloads, or nullability with “field may be omitted.”

1. **One row type per table** — describes the entity as stored. Field nullability matches the schema. Database defaults do not make read fields optional.
2. **Write payloads are derived from the row type** — use `Pick`, `Partial`, `Omit`, and intersections instead of duplicating the full interface.
3. **Nullability ≠ optionality** — `| null` means the column can store null; `?` / `Partial` means the caller may omit the key. Both can appear; they mean different things.
4. **System-owned fields stay off write payloads** — things like tenant id, audit timestamps, and soft-delete markers are set by persistence code, not by every caller.
5. **Layers that supply defaults may widen further** — e.g. a test factory can take a partial write payload and fill required fields before calling DAL. That does not change the row type or the DAL contract.

## Alternatives Considered

### Alternative 1: Keep defining types ad hoc next to each feature
- **Pros**: Fast locally; no cross-package coordination.
- **Cons**: Duplicates and drift continue; nullability and field sets disagree across call sites.
- **Why not**: That is the current pain. It does not scale as more packages share the same entities.

### Alternative 2: Own canonical entity types inside `@crowd/data-access-layer`
- **Pros**: Types sit next to SQL; easy for DAL authors to update.
- **Cons**: Anything that needs a row shape (test-kit, common services, workers, backend) must depend on DAL or re-copy types.
- **Why not**: Row shapes are shared contracts, not DAL implementation details. Putting them in `@crowd/types` keeps the dependency graph thin and matches how other shared models already live.

### Alternative 3: Generate types from the schema (e.g. introspection tooling)
- **Pros**: Always in sync with migrations; less manual work.
- **Cons**: Tooling and review process are not in place; generated output can be noisy and hard to evolve with domain naming.
- **Why not**: Manual schema-aligned types are enough to establish the pattern now. Generation can be revisited once the convention is stable.

## Consequences

### Positive
- One place to look for “what does this table look like in TypeScript.”
- Downstream APIs and tests extend from the same base instead of inventing parallel models.
- Clearer package boundaries: `@crowd/types` for shapes, DAL for access, factories/services for composition.

### Negative
- Existing aliases and legacy shapes (`MemberRow`, feature-local inputs, etc.) will coexist during migration.
- Authors must update `db/` types when migrations change columns or nullability.

### Risks
- **Partial adoption** — mitigated by migrating types when a table is touched (factories, DAL hardening, new features), not a big-bang rewrite.
- **Drift from schema** — mitigated by checking live schema (or migrations) when adding or changing `db/` types; optional codegen later.
