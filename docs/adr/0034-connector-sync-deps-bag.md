# ADR-0034: Connector syncs get database access through a typed per-connector deps bag

**Date**: 2026-10-13
**Status**: accepted
**Deciders**: Uros Marolt

## Context

Some connector syncs need database reads and writes that aren't activity emits: the Octolens mention poll resolves each mention's keyword to a segment and integration. (Octolens mapping discovery was first planned as a second sync here; it shipped as a separate connectors_worker Temporal schedule and does not use deps.) `SyncContext` had no database access, so the mention poll got an Octolens-specific `resolveSegmentId` closure added directly to the generic `SyncContext` that every connector sees (now replaced by `deps.resolveKeywordTarget`). Adding more connector-specific hooks there, or handing every sync a raw query executor, would both spread one connector's needs into the shared framework.

## Decision

A `Manifest` can declare `createDeps({ qx, integrationId })`, which returns a typed, connector-specific deps object; the worker builds it once per sync run and passes it to the sync as `ctx.deps`. `SyncContext`, `SyncDefinition` and `Manifest` are generic over the deps type, and a manifest whose syncs need deps must provide `createDeps` (enforced by the type).

## Alternatives Considered

### Raw query executor on `SyncContext`

- **Pros**: a single generic field; the simplest change.
- **Cons**: every connector's sync gets unrestricted database access, and data-access calls spread through sync code.
- **Why not**: it trades a small typing cost for an unbounded surface in every connector.

### Narrow closures on `SyncContext`

- **Pros**: consistent with the `resolveSegmentId` closure that already shipped.
- **Cons**: the generic context accumulates hooks that only one connector uses, and every connector sees them.
- **Why not**: it doesn't scale past one connector and blurs what the framework guarantees.

## Consequences

### Positive

- The framework stays generic; each connector's database surface is explicit and typed in one place.
- Per-run caching (e.g. Octolens loading its keyword mappings once per run) lives with the connector, not the worker.

### Negative

- The core framework types gain a generic parameter, and the registry stores a type-erased `AnyManifest`.

### Risks

- Services compile without `strictNullChecks`, so the "factory required when deps are declared" rule relies on a tuple-wrapped conditional type; a type-level test guards it.
- Deps are rebuilt for every run, so connector caches never outlive a run (intended, but a connector that assumes a long-lived cache would be wrong).
