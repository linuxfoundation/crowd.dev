# ADR-0033: Connector emit() takes per-call segment overrides

**Date**: 2026-09-30
**Status**: accepted
**Deciders**: Uros Marolt

## Context

Octolens exposes one org-wide REST API with a 500 requests/hour budget shared across thousands of LFX projects, so its connector has to run a single global poll rather than one sync unit per project. A polled mention lists the Octolens keywords it matched (`keywords[]`), not a project, so mentions from one poll belong to many different CDP segments. The connector framework's `emit()` fixed `segmentId` and `integrationId` for the whole sync run, which only fits connectors with one segment per sync unit.

## Decision

`createEmit()` accepts optional `segmentId` and `integrationId` overrides on each `emit()` call (both or neither); without them it falls back to the sync unit's integration segment, as before. A connector whose Manifest sets `emitsOwnSegment` must pass both overrides on every call; its sync unit can then live on an integration with no segment, which is how Octolens runs one global poll on a segment-less anchor integration and emits each mention under its subproject's own integration.

## Alternatives Considered

### One sync unit per project sharing the HTTP call

- **Pros**: no framework change.
- **Cons**: per-project units would each poll the same org-wide feed, or need a separate coordination layer to share one response.
- **Why not**: it moves the coordination problem elsewhere instead of solving it, against a budget that can't absorb per-project polling.

### A new emitter per mapped segment

- **Pros**: no change to the `emit()` signature.
- **Cons**: allocates an emitter per record or segment and breaks the per-run instrumentation (`emittedCount`, run summaries) that assumes one emitter per run.
- **Why not**: more expensive and more invasive than a small, backward-compatible extension.

## Consequences

### Positive

- A single poll can fan out to any number of segments.
- Backward compatible: existing connectors (GitHub) call `emit()` without overrides and are unaffected.

### Negative

- Overrides apply per `emit()` call, not per record, so a connector has to group records by segment before emitting.

### Risks

- A wrong keyword→segment mapping routes activities to the wrong project; correctness depends on the mapping data.
- Monitoring of per-segment emit volume was deferred and isn't in place.
