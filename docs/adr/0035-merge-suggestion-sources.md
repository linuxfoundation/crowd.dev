# ADR-0035: Track where merge suggestions come from

**Date**: 2026-10-20
**Status**: accepted
**Deciders**: Yeganathan S

## Context

Merge suggestions are created by a few independent flows, and each relies on different evidence. Global similarity compares a profile against all other profiles, so it needs to stay conservative. The subproject job (ADR-0025) only looks inside a single project, which lets it trust weaker signals. Identity conflicts start from two profiles holding the same verified identity, found either through the public API or during enrichment.

Because the evidence differs, so does the quality of the suggestions. To know whether a flow, or a change to one, is helping, we need to see how its suggestions end up: merged, rejected, or still waiting for review.

The suggestion tables can't answer this. A suggestion is deleted once it is merged or rejected. The decision itself is still recorded, but nothing records which flow created the suggestion.

## Decision

We record the source of a suggestion when it is created, in a separate table that keeps it after the suggestion is gone. To see how a source performs, we use the existing merge and rejection records.

The source describes which flow found the pair. It doesn't describe how confident the match is or what was decided. A pair keeps its original source even if it is rescored later. If a different flow suggests the same pair, that flow is recorded too, so counts per source can overlap.

LLM review is not a source, because it decides on suggestions that already exist rather than creating new ones.

### Sources

| Source                  | Meaning                                                                                    |
| ----------------------- | ------------------------------------------------------------------------------------------ |
| `global-similarity`     | The member or organization similarity job found the pair by comparing across all profiles. |
| `subproject-similarity` | The subproject job found the pair using signals that only hold inside one project.         |
| `identity-conflict`     | The public API found an identity that another member already has.                          |
| `enrichment-conflict`   | Enrichment found an identity that another profile already has.                             |

`identity-conflict` covers adding or verifying an identity that another member already has, and resolve matching more than one member. Enrichment has its own source because its data comes from third-party providers rather than LFX products, so its suggestions can turn out differently.

When adding a new flow, reuse a source if it means the same thing, or add a new one if the flow should be measured separately. Existing source values shouldn't be renamed or given a new meaning, so we can compare results consistently over time.

## Alternatives Considered

### Alternative 1: A source column on the suggestion tables

- **Pros**: No new table.
- **Cons**: The suggestion is deleted when it is merged or rejected.
- **Why not**: The source would be lost before we can count outcomes.

### Alternative 2: Copy the source onto the merge and rejection records

- **Pros**: The outcome and its source would be stored together.
- **Cons**: Every merge and reject path, now and in the future, would need to copy it.
- **Why not**: A path that forgets to copy it loses the data without any error.

### Alternative 3: Soft-delete suggestions

- **Pros**: The full lifecycle of a suggestion would stay in one place.
- **Cons**: Merges still remove suggestions through cascading deletes, re-suggesting a rejected pair conflicts with its old row, and every read of the suggestions needs an extra filter.
- **Why not**: It's a large change to keep one missing value.

### Alternative 4: A dedicated outcomes table

- **Pros**: The complete history would live in one place.
- **Cons**: It duplicates the merge and rejection records we already store.
- **Why not**: The source is the only thing missing.

## Consequences

### Positive

- Each flow can be measured on real outcomes instead of estimates.
- When two flows find the same pair, both are kept.
- Reviewing and deciding on suggestions works the same way as before.

### Negative

- Suggestions created before this change have no source.
- The table keeps growing, so old rows may need archiving at some point.
- Rejection counts can be a bit lower than reality, because a rejection is removed when the pair is suggested again or a profile is deleted.

### Risks

- A merge linked to a source doesn't always mean the suggestion led to it, since someone may have merged the pair directly.
- Reusing a source for a flow with a different meaning would make reports misleading, so the Sources table should stay up to date.

**Related**: ADR-0025 (subproject merge suggestions), ADR-0027 (noMerge is evidence-bound).
