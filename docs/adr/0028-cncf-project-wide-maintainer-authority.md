# ADR-0028: CNCF `.project` Repo as Project-Wide Maintainer Authority

**Date**: 2026-09-16
**Status**: accepted
**Deciders**: Joana Maia

## Context

ADR-0023 established that `maintainers.yaml` in a CNCF `.project` repo is authoritative for that repo's own maintainer detection. However, `MaintainerService` runs per-repository with no project-level awareness: every other repo in the same CNCF project (e.g. `cri-o/cri-o`, `cri-o/cri-o-tests`) still runs independent file detection, scoring, and LLM extraction against their own `MAINTAINERS`, `CODEOWNERS`, and `README` files. This produces significant over-collection — audits showed 57–322× excess maintainer entries across sibling repos — and 390 emeritus-classified identities incorrectly marked as active maintainers because the LLM extraction prompt only allowed `"maintainer"` or `"contributor"` as normalized roles, with no emeritus bucket.

## Decision

For CNCF projects (repos whose segment has `grandparentSlug = 'cncf'`), treat the `.project` sibling repo's `maintainers.yaml` as the sole authoritative source for the entire project:

1. **Skip gate**: When processing any non-`.project` repo that has a `.project` sibling (determined by querying sibling repos via `segments.parentId` under the CNCF projectGroup), raise `MaintainerSkippedProjectLevelError` — no file detection, no LLM calls, no writes to `maintainersInternal`.
2. **Sibling end-date**: After a `.project` repo successfully saves its maintainer roster, bulk-update `maintainersInternal` to set `endDate` on all active rows belonging to sibling repos in the same project.
3. **Emeritus extraction**: Add `"emeritus"` as a third valid `normalized_title` in the LLM prompt and model. The LLM uses `"emeritus"` for any person explicitly marked as retired, inactive, or alumni (e.g. "Emeritus Maintainer", "Alumni", "Past Maintainer"). Emeritus entries are stored in `maintainersInternal` with `role = 'emeritus'` — they are never counted as active maintainers since all downstream consumers filter on `role = 'maintainer'`.
4. **Case-insensitive handle lookup**: `find_github_identity` uses `LOWER(value) = LOWER($1)` when querying `memberIdentities`. GitHub handles are case-insensitive; CDP sometimes stores the lowercase variant as the non-deleted identity (e.g. `thor-wl` active, `Thor-wl` deleted). A case-sensitive match silently misses these, leaving a maintainer with no `identityId` row in `maintainersInternal`.

## Alternatives Considered

### Alternative 1: Fan out `.project` maintainers to every sibling repo
- **Pros**: Preserves per-repo query semantics — consumers can still look up maintainers by any repo URL.
- **Cons**: Bloats `maintainersInternal`; requires maintaining the sibling set on every `.project` run; adds complexity to the upsert logic.
- **Why not**: Downstream read by sibling URL is not a current consumer requirement; keeping rows under the `.project` repo is simpler and avoids table bloat.

### Alternative 2: Project-level segment as grouping key instead of sibling URL pattern
- **Pros**: Cleaner model — no URL pattern matching.
- **Cons**: Requires a new `maintainersInternal` FK to segment; larger schema change; segment hierarchy is stable but `.project` URL convention is already established.
- **Why not**: URL-pattern detection (`repo_name == ".project"`) is already in place from ADR-0023; extending it is lower risk than a schema change.

### Alternative 3: One-time backfill to end-date stale sibling rows
- **Pros**: No runtime change to processing loop.
- **Cons**: Doesn't prevent re-population — sibling repos would accumulate new rows again on their next processing cycle.
- **Why not**: Same failure mode as ADR-0023 alternative 2; the problem recurs without a gate in the processing path.

## Consequences

### Positive
- Eliminates over-collection from sibling repos for CNCF projects; one authoritative source per project.
- Emeritus roles stored with `role = 'emeritus'` — visible for historical queries, excluded from active maintainer counts by all downstream consumers.
- Skip gate is cheap: one DB query per repo per cycle, no LLM cost for skipped repos.
- Case-insensitive handle lookup recovers maintainers whose `memberIdentities` row was stored under a different casing than the handle in `maintainers.yaml`.

### Negative
- Per-repo maintainer queries against sibling repo URLs return empty; consumers that need project-level maintainers must query via the `.project` repo. The `mv_maintainer_roles` MV does not yet filter `endDate IS NULL` (tracked separately), so sibling rows remain visible downstream until that is addressed.
- Adds a second CNCF-specific branch to `MaintainerService`, extending the surface area introduced in ADR-0023.

### Risks
- Transient gap on first onboarding: if a sibling repo is processed before the `.project` repo has run, the sibling is skipped immediately (no data collected), leaving the project with no maintainers until `.project` processes. Accepted; resolves on the next cycle.
- If the `.project` repo is removed or fails to parse on a run, sibling repos remain permanently skipped until the `.project` repo is re-onboarded or removed from the system. Monitor via `MAINTAINER_SKIPPED_PROJECT_LEVEL_SOURCE` error codes in service execution logs.
