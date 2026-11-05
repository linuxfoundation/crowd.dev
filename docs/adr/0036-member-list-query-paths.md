# ADR-0036: Member list query picks one of three query shapes

**Date**: 2025-11-19
**Status**: accepted
**Deciders**: CDP team

## Context

The members list API (`queryMembersAdvanced` in `services/libs/data-access-layer/src/members/base.ts`) serves everything from a single-member lookup to the default "most active members of a project" page. It used to build one query shape for every request: start from `members`, join `memberSegmentsAgg`, enrichments and organizations, then sort and paginate. For large project groups that meant sorting millions of rows before applying `LIMIT`, and the planner could not pick a good plan for such different requests from one shape. Measured at the time, CNCF list requests took 45 to 50 seconds and Linux Foundation project requests regularly timed out.

## Decision

`buildQuery` in `services/libs/data-access-layer/src/members/queryBuilder.ts` inspects the filter and sort and emits one of three shapes. A filter that pins `m.id` to one value or an `IN` list of at most 100 IDs (with a segment) starts from `memberSegmentsAgg` for that segment; a request sorted by `activityCount` (the default) with no enrichment (`me.*`) or organization (`mo.*`) filters takes the top N rows of `memberSegmentsAgg` by activity count in a CTE, filtered inside the CTE, and joins back for the selected fields; everything else uses a fallback query from `members` that adds only the joins and CTEs the filter and fields need. The count runs as a separate query and is cached, so page navigation does not recount.

## Alternatives Considered

### One general query, tuned with indexes only

- **Pros**: one code path; every filter and sort behaves the same way.
- **Cons**: the default page still sorts the whole segment's members before `LIMIT`, and the planner has to cover very different requests with one plan.
- **Why not**: this was the shape that timed out; indexes alone did not remove the full sort for the most common request.

### Precompute the sorted member list (materialized view or similar)

- **Pros**: the default page becomes a plain index read.
- **Cons**: another artifact to refresh and keep in sync with merges and new activity; it still has to support arbitrary filters and sorts.
- **Why not**: `memberSegmentsAgg` already holds per-segment activity counts, so an index on it (`idx_msa_segment_activitycount_desc_member` on `("segmentId", "activityCount" DESC, "memberId")`) gives the top-N read without a new pipeline.

## Consequences

### Positive

- The default page reads a bounded slice of `memberSegmentsAgg` through the activity-count index instead of sorting every member. Measured at the time, CNCF requests dropped to about 11 seconds and Linux Foundation project requests to about 15 seconds without timeouts.
- Lookups of a few known members skip the top-N step and stay small.
- The fallback keeps every filter and sort working, joining enrichments and organizations only when the filter or selected fields reference them.

### Negative

- Three query shapes have to return the same rows for the same request, so a change to filtering or joins has to be made in each one.
- Path selection relies on string checks on the filter (`m.id = ...`, `m.id IN (...)`, the `me.` and `mo.` prefixes, other `m.*` fields), so it depends on the aliases the filter parser emits.
- The top-N CTE is capped at 50,000 candidates (10 times `limit + offset` when the filter uses other `m.*` fields). Requests whose page would pass the cap, such as large exports, fall back to the slower full query instead of returning a truncated page.

### Risks

- A new filter alias or a renamed table alias can silently route requests to the wrong path. The fallback still returns correct results, so the likely failure is slowness rather than wrong data, but there is no test that pins which path each request takes.
- The count is cached for up to six hours and depends on member write paths invalidating the member query cache; a write path that skips invalidation leaves a stale total next to fresh rows.
