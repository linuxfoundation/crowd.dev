# ADR-0014: Collaboration track record signal

**Date**: 2026-07-28
**Status**: accepted
**Deciders**: Mouad BANI

## Context

Automated security outreach needs to know whether a project historically
engages with external reports before sending it enriched findings. The inputs
(issue response latency, PR merge rate for external contributors, past
advisory response) largely already exist in `repo_activity_snapshot` (written
by the github-repos-enricher sweep) and the OSV advisory tables. The signal
covers repos backing `is_critical` packages (~118k), and lives on the `repos`
table, which is in the `sequin_pub` logical-replication publication
(Tinybird) — so write churn matters.

## Decision

Store two nullable columns on `repos` — `collaboration_score` (int, 0–100) and
`collaboration_tier` (text) — recomputed by a single set-based SQL UPDATE
(`src/enricher/updateCollaborationSignal.ts`) at the end of every enricher
sweep in `runEnrichmentLoop`.

### Formula

```
score = ROUND(100 × (R + M + A) / count(non-null components))
        NULL when all components are null
```

Components are 0–1 or **null when there is no evidence** — no data must never
read as "unresponsive":

| Component | Rule |
| --- | --- |
| R — issue responsiveness | median time to first response (12m): ≤72h → 1.0; ≤336h → 0.5; else 0.0; NULL median with the guard passed means no issue ever got a non-author response → 0.0. Null if `issues_opened_last_12m < 5` |
| M — external PR acceptance | `external_prs_merged_12m / external_prs_opened_12m`. Null if fewer than 3 external PRs |
| A — advisory response | 1.0 if any linked advisory has a `fixed_version`; 0.0 if advisories exist, the oldest is >90 days old, and none is fixed; else null |

Tier is derived from the score in the same statement, precedence first-match:
`inactive` (`archived` or `disabled`, score NULL) → `unknown` (score NULL) →
`responsive` (≥70) → `mixed` (≥40) → `unresponsive`.

Weights are equal (plain average) and documented in weighted form in the spec
(`docs/superpowers/specs/2026-07-27-collaboration-track-record-design.md`) so
retuning is a parameter change once outreach response data exists to calibrate
against.

### External PR collection

- "External" = PR `authorAssociation` ∉ {`OWNER`, `MEMBER`, `COLLABORATOR`};
  a missing/null association (ghost author) counts as external.
- `authorAssociation` is a field on PR nodes the enricher already pages
  through (`PR_PAGE_QUERY`) — no extra HTTP requests or rate-limit cost.
  Counts are computed in `computeExternalPrCounts` (`computeMedians.ts`) and
  stored as `repo_activity_snapshot.external_prs_opened_12m/merged_12m`.
- **NULL external columns (row not yet re-enriched) fall back to overall
  merge rate** (`prs_merged_last_12m / prs_opened_last_12m`, ≥3 guard). Once
  the columns are populated they are authoritative: fewer than 3 external PRs
  means M = null, with no fallback — the overall rate is inflated by
  maintainer self-merges, especially in solo-maintainer repos.

### Advisory proxy

"Advisory acknowledgement" is derived in-DB rather than fetched:
`advisories` → `advisory_packages` (`package_id IS NOT NULL`) →
`package_repos` → repo, with `EXISTS` on `advisory_affected_ranges.fixed_version`
per advisory. A shipped fix is the response evidence; the 90-day grace window
avoids penalizing a fresh CVE still being worked. Fetching repo-published
GitHub security advisories (REST) was deferred.

### Scoring pass

- One statement: CTEs `critical_repos` → `advisory_repos` → `components` →
  `scored` → `final`, then `UPDATE repos ... FROM final`. Repos without a
  `repo_activity_snapshot` row still score via component A alone (LEFT JOINs).
- **`(collaboration_score, collaboration_tier) IS DISTINCT FROM (new values)`
  guard**: unchanged rows are not rewritten, keeping per-sweep recompute from
  generating `sequin_pub` replication churn. For the same reason there is no
  per-row `computed_at` column — it would make every row distinct every run.
  No `components_used` column either; it is derivable.
- Trigger: end of each sweep, after the sweep-complete log and before the
  idle sleep, wrapped in try/catch — a failed pass logs, keeps the previous
  values, and retries next sweep; it can never take the enrichment loop down.
  No Temporal schedule: the inputs only change when a sweep writes them.

### Minimum-sample guards

The ≥5-issues and ≥3-PRs guards are load-bearing: they prevent one lucky
reply or a single merged PR on a tiny repo from producing a confident score.
Prod evaluation showed most guarded-out repos have zero issues and zero PRs
in 12m, so `unknown` is the honest answer, kept as a first-class tier rather
than loosening the guards.

## Consequences

### Positive
- No new API budget, worker, or table: two columns, one SQL statement, one
  existing GraphQL query extended by one field.
- Signal freshness tracks input freshness by construction (at most one sweep
  stale); steady-state UPDATE touches only rows whose tier/score changed.

### Negative
- The majority of critical-package repos are `unknown`/`inactive` with NULL
  score by design — consumers must filter on `collaboration_tier`, never on
  `collaboration_score IS NOT NULL`.
- M runs at degraded fidelity (overall merge rate) for any repo until its
  next enrichment populates the external columns.
- Scoring cadence silently follows the enricher sweep interval config.

### Risks
- Equal weights and the threshold values (72h/336h, 90 days, guards) are
  judgment values, not calibrated ones — revisit against outreach response
  outcomes.
- `collaboration_tier` is free text with values enforced only by the scoring
  SQL; a second writer would need to respect the same vocabulary.
