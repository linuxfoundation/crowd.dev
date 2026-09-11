# ADR-0025: Subproject member merge suggestions

**Date**: 2026-09-11
**Status**: accepted
**Deciders**: Yeganathan S

## Context

When a project is onboarded, Git and GitHub often produce two member profiles for the same contributor. The global merge-suggestions pipeline is deliberately conservative: it searches across the whole tenant, so weak signals like a shared display name or a matching handle cannot justify a high score. As a result, obvious onboarding splits either never surface or sit below the merge-suggestion threshold indefinitely.

Inside a single recently onboarded project, the same weak signals are much stronger evidence. The candidate set is small, and two profiles on the same repo sharing a handle or an inverted name is usually the same person. The project dashboard is also where these duplicates are most visible, right after onboarding.

## Decision

Add a daily, subproject-scoped merge-suggestions generator for recently onboarded Insights projects. For each eligible subproject (an Insights project's segment, with integrations finished and member aggregates populated), Postgres finds plausible duplicate pairs using project-scoped signals — shared display name, reversed name tokens, display name matching another member's username, or an email local-part matching another member's username. When a key matches more than two members, the busiest member is the primary and the others pair with it (a star, not a clique).

Application code then classifies which rules matched each pair and assigns a similarity score, trusting complementary git/GitHub splits more than name-only matches. Suggestions are written through the existing merge-suggestion upsert, which overwrites any lower global score for the same pair, and flow through the existing LLM merge review. The global scorer and its OpenSearch query are not changed.

## Alternatives Considered

### Alternative 1: Relax the global scorer for display-name and handle matches
- **Pros**: No new pipeline; reuses the existing search and scoring.
- **Cons**: Those signals are meaningless at tenant scale; raising their weight globally would flood suggestions with false positives.
- **Why not**: The signal is only trustworthy inside a small, closed set. Scope is what makes it safe.

### Alternative 2: A cron job instead of Temporal
- **Pros**: Simpler; no workflow machinery for what is mostly a query plus an upsert.
- **Cons**: One slow or failing project shares fate with the rest; retrying a failed write re-runs the query; no per-project history.
- **Why not**: One workflow per subproject gives isolation, retries, and observability, and keeps merge-suggestion jobs in one worker instead of splitting them across deploys.

### Alternative 3: Score entirely in SQL
- **Pros**: One round trip; no application-layer scorer.
- **Cons**: Rule classification, star pairing, and similarity weighting are policy; embedding them in SQL makes them hard to read, test, and adjust.
- **Why not**: Postgres is a data source — it finds candidate pairs cheaply with the right indexes. The code layer owns the decision logic.

### Alternative 4: Load all subproject members into the workflow and match in code
- **Pros**: Simplest possible query.
- **Cons**: Large projects put tens of thousands of members into workflow history, hitting Temporal payload limits and re-paying that cost on every replay.
- **Why not**: Only the small set of candidate pairs needs to cross the activity boundary.

## Consequences

### Positive
- Onboarding duplicates become merge suggestions within a day instead of never surfacing.
- Global matching behavior is unchanged; the risk is contained to the project scope.
- Per-subproject workflows isolate failures and make "why did this project not produce suggestions" answerable from history.
- Existing merge-suggestion storage and LLM review are reused, so there is no new downstream flow to operate.

### Negative
- Another scheduled workload and a second, project-scoped scoring policy to keep in sync with the global one.
- The generator intentionally overwrites lower global scores for the same pair, so the two scorers can disagree and the project-scoped one wins within its window.

### Risks
- Name-based matches can still pair two different people with the same name. Mitigated by scoping to one project, skipping placeholder names, and routing everything through the existing LLM review rather than auto-merging.
- Eligibility depends on integrations being done and aggregates being populated; a project whose data lands late may be missed until the window logic is revisited.
