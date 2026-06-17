# ADR-0003: Use DependencyGraphEdgesLatest for deps ingestion; defer DependenciesLatest until NUGET or GO needed

**Date**: 2026-05-29
**Status**: accepted
**Deciders**: Uroš Marolt

## Context
The deps.dev BigQuery public dataset exposes two tables for direct package
dependencies: `DependencyGraphEdgesLatest` (Option A) and `DependenciesLatest`
(Option B). A cost analysis was done comparing both for a full bootstrap and
weekly incremental runs across all 6 ecosystems. The initial import scope is
NPM + MAVEN only.

## Decision
Use `DependencyGraphEdgesLatest` (Option A) with `From.Name = Name AND
From.Version = Version` as the depth-1 filter. Switch to `DependenciesLatest`
(Option B) only when NUGET or GO ingestion is required, since Option A does not
support either ecosystem. At that point evaluate whether to migrate all ecosystems
or add GO/NUGET via Option B only.

## Alternatives Considered

### Option B — DependenciesLatest + MinimumDepth=1
- **Pros**: 65% cheaper full bootstrap (~$494 vs ~$1,291 for NPM+MAVEN), 73%
  cheaper weekly runs (~$7 vs ~$26), covers all 6 ecosystems including NUGET
- **Cons**: loses `version_constraint` field (the raw declared semver range
  e.g. `^1.2.3`); only exposes resolved version
- **Why not**: NPM and MAVEN are the only ecosystems needed now. Option A
  retains `version_constraint` which has security feature value. Cost delta is
  acceptable for the current scope. Re-evaluate when NUGET or GO is required.

## Consequences

### Positive
- Retains `version_constraint` (declared semver range) for future security features
- No migration risk — simpler to stay on the table already coded

### Negative
- NUGET and GO not supported; must revisit when either ecosystem is added.
  Confirmed empirically: full bootstrap of GO against `DependencyGraphEdgesLatest`
  returns 0 rows — deps.dev does not resolve GO dependency graphs in that table
  (GO uses Minimal Version Selection, a different resolution model from NPM/MAVEN/PYPI/CARGO).
- Full bootstrap ~$1,291 vs ~$494 for NPM+MAVEN (Option B cheaper)
- Weekly incremental ~$26 vs ~$7 (Option B cheaper)

### Risks
- If NUGET or GO is added before a re-evaluation, the team may miss that Option B
  is required. Mitigation: this ADR and the note in
  `personal/osspckgs-bq-cost-report.txt` flag the trigger condition.
