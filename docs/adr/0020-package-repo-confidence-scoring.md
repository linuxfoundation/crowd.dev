# ADR-0020: Deterministic package→repo confidence scoring

**Date**: 2026-09-01
**Status**: accepted
**Deciders**: Joana Maia

## Context

`package_repos` is the seam every downstream consumer (Akrites, Insights,
criticality, blast radius) reads to answer "which repo does this package come
from". The read side has always taken the highest-confidence link
(`ORDER BY confidence DESC LIMIT 1`), but all nine registry writers hardcoded
`0.8` regardless of evidence, and deps.dev carried its own
`RelationProvenance → confidence` CASE. A squatting package and a legitimate
publisher were therefore indistinguishable, and packages whose links tied on
`0.8` resolved arbitrarily. Repos accumulated inflated `packages_published`
counts, and every impact/health metric derived from those aggregates inherited
the error.

`package_repos` is large, carries `REPLICA IDENTITY FULL`, and is published to
Sequin → Tinybird (`V1781009234`), so both the schema change and the backfill
have a replication cost that has to be planned rather than absorbed.

## Decision

One IMMUTABLE SQL function, `package_repo_confidence(...)`, is the only path
that may produce a `package_repos.confidence` value. Every writer — the eight
registry loops, the deps.dev staging merge, and the enricher rescore — calls
it. `confidence` is widened in place from `numeric(3,2)` to `numeric(12,9)`;
there is no second column.

### Score composition

Base tier by `source` (and `provenance` for deps.dev):

| Source | Base |
| --- | --- |
| `manual` | 0.99 |
| `deps_dev` — SLSA provenance | 0.99 |
| `deps_dev` — rubygems/pypi attestation | 0.95 |
| `deps_dev` — `GO_ORIGIN` | 0.90 |
| `deps_dev` — other | 0.50 |
| `declared` | 0.85 (0.80 for maven) |
| `heuristic` | 0.30 |

Penalties, stacked, floored at 0.05:

- declared-only evidence penalties: `signal='secondary'` −0.10,
  `ownership_match='unmatched'` −0.25 (`'no_evidence'` is a no-op rollout default, penalised only when CM-1394 sets real values)
- repo state: `disabled` → 0.05 + offset, `archived` −0.20, fork −0.10,
  non-GitHub host while a competing GitHub link exists −0.05

Two new columns feed those penalties: `signal` (`primary|secondary`, written by
CM-1393 / [ADR-0021](./0021-secondary-manifest-repository-signal.md)) and
`ownership_match` (`matched|unmatched|no_evidence`, written by CM-1394 /
[ADR-0022](./0022-package-repo-ownership-evidence.md)). Both default to the
no-op value, so this ADR's change is behaviour-neutral on merge. `provenance`
is stored so deps.dev rows can be rescored without re-reading BigQuery.

### Uniqueness offset

`source_priority * 1e6 + repo_id % 1e6`, scaled by `1e-9` — max
`0.003999999`. It sits below the 0.05 tier gap and below both label boundaries
(0.80 high / 0.50 medium), so it can never move a row across a tier or a
label. This is why the column needs nine decimal places: ranking must be
total and reproducible, not arbitrary on ties. Reachable range is `0.05` to
`0.993999999`.

### Write and read policy

- **Same-source refreshes** always replace the stored claim so that updated
  ownership evidence (e.g. `no_evidence` → `unmatched`) and provenance
  downgrades are persisted. **Cross-source conflicts** use keep-highest:
  `confidence = GREATEST(EXCLUDED.confidence, package_repos.confidence)`, and
  the descriptive columns only move when the incoming score wins. A weaker
  source (routine registry refresh) cannot overwrite a stronger one (manual,
  attested deps.dev).
- All read paths use one shared ordering fragment (`bestRepoLinkOrderBy` /
  `BEST_REPO_LINK_JOIN` in `osspckgs/sqlFragments.ts`), replacing five inline
  copies with divergent tie-breakers. The fragment keeps
  `ROUND(pr2.confidence, 2)` so API payloads stay at two decimals.
- Repo-state penalties depend on columns that are NULL until the enricher
  runs, so writers score with what exists and the enricher rescores that
  repo's links (`rescorePackageReposForRepos`) when `archived` / `disabled` /
  `is_fork` flip.

### Migration and backfill

`ALTER COLUMN confidence TYPE numeric(12,9)` rewrites the table under
`ACCESS EXCLUSIVE`. Run with the Sequin and Tinybird sinks paused. Existing
rows keep their values until `rescore_package_repo_confidence()` backfills
them: keyset-paged, `COMMIT` per chunk, guarded by a session advisory lock, run
out-of-band via `scripts/rescorePackageRepos.ts` (which also reports the
no-ties invariant).

## Consequences

### Positive

- One formula, one place to change it; retuning a tier is a migration, not a
  sweep through nine writers.
- Ranking is total and reproducible — `ORDER BY confidence DESC LIMIT 1` has
  exactly one answer per package.
- Evidence quality is now expressible, which is what makes CM-1393 and
  CM-1394 possible without either PR hardcoding a confidence it does not own.
- Keep-highest everywhere means re-running any loop is idempotent and cannot
  downgrade a link.

### Negative

- The scoring logic lives in SQL, so it is tested through an integration test
  against packages-db rather than a pure unit test.
- Changing a tier or penalty requires a migration plus a full rescore.
- `numeric(12,9)` values are not human-readable at a glance; the two-decimal
  rounding lives in the read fragment.

### Risks

- **The `ALTER TYPE` rewrite blocks writes and produces a large WAL burst.**
  Mitigation: run in a maintenance window with the Sequin and Tinybird sinks
  paused; the migration header states this.
- **The backfill touches every row of a `REPLICA IDENTITY FULL` table.**
  Mitigation: chunked procedure with a commit per chunk and a tunable chunk
  size, run out-of-band rather than inside the migration.
- **Scores shift for existing links, so a package's best repo can change.**
  Mitigation: the offset is bounded below every tier and label boundary, so
  only genuinely competing links can reorder; spot-check
  `GET /v1/packages/{purl}` against a set of unambiguous packages before the
  backfill is declared done.
