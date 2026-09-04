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

- repo state: `disabled` collapses the score into a `[0.05, 0.054]` band that
  preserves the source ordering, `archived` −0.20, fork −0.10, non-GitHub host
  while a competing GitHub link exists −0.05

One new column feeds the tiers: `provenance`, stored so deps.dev rows can be
rescored without re-reading BigQuery. Evidence-quality penalties are out of
scope here and arrive on top of this function: the secondary-manifest signal in
CM-1393 / [ADR-0021](./0021-secondary-manifest-repository-signal.md), and
ownership evidence in CM-1394 /
[ADR-0022](./0022-package-repo-ownership-evidence.md). Each adds its own column
and its own penalty branch; because every writer already routes through
`package_repo_confidence`, neither needs to touch a call site.

### Uniqueness offset

`source_priority * 1e6 + repo_id % 1e6`, scaled by `1e-9` — max
`0.003999999`. It sits below the 0.05 tier gap and below both label boundaries
(0.80 high / 0.50 medium), so it can never move a row across a tier or a
label. This is why the column needs nine decimal places. Reachable range is
`0.05` to `0.993999999`; a live (non-disabled) row floors at `0.05` and a
disabled row sits between `0.05` and `0.054`.

The offset separates same-source claims but is not injective: two repo IDs for
the same package that are congruent modulo 1e6 (1e3 when disabled) still score
identically. Total ordering therefore comes from the shared read fragment,
which tie-breaks on `repo_id` after `confidence`; the offset only keeps the
common case from reaching that tie-break. The daily sweep counts remaining
ties and logs them as a data-quality signal rather than failing.

### Write and read policy

- **Conflicts are keep-highest**, in every writer:
  `confidence = GREATEST(EXCLUDED.confidence, package_repos.confidence)` when
  the claim comes from a different source than the stored row, with `source` and
  `provenance` moving only when the incoming score wins strictly. A weaker source
  (routine registry refresh) therefore cannot overwrite a stronger one (manual,
  attested deps.dev), and the stored row does not depend on the order different
  writers run in.

  A same-source write is latest-refresh: that source is restating its own row, so
  `confidence` and `provenance` are replaced outright, downgrades included. This
  is what lets evidence that genuinely got weaker — a deps.dev link that lost its
  attestation — reach the table at all, and what lets a row written before
  `provenance` existed acquire one. Keep-highest arbitrates between sources; it is
  not a ratchet within one.
- **Rescores lock in primary-key order** (`ORDER BY pr.id FOR UPDATE`) so a
  package-scoped and a repo-scoped rescore over an overlapping set cannot
  deadlock.
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
out-of-band via `scripts/rescorePackageRepos.ts` (which also reports the tie
count). Afterwards the `package-repo-confidence-sweep-daily` Temporal schedule
reruns the same procedure and logs the tie count.

## Alternatives Considered

### Keep per-writer confidence constants

Each registry loop keeps its own literal (`0.8`, `0.95`, …) and the deps.dev
BigQuery query keeps its inline `CASE`.

- **Pros**: no migration, no table rewrite, no backfill, no cross-service
  coupling to a database function.
- **Cons**: nine writers drifted to five different tie-breakers and two
  incompatible meanings of 0.8; a tier change means a coordinated deploy;
  nothing can be rescored, because the inputs are collapsed at write time.
- **Why not**: the drift is the problem CM-1306 exists to fix, and the
  evidence-quality work in CM-1393/CM-1394 needs a single place to hook into.

### Score in TypeScript instead of in the database

A shared `computeConfidence()` in `@crowd/data-access-layer`, called by every
writer before the INSERT.

- **Pros**: unit-testable without a database, no migration to change a tier,
  familiar to every service author.
- **Cons**: rescoring 200M+ rows means pulling them into Node and writing them
  back; the deps.dev merge is a set-based `INSERT … SELECT` from staging that
  would have to become row-by-row; nothing stops a raw SQL writer from
  bypassing it.
- **Why not**: the backfill and the daily sweep are both bulk set operations —
  an `IMMUTABLE` function lets them stay single statements, and it can be used
  from a `LATERAL` in any query.

### Store the inputs and compute confidence at read time

Persist `source`, `provenance` and repo state only; rank with the scoring
expression inlined into each read query.

- **Pros**: no stored value to go stale, no backfill, no sweep.
- **Cons**: every read path repeats the expression (the drift problem, moved);
  no index can serve `ORDER BY confidence DESC`, so best-repo lookups become
  sorts over a package's whole link set; Tinybird and the public API have no
  column to mirror.
- **Why not**: `package_repos_package_id_confidence_idx` is what keeps the
  best-repo read cheap at this scale.

## Consequences

### Positive

- One formula, one place to change it; retuning a tier is a migration, not a
  sweep through nine writers.
- Ranking is total and reproducible — the shared read fragment gives
  `ORDER BY confidence DESC LIMIT 1` exactly one answer per package.
- A single scoring seam is what makes CM-1393 and CM-1394 possible without
  either PR hardcoding a confidence it does not own.
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
