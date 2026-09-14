# ADR-0004: Compute GO/NUGET transitive dependent counts via exact reverse closure (over HLL approximation)

**Date**: 2026-06-23
**Status**: accepted
**Deciders**: Uroš Marolt

## Context
deps.dev publishes its reverse-dependent index (`Dependents`) and its resolved/transitive
dependency graph (`Dependencies`, `DependencyGraphEdges`) for only four ecosystems —
NPM, MAVEN, PYPI, CARGO (verified via BQ 2026-06-23: each `GROUP BY System` returns exactly
those four). GO and NUGET have only raw direct manifests (`GoRequirementsLatest`,
`NuGetRequirementsLatest`). The `dependent_counts` ingest was therefore split three ways
(edges / go / nuget — see the dependent-counts split). Direct `dependent_count` for GO/NUGET
is a cheap manifest invert, but `transitive_dependent_count` (MinimumDepth>1) and the all-depth
`dependent_repos_count` require the reverse **transitive closure**, which deps.dev does not
provide for these two ecosystems — so we must compute it ourselves from the direct edges.

## Decision
Compute the **exact** reverse transitive closure for GO/NUGET — a semi-naive fixpoint over
module-level edges (names hashed to INT64 via `FARM_FINGERPRINT`, persisted + clustered) iterated
to convergence — so GO/NUGET transitive counts are true integers matching the edge-system
methodology. We chose exact over an HLL-sketch approximation, accepting the higher cost for now.

## Alternatives Considered

### Alternative 1: HLL sketch propagation (approximate)
- **Pros**: ~cents/run, seconds, tiny tables (one HyperLogLog sketch per subject, ~188K rows for
  GO, merged along edges to a fixpoint — never materializes the billions of pairs). Full-depth.
- **Cons**: probabilistic cardinality estimate with bounded relative error ≈ `1.04/sqrt(2^p)`
  (~1.6% at precision 12, ~0.4% at 15). Methodology differs from the exact edge-system counts, so
  GO/NUGET would be ~1% approximate while the other four ecosystems are exact.
- **Why not**: the decision is to have identical, exact integer methodology across all six
  ecosystems. The measured exact cost (below) is acceptable for now. HLL remains the documented
  fallback if weekly cost becomes prohibitive.

### Alternative 2: Bounded-depth closure (cap at K hops)
- **Pros**: K deterministic joins in a single statement, dry-runnable, cost-bounded, no scripting.
- **Cons**: undercounts. The GO closure needs **31 hops** to converge; new pairs peak at hop 8
  (~507M) and only decay to zero by hop 31. Capping at a small K drops hundreds of millions of
  genuine transitive pairs.
- **Why not**: a low cap is materially wrong; a cap high enough to be correct is no cheaper than
  full convergence.

## Consequences

### Positive
- Exact integer counts, full depth, identical methodology to NPM/MAVEN/PYPI/CARGO.
- Deterministic and reproducible; no estimator error to explain to downstream consumers.
- Termination is guaranteed by construction: each iteration's frontier is anti-joined against the
  accumulated `reach`, so only brand-new pairs survive and the finite pair space converges (proven:
  GO converged at hop 31 with `new_pairs → 0`).

### Negative
- This is by far the heaviest job in the packages pipeline. Measured exact runs (2026-06-23,
  INT64-fingerprint-optimized; raw string names would be ~5×). The build-`reach` figures are from the
  closure probe; the full-pipeline figures (incl. the all-depth repos aggregation + per-subject
  counts → `_export_data`) are from the end-to-end validation run of the production script:
  - **GO**: 5.81B closure pairs, 31 iterations. Build reach ≈ 1.86 TB; **full pipeline = 2.31 TB
    billed (~$14.5 at $6.25/TiB), 99 slot-hours, 16 min wall, 132 child statements** (validated).
  - **NUGET**: 114M closure pairs, 19 iterations, ~4 min wall, ~32 GB billed (~$0.19); full pipeline
    validated → 191,762 subject rows.
  - GO dominates (its graph is ~50× larger and deeper). Combined ≈ **~$15/run, ~20 min, weekly**.
    For comparison the edge `dependent_counts` job scans ~310 GB (~$2).
- Requires a semi-naive BQ scripting job (not a single exportable SELECT), so it does not fit the
  existing one-query→GCS pipeline shape as-is (integrated via an `isScript` mode — see below).

### Risks
- Cost/runtime grow with the GO/NUGET graphs. Mitigations: INT64 fingerprints (~5× cheaper),
  clustering on the join keys, and a documented escape hatch to switch GO/NUGET to HLL (Alternative
  1) if cost becomes prohibitive — counts in the hundred-thousands make ~1% error invisible for
  popularity/ranking.
- `FARM_FINGERPRINT` collisions are negligible at ~1–2M nodes (~1e-6) but non-zero; acceptable for
  these metrics. Switch to a dictionary-encoded INT id if exactness must be collision-proof.

## Pipeline integration (decided 2026-06-23)
- **No persistent scratch dataset.** The closure runs as one multi-statement scripting job using
  session-scoped `CREATE TEMP TABLE`s (edges → INT64-fingerprinted edges → `reach` fixpoint →
  fingerprint→name lookup → `_export_data`). Temp tables are clustered on the join keys and are
  auto-dropped when the script's session ends — on success *and* failure — so there is no lingering
  storage to bill or clean up. (The interactive probe used regular tables in
  `lfx-insights:scratch_closure_probe`; that is leftover, dropped separately — not the pipeline.)
- **Reuses the existing job path, not a new kind.** `bqExportToGcs` gains an `isScript` mode: when
  set, `sql` is the full script (ending in `CREATE TEMP TABLE _export_data AS …`) and the activity
  appends only the `EXPORT DATA … AS SELECT * FROM _export_data` statement rather than wrapping
  `sql` in a subquery. Everything downstream (listParquetFiles → guard → chunked
  gcsParquetToStaging → mergeStagingToTable) is unchanged, and the `dependent_counts_go` /
  `dependent_counts_nuget` kinds, guard baselines, and monitor entries stay continuous.
- **Cost guard.** A dry-run cannot price a `WHILE` loop, so script mode skips it and enforces the
  byte ceiling server-side via `maximumBytesBilled` (= the per-variant `maxBytesGb`). The script
  also carries an iteration cap as the deterministic runaway guard (convergence already proven:
  GO hop 31, NUGET hop 19, both `new_pairs → 0`).
- **Final output → counts.** `_export_data` emits `(purl, dependent_count,
  transitive_dependent_count, dependent_repos_count)`: fingerprints in `reach` are mapped back to
  names via a `names` temp table, joined to `purl_map` (name→purl). `transitive_dependent_count =
  COUNT(DISTINCT dep over reach) − direct_count`; `dependent_repos_count` = distinct
  `PackageVersionToProject` SOURCE_REPO_TYPE repos over the all-depth dependent set.
- **Weekly trigger.** No new schedule — the `go`/`nuget` variants are already child workflows of
  `bootstrapOsspckgs`, which the existing `osspckgs-bootstrap-weekly` schedule runs incrementally.

## Addendum: RUBYGEMS (2026-07-13)
RubyGems is also absent from deps.dev's `Dependents` reverse index (verified via BQ 2026-07-13:
`Dependents` contains only NPM/MAVEN/PYPI/CARGO) and has no resolved/transitive graph table, only
the raw manifest `RubyGemsRequirementsLatest` — same shape as GO/NUGET. It was added as a third
`isScript` closure variant (`dependent_counts_rubygems`), reusing this ADR's design unchanged:
same semi-naive fixpoint, same `isScript` pipeline integration, same guard/monitor pattern. Corpus
is NUGET-scale (1.7M pkgs / 4.5M runtime edges), so cost/runtime are expected to track NUGET, not
GO. This ADR's title and body still say "GO/NUGET" for the original decision; treat this addendum
as extending scope to a third ecosystem rather than superseding the analysis above.

Related: ADR-0003 (deps BQ table selection — same root cause: GO/NUGET absent from the resolved-graph tables).
