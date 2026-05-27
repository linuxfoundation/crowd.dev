# ADR-0005: CVSS scoring strategy for OSV ingestion (inline v3.1, defer v4)

**Date**: 2026-05-27
**Status**: accepted
**Deciders**: CDP/Insights team

## Context

OSV records carry severity as a `severity[]` array of `{type, score}` entries, where `type` is `CVSS_V2 | CVSS_V3 | CVSS_V4 | ...` and `score` is the **vector string** (e.g. `CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:C/C:H/I:H/A:H`), not a numeric base score. We need the numeric base score to flip `advisories.is_critical` (`cvss >= 7.0`) and to drive the `has_critical_vulnerability` derivation per ADR-0003.

The 2026-05-27 spike against the live OSV bucket showed:

- ~213k npm + 2 Maven `MAL-*` malicious-package reports with no `severity[]` at all.
- 4,692 npm + 5,725 Maven advisories with a `CVSS_V3` vector.
- 1,641 npm + 800 Maven advisories with a `CVSS_V4` vector (often, but not always, alongside V3).
- A residual ~1k records with only `database_specific.severity` (a `CRITICAL`/`HIGH`/`MEDIUM`/`LOW` qualitative tag) and no vector.

CVSS v3.1 has a closed-form base-score formula (~80 LOC, documented in the FIRST spec). CVSS v4.0 does not — its base score is derived through a "macro-vector" lookup table of ~270 entries plus a multi-step algorithm with several optional modifiers. The v3 formula is mature and bug-stable; the v4 algorithm is newer (released 2023) and its npm-ecosystem implementations are correspondingly less battle-tested.

## Decision

Implement CVSS v3.1 base-score computation inline in `cvssScoring.ts` from the FIRST specification (no third-party dependency). For records where the only available vector is V4, fall back to the qualitative tag (`osv_qualitative_fallback`) or, absent that, leave `cvss = NULL` (`cvss_source = NULL`). Track this as a known v1 limitation in ADR-0003 and revisit by either embedding the FIRST v4.0 macro-vector lookup or adopting a vetted v4-capable library in a follow-up.

The fallback chain in `extractSeverity.ts` is:

1. `MAL-*` id → `cvss = NULL`, `cvss_source = 'osv_malicious_package'`.
2. CVSS_V3 vector → compute inline, `cvss_source = 'osv_cvss_v3'`.
3. `database_specific.severity` qualitative tag → `CRITICAL=9.5 / HIGH=7.5 / MEDIUM=5.0 / LOW=3.0`, `cvss_source = 'osv_qualitative_fallback'`.
4. Nothing → all NULL.

## Alternatives Considered

### Alternative 1: Use the npm `cvss` package

- **Pros**: ~80 LOC of inline math goes away. Library is well-established for v2/v3.
- **Cons**: The npm `cvss` package supports v2/v3 only (no v4 as of this writing). For a third-party dep that doesn't cover the full surface we need, the supply-chain cost (one more transitive in the Docker image, one more thing to track for CVE notices) outweighs the LOC saving on a stable, documented formula.
- **Why not**: We'd add a dep and still own a v4 problem.

### Alternative 2: Use a v4-capable library (`cvss-suite`, `@vulncheck/cvss`)

- **Pros**: Single dep covers v3 + v4. Zero formula code in our tree.
- **Cons**: v4 support in npm libraries is recent and less battle-tested than v3. Adopting one means trusting a third party's implementation of an algorithm we'd want to verify against FIRST's reference test vectors anyway. If the library mis-scores a known CVE during a real incident, the time cost is much higher than the time we'd save now.
- **Why not**: The risk profile is wrong: we'd be trading inline code we can unit-test against published reference scores for opaque library behavior we'd have to validate the same way. The library still has to be picked, vetted, and pinned — and we'd carry that vetting burden into every minor upgrade.

### Alternative 3: Implement both v3.1 and v4.0 inline now

- **Pros**: Full coverage of every OSV record on day one. No follow-up needed.
- **Cons**: v4 requires embedding the ~270-entry macro-vector table from the FIRST spec (~6 KB of constants), plus an algorithm with several conditional branches for optional modifiers. The volume is doable but the validation burden (reference vectors for every macro-vector class) is large; getting v4 wrong on a critical CVE during an incident is worse than scoring it as `NULL` and falling back to qualitative.
- **Why not**: The combined effort to implement v4 correctly, write fixture tests covering every macro-vector class, and validate against the FIRST reference calculator is its own slice of work. Doing it under the same PR as the rest of OSV ingestion would push the PR past its size budget (already ~2,000 LOC including tests) and mix two distinct review surfaces.

### Alternative 4: Reject records without a numeric CVSS entirely

- **Pros**: Avoids the qualitative-fallback bucket (with its 9.5/7.5/5.0/3.0 synthesis) being mistaken for a real measurement.
- **Cons**: The qualitative tag is the only signal we have for ~1,400 advisories that happen to have it. Dropping them loses `is_critical` coverage for those records — they'd never contribute to `has_critical_vulnerability` even when their qualitative tag is `CRITICAL`. For the 213k MAL- records the question doesn't arise (the MAL- branch flips the flag directly), but for the 1.4k qualitative-only advisories the cost is real.
- **Why not**: A coarse score with a clearly-marked `cvss_source` is more useful than no score. The `cvss_source` column exists exactly so downstream consumers can decide whether to trust a fallback-synthesized score.

## Consequences

### Positive

- Zero CVSS supply-chain surface: no npm `cvss` library to pin, audit, or upgrade.
- v3.1 implementation is unit-testable against FIRST-published reference vectors. `cvssScoring.test.ts` pins six known scores (log4shell 10.0, shellshock 9.8, heartbleed 7.5, ChangeCipherSpec 4.8, a low end at 3.3, all-None at 0). Any future regression in the formula fails the suite.
- The `cvss_source` column makes the score provenance explicit, so the qualitative-fallback bucket (which is a synthesized number, not a measured one) is distinguishable from a real V3-derived score at query time.
- The v3 implementation is local to `cvssScoring.ts` and `extractSeverity.ts`. Adding v4 later is a contained change to those two files — no plumbing changes anywhere else.

### Negative

- ~1.1% of ingested advisories land with `cvss = NULL` because they're V4-only with no qualitative tag (1,092 of 226k as of 2026-05-27). These never contribute to `is_critical` or `has_critical_vulnerability`. ADR-0003 documents this gap.
- ~0.6% of advisories (1,387) are V4-with-qualitative — we score them coarsely (qualitative bucket) when we could be scoring them precisely. The binary `is_critical` decision is preserved for the `HIGH`/`CRITICAL` bucket (both map to ≥7.0), but the cvss number itself reads as 9.5/7.5/5.0/3.0 rather than the real v4 base score.
- The next engineer touching `cvssScoring.ts` has to choose between extending the inline implementation (more LOC) or introducing a CVSS dependency (this ADR explains why we didn't). The decision recurs.

### Risks

- A bug in the v3.1 inline implementation silently mis-scores critical CVEs. Mitigation: the reference-vector unit tests catch any formula drift; the integration test against the real packages-db confirms log4shell (10.0) and lodash CVE-2021-23337 (7.2) land at their published scores.
- The "defer v4" stance becomes permanent. Mitigation: ADR-0003 lists v4 as an explicit follow-up; the `osv_qualitative_fallback` and NULL buckets are queryable (`SELECT COUNT(*) FROM advisories WHERE cvss_source IS NULL OR cvss_source = 'osv_qualitative_fallback'`), so the cost of *not* doing v4 is a single SQL query away from being visible.
- The qualitative-fallback synthesized score (9.5/7.5/5.0/3.0) is mistaken for a precise measurement by a downstream consumer. Mitigation: the `cvss_source` column is the contract — any UI or query that displays `cvss` should consult `cvss_source` and label fallback scores accordingly. This is documented in ADR-0003 as well.
