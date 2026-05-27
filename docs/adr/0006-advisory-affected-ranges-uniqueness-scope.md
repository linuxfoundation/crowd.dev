# ADR-0006: Uniqueness scope of `advisory_affected_ranges`

**Date**: 2026-05-27
**Status**: accepted
**Deciders**: CDP/Insights team

## Context

OSV records describe vulnerable version windows as one or more `affected[]` blocks per advisory, each carrying its own `ranges[]`. The `parseOsvRecord` step in osv-sync coalesces multiple `affected[]` blocks targeting the same `(ecosystem, package_name)` into a single `NormalizedPackageEntry`, accumulating every range across those blocks (`services/apps/packages_worker/src/osv/parseOsvRecord.ts`). This is needed because OSV emits cross-distro patches, partial-fix advisories, and platform-specific windows as separate `affected[]` blocks for the same package.

The initial schema (`V1779710880__initial_schema.sql`, shipped in PR #4146) declared the uniqueness key for `advisory_affected_ranges` as:

```sql
CREATE UNIQUE INDEX ON advisory_affected_ranges
  (advisory_package_id, COALESCE(introduced_version, ''));
```

That is strictly narrower than the natural uniqueness of a range tuple — it forces any two ranges sharing an `introduced_version` to collapse into one row, even if they have different `fixed_version` or `last_affected` values. The `dedupeRanges` pre-flight in `upsertAdvisory.ts` keyed on `introduced_version` alone to avoid INSERT conflicts, with the side effect that the wider range was silently discarded for any pair that shared an `introduced_version`. This directly contradicts the principle locked in `osv-plan.md §2 #1`: _"one package has many version ranges. No denormalization."_

Cursor's bot review on commit `1b978ac22` surfaced the data-loss path: in cross-distro / partial-fix OSV records, the surviving range may be the _narrower_ one, leading `isInRange` (and thus `packages.has_critical_vulnerability`) to return FALSE for versions inside the wider window — a missed critical alert. The bug is real and not theoretical; this category of OSV record exists in the live dataset.

## Decision

Widen the unique index on `advisory_affected_ranges` to cover the full range tuple:

```sql
CREATE UNIQUE INDEX ON advisory_affected_ranges (
    advisory_package_id,
    COALESCE(introduced_version, ''),
    COALESCE(fixed_version,    ''),
    COALESCE(last_affected,    '')
);
```

Update `dedupeRanges` to key on the full tuple `(introducedVersion, fixedVersion, lastAffected)` so the application-side pre-flight matches the database-side constraint. Truly identical tuples (the original "OSV emits a redundant event on the same line" case) still collapse; ranges that differ in any component are all preserved.

The change is captured in migration `V1779897650__widen_advisory_affected_ranges_unique_index.sql`, which drops the narrow index by definition (via `pg_indexes` lookup, since the initial migration did not name it) and recreates the wider one. The drop is idempotent: re-running on a database that already has only the wide index is a no-op.

## Alternatives Considered

### Alternative 1: Keep the narrow index, coalesce-to-widest at parse time

Merge ranges sharing an `introduced_version` in `parseOsvRecord` by taking the broadest `fixed_version` (max) and `last_affected` (max), so the surviving row always describes the union of the inputs.

- **Pros**: No migration. The narrow unique index stays as-is. Default skews toward over-reporting (broader vulnerable window) rather than under-reporting — better for the Osprey threat model where missed alerts are worse than false positives.
- **Cons**: This _is_ denormalization, which `osv-plan §2 #1` explicitly excluded. The merged row loses per-distro provenance: a downstream consumer can no longer tell that the wider window came from distro A while distro B was patched earlier. Comparator semantics also become ambiguous when ecosystems use different version orderings on different platforms.
- **Why not**: Encodes a workaround for a schema mistake in business logic. The schema is the contract; making the application reshape its data to fit a too-narrow constraint is the wrong direction.

### Alternative 2: Drop the unique index entirely, rely on the FK + dedupe in TS

Treat the table as an append log of ranges, with dedupe handled purely in the upsert path.

- **Pros**: Fewer constraints to maintain. Migration is one `DROP INDEX` line.
- **Cons**: Real duplicate ranges (the OSV "redundant event" case the original dedupe was designed for) now land in the table and inflate scans during derivation. The query in `deriveCriticalFlag.ts` joins through this table per package; row count matters. Also loses the safety net that catches an application bug producing accidental duplicates — without the index, a malformed insert path can silently flood the table.
- **Why not**: The unique constraint _should_ exist; the only question is what its scope should be. Removing it sacrifices the safety net to avoid choosing the right key.

### Alternative 3: Track the constraint loosely, deduplicate at query time

Keep the table un-constrained, then `SELECT DISTINCT` at read time when derivation reads ranges.

- **Pros**: Zero migration effort. Insert path becomes trivially append-only.
- **Cons**: Same row-count inflation as Alternative 2, plus query overhead that scales with duplicate count. The derive step already iterates ranges for every package in the corpus (hundreds of thousands of packages); making each iteration paginate through more rows than necessary is wasteful, and ADR-0003 already calls out derivation cadence as a sensitive path.
- **Why not**: Trades storage and per-query cost for an implementation shortcut.

## Consequences

### Positive

- `osv-plan §2 #1` ("one package has many version ranges; no denormalization") is now actually enforced by the schema, not just intended.
- Cross-distro and partial-fix OSV records no longer lose ranges. `has_critical_vulnerability` derivation sees every vulnerable window OSV reports, so missed-alert false negatives caused by this specific class of OSV data are eliminated.
- The dedup contract in `upsertAdvisory.ts` matches the database constraint exactly, making the invariant easy to audit ("the dedup key is the unique-index key").
- The migration is idempotent in both directions — running it on a fresh DB with only the wide index is a no-op (the lookup finds nothing to drop).

### Negative

- Adds a small migration to the slice (one DO-block + one CREATE UNIQUE INDEX, ~20 LOC). Flyway has to apply it; databases with the narrow index need the drop-then-create to run successfully.
- The wide index is slightly larger on disk than the narrow one (~3 columns of additional COALESCE expression evaluation per row). Negligible at OSV scale (~250k–500k rows across npm + Maven; the table is dwarfed by `packages`).
- The application-side dedup key has to stay in sync with the index key. If a future migration adds another range column (e.g. introducing `last_affected_inclusive` boundaries), `dedupeRanges` needs the same column added. Captured here so future readers know to update both.

### Risks

- The `DO $$ ... $$` block uses heuristics (`indexdef` LIKE patterns) to find the old index by definition because the initial migration didn't name it. If a future deployment ever ends up with multiple unique indexes on `advisory_package_id` that match the pattern, only the first found is dropped. Mitigation: the pattern is specific (`NOT ILIKE '%fixed_version%' AND NOT ILIKE '%last_affected%'` rules out the wide index itself, and `ILIKE '%COALESCE(introduced_version%'` rules out any unrelated index). Verified by inspection against the initial schema and the new wide index; no other unique index on this table exists.
- A pathological OSV record with hundreds of overlapping ranges per package could now produce hundreds of rows where the narrow index would have folded them. Real-world OSV records have at most a handful of ranges per package; the row-count blowup is bounded.
- The change is forward-compatible with Postgres versioning (`pg_indexes` and dynamic SQL via `EXECUTE` are stable since 9.x), but the migration relies on `current_schema()` matching the schema the table lives in. The `packages-db` worker uses the default `public` schema (per ADR-0001), so this matches; documented here so a future schema split doesn't break the lookup silently.
