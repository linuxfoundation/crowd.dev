import { describe, expect, it } from 'vitest'

import { NormalizedRange } from '../types'
import { dedupeRanges } from '../upsertAdvisory'

// dedupeRanges is the pre-flight pass that keeps inserts from colliding on the
// (advisory_package_id, introduced, fixed, last_affected) unique index. The
// scope of the dedup key is the load-bearing detail — keying only on
// introduced_version (the historical bug guarded by ADR-0001
// §`advisory_affected_ranges` uniqueness scope) silently dropped
// ranges that share an introduced_version but differ in fixed_version or
// last_affected, which OSV emits for cross-distro patches and partial-fix
// scenarios.

describe('dedupeRanges', () => {
  it('preserves two ranges sharing introduced but with different fixed_version', () => {
    // Realistic OSV scenario: two affected[] blocks for the same package
    // patched at different upstream commits across distros.
    const ranges: NormalizedRange[] = [
      { introducedVersion: '1.0.0', fixedVersion: '1.5.0', lastAffected: null },
      { introducedVersion: '1.0.0', fixedVersion: '2.0.0', lastAffected: null },
    ]
    expect(dedupeRanges(ranges)).toEqual(ranges)
  })

  it('preserves two ranges sharing introduced but with different last_affected', () => {
    const ranges: NormalizedRange[] = [
      { introducedVersion: '1.0.0', fixedVersion: null, lastAffected: '1.4.9' },
      { introducedVersion: '1.0.0', fixedVersion: null, lastAffected: '1.9.9' },
    ]
    expect(dedupeRanges(ranges)).toEqual(ranges)
  })

  it('collapses two truly identical tuples to one', () => {
    // Same OSV record occasionally emits redundant events for the same line —
    // the Set should still fold those.
    const ranges: NormalizedRange[] = [
      { introducedVersion: '1.0.0', fixedVersion: '2.0.0', lastAffected: null },
      { introducedVersion: '1.0.0', fixedVersion: '2.0.0', lastAffected: null },
    ]
    expect(dedupeRanges(ranges)).toEqual([
      { introducedVersion: '1.0.0', fixedVersion: '2.0.0', lastAffected: null },
    ])
  })

  it('treats null introducedVersion the same as the empty string for keying', () => {
    // Two MAL-style "always vulnerable" entries with different fix lines
    // (rare, but the keying still has to disambiguate them).
    const ranges: NormalizedRange[] = [
      { introducedVersion: null, fixedVersion: null, lastAffected: null },
      { introducedVersion: null, fixedVersion: '1.0.0', lastAffected: null },
    ]
    expect(dedupeRanges(ranges)).toEqual(ranges)
  })

  it('keeps first occurrence when a tuple is repeated', () => {
    const first: NormalizedRange = {
      introducedVersion: '1.0.0',
      fixedVersion: '2.0.0',
      lastAffected: null,
    }
    const ranges: NormalizedRange[] = [
      first,
      { introducedVersion: '1.0.0', fixedVersion: '2.0.0', lastAffected: null },
    ]
    const out = dedupeRanges(ranges)
    expect(out).toHaveLength(1)
    expect(out[0]).toBe(first)
  })
})
