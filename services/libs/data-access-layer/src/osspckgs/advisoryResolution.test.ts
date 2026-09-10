import { describe, expect, it } from 'vitest'

import { type AdvisoryAffectedRange, resolveAdvisory } from './advisoryResolution'

function range(overrides: Partial<AdvisoryAffectedRange> = {}): AdvisoryAffectedRange {
  return {
    introduced: null,
    fixed: null,
    lastAffected: null,
    rangeRaw: null,
    unaffectedRaw: null,
    ...overrides,
  }
}

describe('resolveAdvisory', () => {
  it('reports open when the latest version is inside an affected range', () => {
    expect(resolveAdvisory('2.0.0', [range({ introduced: '1.0.0', fixed: '3.0.0' })])).toBe('open')
  })

  it('reports patched when the latest version is at or past the fix', () => {
    expect(resolveAdvisory('2.0.0', [range({ introduced: '1.0.0', fixed: '1.5.0' })])).toBe(
      'patched',
    )
  })

  // #4: lexicographic text comparison said 1.9.0 >= 1.10.0 → wrongly "patched".
  it('compares versions numerically, not lexicographically (1.9.0 < 1.10.0)', () => {
    expect(resolveAdvisory('1.9.0', [range({ introduced: '1.0.0', fixed: '1.10.0' })])).toBe('open')
  })

  // #3: a version predating the range start must not be reported as still affected.
  it('reports patched when the latest version predates the introduced bound', () => {
    expect(resolveAdvisory('1.0.0', [range({ introduced: '2.0.0', fixed: '3.0.0' })])).toBe(
      'patched',
    )
  })

  it('honors last_affected (inclusive upper bound) when there is no fix', () => {
    expect(resolveAdvisory('2.5.0', [range({ introduced: '1.0.0', lastAffected: '2.0.0' })])).toBe(
      'patched',
    )
    expect(resolveAdvisory('2.0.0', [range({ introduced: '1.0.0', lastAffected: '2.0.0' })])).toBe(
      'open',
    )
  })

  // #2: deps.dev raw-only rows are unknowable, not "no fix" — they must not force open.
  it('ignores raw-only rows and resolves from the structured OSV row', () => {
    const rows = [
      range({ rangeRaw: '>=1.0.0 <1.5.0' }), // raw-only, no structured bounds
      range({ introduced: '1.0.0', fixed: '1.5.0' }), // OSV structured → patched at 2.0.0
    ]
    expect(resolveAdvisory('2.0.0', rows)).toBe('patched')
  })

  it('returns null when the only ranges are raw-only', () => {
    expect(resolveAdvisory('2.0.0', [range({ rangeRaw: '>=1.0.0' })])).toBeNull()
  })

  // OSV MAL-* rows are all-null structured with no raw payload → "always vulnerable".
  it('keeps all-null structured (MAL) ranges and resolves them to open', () => {
    expect(resolveAdvisory('2.0.0', [range()])).toBe('open')
  })

  it('returns null when there is no latest version', () => {
    expect(resolveAdvisory(null, [range({ introduced: '1.0.0', fixed: '2.0.0' })])).toBeNull()
  })

  it('returns null (unknown) rather than guessing on an unparseable version', () => {
    expect(
      resolveAdvisory('1.2.3-rc.1', [range({ introduced: '1.0.0', fixed: '2.0.0' })]),
    ).toBeNull()
  })

  it('does not claim patched when a determinable miss coexists with an unknown range', () => {
    const rows = [
      range({ introduced: '1.0.0', fixed: '1.5.0' }), // 2.0.0 not in range (miss)
      range({ introduced: 'weird', fixed: 'alsoweird' }), // undeterminable → unknown
    ]
    expect(resolveAdvisory('2.0.0', rows)).toBeNull()
  })
})
