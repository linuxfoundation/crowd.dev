import { describe, expect, it } from 'vitest'

import { ecosystemRangeEvents, highestVersion, versionsInRanges } from '../ecosystemVersions'

describe('ecosystemVersions', () => {
  describe('ecosystemRangeEvents', () => {
    it('reads ECOSYSTEM-typed ranges with an introduced/fixed pair', () => {
      const events = ecosystemRangeEvents({
        package: { ecosystem: 'Maven', name: 'com.example:foo' },
        ranges: [{ type: 'ECOSYSTEM', events: [{ introduced: '1.0' }, { fixed: '2.0' }] }],
      })
      expect(events).toEqual([{ introduced: '1.0', fixed: '2.0', lastAffected: null }])
    })

    it('ignores SEMVER-typed ranges', () => {
      const events = ecosystemRangeEvents({
        package: { ecosystem: 'NuGet', name: 'Some.Package' },
        ranges: [{ type: 'SEMVER', events: [{ introduced: '1.0' }, { fixed: '2.0' }] }],
      })
      expect(events).toEqual([])
    })

    it('falls back to the explicit versions list when there are no ranges', () => {
      const events = ecosystemRangeEvents({
        package: { ecosystem: 'NuGet', name: 'Some.Package' },
        versions: ['1.0', '1.1'],
      })
      expect(events).toEqual([
        { introduced: '1.0', fixed: null, lastAffected: '1.0' },
        { introduced: '1.1', fixed: null, lastAffected: '1.1' },
      ])
    })
  })

  describe('versionsInRanges', () => {
    it('orders Maven versions via Maven comparison, not lexical/semver', () => {
      const versions = ['1.0', '1.9', '1.10', '2.0']
      const ranges = [{ introduced: '1.0', fixed: '2.0', lastAffected: null }]
      // Lexically "1.10" < "1.9", but Maven orders 1.10 > 1.9 — both must be included,
      // and the fixed version 2.0 must be excluded.
      expect(versionsInRanges('maven', versions, ranges)).toEqual(['1.0', '1.9', '1.10'])
    })

    it('orders NuGet versions via semver comparison', () => {
      const versions = ['1.0.0', '1.9.0', '1.10.0', '2.0.0']
      const ranges = [{ introduced: '1.0.0', fixed: '2.0.0', lastAffected: null }]
      // Semver orders 1.9.0 > 1.10.0's minor is 10 > 9, so unlike the Maven case above,
      // 1.10.0 must sort after 1.9.0 here too, but via a different comparator entirely.
      expect(versionsInRanges('nuget', versions, ranges)).toEqual(['1.0.0', '1.9.0', '1.10.0'])
    })

    it('excludes when an unparseable bound makes a bound comparison ambiguous', () => {
      const versions = ['1.0.0']
      const ranges = [{ introduced: '---', fixed: null, lastAffected: null }]
      expect(versionsInRanges('nuget', versions, ranges)).toEqual([])
    })

    it('treats introduced "0" as "from the beginning" instead of an unparseable bound', () => {
      const versions = ['1.0.0', '1.5.0', '2.0.0']
      const ranges = [{ introduced: '0', fixed: '2.0.0', lastAffected: null }]
      expect(versionsInRanges('nuget', versions, ranges)).toEqual(['1.0.0', '1.5.0'])
    })
  })

  describe('highestVersion', () => {
    it('returns the Maven-ordered highest version', () => {
      expect(highestVersion('maven', ['1.0', '1.10', '1.9'])).toBe('1.10')
    })

    it('returns the semver-ordered highest version for NuGet', () => {
      expect(highestVersion('nuget', ['1.0.0', '1.10.0', '1.9.0'])).toBe('1.10.0')
    })

    it('handles an empty list', () => {
      expect(highestVersion('nuget', [])).toBeNull()
    })
  })
})
