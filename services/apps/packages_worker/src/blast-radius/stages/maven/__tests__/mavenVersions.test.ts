import { describe, expect, it } from 'vitest'

import { highestVersion, mavenRangeEvents, versionsInRanges } from '../mavenVersions'

describe('mavenVersions', () => {
  describe('mavenRangeEvents', () => {
    it('reads ECOSYSTEM-typed ranges with an introduced/fixed pair', () => {
      const events = mavenRangeEvents({
        package: { ecosystem: 'Maven', name: 'com.example:foo' },
        ranges: [
          {
            type: 'ECOSYSTEM',
            events: [{ introduced: '1.0' }, { fixed: '2.0' }],
          },
        ],
      })
      expect(events).toEqual([{ introduced: '1.0', fixed: '2.0', lastAffected: null }])
    })

    it('ignores SEMVER-typed ranges', () => {
      const events = mavenRangeEvents({
        package: { ecosystem: 'Maven', name: 'com.example:foo' },
        ranges: [{ type: 'SEMVER', events: [{ introduced: '1.0' }, { fixed: '2.0' }] }],
      })
      expect(events).toEqual([])
    })

    it('falls back to the explicit versions list when there are no ranges', () => {
      const events = mavenRangeEvents({
        package: { ecosystem: 'Maven', name: 'com.example:foo' },
        versions: ['1.0', '1.1'],
      })
      expect(events).toEqual([
        { introduced: '1.0', fixed: null, lastAffected: '1.0' },
        { introduced: '1.1', fixed: null, lastAffected: '1.1' },
      ])
    })
  })

  describe('versionsInRanges', () => {
    it('filters versions ordered via Maven comparison, not lexical/semver', () => {
      const versions = ['1.0', '1.9', '1.10', '2.0']
      const ranges = [{ introduced: '1.0', fixed: '2.0', lastAffected: null }]
      // Lexically "1.10" < "1.9", but Maven orders 1.10 > 1.9 — both must be included,
      // and the fixed version 2.0 must be excluded.
      expect(versionsInRanges(versions, ranges)).toEqual(['1.0', '1.9', '1.10'])
    })

    it('excludes when an unparseable bound makes a bound comparison ambiguous', () => {
      const versions = ['1.0']
      const ranges = [{ introduced: null, fixed: null, lastAffected: null }]
      expect(versionsInRanges(versions, ranges)).toEqual(['1.0'])
    })
  })

  describe('highestVersion', () => {
    it('returns the Maven-ordered highest version', () => {
      expect(highestVersion(['1.0', '1.10', '1.9'])).toBe('1.10')
    })

    it('handles an empty list', () => {
      expect(highestVersion([])).toBeNull()
    })
  })
})
