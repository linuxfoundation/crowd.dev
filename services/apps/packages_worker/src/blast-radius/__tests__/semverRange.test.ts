import { describe, expect, it } from 'vitest'

import { highestVersion, rangeIncludesAny, versionsInRanges } from '../semverRange'

describe('semverRange', () => {
  describe('versionsInRanges', () => {
    it('filters versions matching at least one range', () => {
      const versions = ['1.0.0', '1.5.0', '2.0.0', '2.5.0', '3.0.0']
      const ranges = [
        { introduced: '1.0.0', fixed: '2.0.0' },
        { introduced: '3.0.0', fixed: null },
      ]
      const result = versionsInRanges(versions, ranges)
      expect(result).toEqual(['1.0.0', '1.5.0', '3.0.0'])
    })

    it('handles ranges with only introduced (no fixed)', () => {
      const versions = ['1.0.0', '1.5.0', '2.0.0']
      const ranges = [{ introduced: '1.5.0', fixed: null }]
      const result = versionsInRanges(versions, ranges)
      expect(result).toEqual(['1.5.0', '2.0.0'])
    })

    it('returns empty list if no versions match', () => {
      const versions = ['1.0.0', '1.5.0']
      const ranges = [{ introduced: '2.0.0', fixed: '3.0.0' }]
      const result = versionsInRanges(versions, ranges)
      expect(result).toEqual([])
    })

    it('skips invalid versions', () => {
      const versions = ['1.0.0', 'not-a-version', '2.0.0']
      const ranges = [{ introduced: '1.0.0', fixed: '3.0.0' }]
      const result = versionsInRanges(versions, ranges)
      expect(result).toEqual(['1.0.0', '2.0.0'])
    })
  })

  describe('rangeIncludesAny', () => {
    it('matches when vulnerable version is in declared range', () => {
      const result = rangeIncludesAny('^1.2.0', ['1.2.5', '1.3.0'])
      expect(result).toEqual({ includes: true, check: 'matched' })
    })

    it('excludes when no vulnerable version is in range', () => {
      const result = rangeIncludesAny('^2.0.0', ['1.0.0', '1.5.0'])
      expect(result).toEqual({ includes: false, check: 'excluded' })
    })

    it('conservatively includes git URLs', () => {
      const result = rangeIncludesAny('git@github.com:user/repo.git', ['1.0.0'])
      expect(result).toEqual({ includes: true, check: 'unparseable-included' })
    })

    it('conservatively includes file paths', () => {
      const result = rangeIncludesAny('file:../local-package', ['1.0.0'])
      expect(result).toEqual({ includes: true, check: 'unparseable-included' })
    })

    it('conservatively includes "latest"', () => {
      const result = rangeIncludesAny('latest', ['1.0.0'])
      expect(result).toEqual({ includes: true, check: 'unparseable-included' })
    })

    it('conservatively includes workspace:* specs', () => {
      const result = rangeIncludesAny('workspace:*', ['1.0.0'])
      expect(result).toEqual({ includes: true, check: 'unparseable-included' })
    })

    it('handles empty declared range', () => {
      const result = rangeIncludesAny('', ['1.0.0'])
      expect(result).toEqual({ includes: true, check: 'unparseable-included' })
    })

    it('handles complex semver ranges', () => {
      const result = rangeIncludesAny('>=1.0.0 <2.0.0 || >=3.0.0', ['1.5.0', '3.5.0'])
      expect(result).toEqual({ includes: true, check: 'matched' })
    })

    it('conservatively includes garbage ranges not caught by the known-prefix checks', () => {
      // semver.validRange returns null (not a throw) for this — regression test for a bug
      // where isValidSemverRange ignored that null and always reported specs as parseable.
      const result = rangeIncludesAny('not-a-real-range!!!', ['1.0.0'])
      expect(result).toEqual({ includes: true, check: 'unparseable-included' })
    })
  })

  describe('highestVersion', () => {
    it('returns the highest valid version', () => {
      const versions = ['1.0.0', '2.5.0', '1.5.0', '2.0.0']
      const result = highestVersion(versions)
      expect(result).toBe('2.5.0')
    })

    it('returns null if no valid versions', () => {
      const versions = ['not-a-version', 'also-invalid']
      const result = highestVersion(versions)
      expect(result).toBeNull()
    })

    it('filters invalid versions and returns highest valid', () => {
      const versions = ['1.0.0', 'invalid', '3.0.0', 'also-bad', '2.0.0']
      const result = highestVersion(versions)
      expect(result).toBe('3.0.0')
    })

    it('handles empty list', () => {
      const result = highestVersion([])
      expect(result).toBeNull()
    })
  })
})
