import { describe, expect, it } from 'vitest'

import { computeV3Score } from '../cvssScoring'

// Reference vectors with FIRST-published base scores. These pin the inline
// implementation against the official spec — a regression here means the
// scoring formula itself drifted.
describe('computeV3Score', () => {
  it.each([
    // CVE-2021-44228 (log4shell) — scope change, max impact
    ['CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:C/C:H/I:H/A:H', 10.0],
    // CVE-2014-6271 (shellshock) — no scope change, max impact
    ['CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H', 9.8],
    // CVE-2014-0160 (heartbleed) — confidentiality-only impact
    ['CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:N/A:N', 7.5],
    // CVE-2014-0224 (ChangeCipherSpec) — high complexity, partial impact
    ['CVSS:3.1/AV:N/AC:H/PR:N/UI:N/S:U/C:L/I:L/A:N', 4.8],
    // Local attack, low privileges, integrity-only, low — sanity-check the low end
    ['CVSS:3.1/AV:L/AC:L/PR:L/UI:N/S:U/C:N/I:L/A:N', 3.3],
    // All-None impact — must yield exactly 0
    ['CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:N/I:N/A:N', 0],
  ])('scores %s as %s', (vector, expected) => {
    expect(computeV3Score(vector)).toBe(expected)
  })

  it('accepts CVSS:3.0 vectors as well as 3.1', () => {
    // 3.0 and 3.1 share the same formula for these base metrics.
    expect(computeV3Score('CVSS:3.0/AV:N/AC:L/PR:N/UI:N/S:C/C:H/I:H/A:H')).toBe(10.0)
  })

  it('returns null for non-v3 prefixes', () => {
    expect(computeV3Score('CVSS:2.0/AV:N/AC:L/Au:N/C:P/I:P/A:P')).toBeNull()
    expect(
      computeV3Score('CVSS:4.0/AV:N/AC:L/AT:N/PR:N/UI:N/VC:H/VI:H/VA:H/SC:N/SI:N/SA:N'),
    ).toBeNull()
  })

  it('returns null for malformed input', () => {
    expect(computeV3Score('')).toBeNull()
    expect(computeV3Score('not-a-vector')).toBeNull()
    expect(computeV3Score('CVSS:3.1/AV:N')).toBeNull() // missing required metrics
    expect(computeV3Score('CVSS:3.1/AV:Z/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H')).toBeNull() // bad enum
  })

  // Regression guards for the unvalidated-Scope bug: missing or invalid S used
  // to silently produce a Scope:Unchanged score instead of returning null.
  it('returns null when the Scope metric is missing', () => {
    expect(computeV3Score('CVSS:3.1/AV:N/AC:L/PR:N/UI:N/C:H/I:H/A:H')).toBeNull()
  })

  it('returns null when the Scope metric is invalid', () => {
    expect(computeV3Score('CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:X/C:H/I:H/A:H')).toBeNull()
  })

  it('handles scope-changed PR remapping', () => {
    // PR:L scores higher under Scope:Changed than under Scope:Unchanged.
    const unchanged = computeV3Score('CVSS:3.1/AV:N/AC:L/PR:L/UI:N/S:U/C:H/I:H/A:H')
    const changed = computeV3Score('CVSS:3.1/AV:N/AC:L/PR:L/UI:N/S:C/C:H/I:H/A:H')
    expect(unchanged).not.toBeNull()
    expect(changed).not.toBeNull()
    expect(changed).toBeGreaterThan(unchanged as number)
  })
})
