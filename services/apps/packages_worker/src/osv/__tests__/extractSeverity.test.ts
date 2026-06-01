import { describe, expect, it } from 'vitest'

import { extractSeverity } from '../extractSeverity'
import { OsvRecord } from '../types'

function record(partial: Partial<OsvRecord>): OsvRecord {
  return { id: 'GHSA-test', ...partial }
}

describe('extractSeverity', () => {
  it('short-circuits MAL- ids before checking severity[]', () => {
    // Even when a real V3 vector is present (it shouldn't be, but defend the path)
    // a MAL- id always classifies as a malicious-package report.
    const r = record({
      id: 'MAL-2024-12345',
      severity: [{ type: 'CVSS_V3', score: 'CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:C/C:H/I:H/A:H' }],
      database_specific: { severity: 'CRITICAL' },
    })
    expect(extractSeverity(r)).toEqual({
      severity: null,
      cvss: null,
      cvssSource: 'osv_malicious_package',
    })
  })

  it('parses a CVSS_V3 vector to numeric and surfaces qualitative tag', () => {
    const r = record({
      severity: [{ type: 'CVSS_V3', score: 'CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:C/C:H/I:H/A:H' }],
      database_specific: { severity: 'critical' }, // lowercase tolerated
    })
    expect(extractSeverity(r)).toEqual({
      severity: 'CRITICAL',
      cvss: 10.0,
      cvssSource: 'osv_cvss_v3',
    })
  })

  it('falls back to qualitative when only V4 is present (V4 numeric not implemented)', () => {
    // Documented v1 limitation in ADR-0001 §CVSS scoring strategy + cvssScoring.ts.
    const r = record({
      severity: [
        {
          type: 'CVSS_V4',
          score: 'CVSS:4.0/AV:N/AC:L/AT:N/PR:N/UI:N/VC:H/VI:H/VA:H/SC:N/SI:N/SA:N',
        },
      ],
      database_specific: { severity: 'HIGH' },
    })
    expect(extractSeverity(r)).toEqual({
      severity: 'HIGH',
      cvss: 7.5,
      cvssSource: 'osv_qualitative_fallback',
    })
  })

  it('prefers V3 over qualitative when both present', () => {
    // V3 numeric is more precise than the qualitative bucket.
    const r = record({
      severity: [{ type: 'CVSS_V3', score: 'CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H' }],
      database_specific: { severity: 'MEDIUM' }, // would map to 5.0 if used
    })
    const result = extractSeverity(r)
    expect(result.cvss).toBe(9.8)
    expect(result.cvssSource).toBe('osv_cvss_v3')
  })

  it('falls back to qualitative when no vector is parseable', () => {
    const r = record({ database_specific: { severity: 'MEDIUM' } })
    expect(extractSeverity(r)).toEqual({
      severity: 'MEDIUM',
      cvss: 5.0,
      cvssSource: 'osv_qualitative_fallback',
    })
  })

  it('returns all nulls when nothing usable is present', () => {
    const r = record({})
    expect(extractSeverity(r)).toEqual({
      severity: null,
      cvss: null,
      cvssSource: null,
    })
  })

  it('returns all nulls when qualitative tag is an unrecognized string', () => {
    const r = record({ database_specific: { severity: 'unknown-severity' } })
    expect(extractSeverity(r)).toEqual({
      severity: null,
      cvss: null,
      cvssSource: null,
    })
  })

  it('falls back through an unparseable V3 vector to qualitative', () => {
    // A bad vector is treated as if V3 weren't there at all.
    const r = record({
      severity: [{ type: 'CVSS_V3', score: 'CVSS:3.1/junk' }],
      database_specific: { severity: 'LOW' },
    })
    expect(extractSeverity(r)).toEqual({
      severity: 'LOW',
      cvss: 3.0,
      cvssSource: 'osv_qualitative_fallback',
    })
  })
})
