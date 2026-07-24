import { describe, expect, it } from 'vitest'

import type { AkritesExternalContactDetailRow } from '@crowd/data-access-layer'

import { toAkritesExternalContactDetail } from './akritesExternalContactDetail'

function baseRow(
  overrides: Partial<AkritesExternalContactDetailRow> = {},
): AkritesExternalContactDetailRow {
  return {
    purl: 'pkg:npm/lodash',
    name: 'lodash',
    ecosystem: 'npm',
    securityPolicyUrl: 'https://example.org/SECURITY.md',
    vulnerabilityReportingUrl: null,
    bugBountyUrl: null,
    pvrEnabled: true,
    declaredRepositoryUrl: 'https://github.com/lodash/lodash.git',
    resolvedRepositoryUrl: 'https://github.com/lodash/lodash',
    repoMappingConfidence: 0.9,
    contactsLastRefreshed: '2024-01-01T00:00:00.000Z',
    securityContacts: [
      {
        channel: 'email',
        value: 'security@example.org',
        role: 'security-team',
        confidence: 'SECONDARY',
        score: 0.735,
      },
      {
        channel: 'github-pvr',
        value: 'https://example.org/advisories/new',
        role: 'security-team',
        confidence: 'PRIMARY',
        score: 0.94,
      },
    ],
    ...overrides,
  }
}

describe('toAkritesExternalContactDetail', () => {
  it('maps contacts with the confidence/score field renames', () => {
    const result = toAkritesExternalContactDetail(baseRow())
    expect(result.purl).toBe('pkg:npm/lodash')
    expect(result.contacts).toEqual([
      {
        channel: 'email',
        value: 'security@example.org',
        role: 'security-team',
        confidenceBand: 'SECONDARY',
        confidenceScore: 0.735,
      },
      {
        channel: 'github-pvr',
        value: 'https://example.org/advisories/new',
        role: 'security-team',
        confidenceBand: 'PRIMARY',
        confidenceScore: 0.94,
      },
    ])
  })

  it('derives overallConfidenceBand from the highest-scoring contact, returned verbatim', () => {
    // max score 0.94 -> PRIMARY (not SECONDARY from the other, lower-scoring contact).
    expect(toAkritesExternalContactDetail(baseRow()).overallConfidenceBand).toBe('PRIMARY')
  })

  it('returns the aggregate band on the internal scale (no crosswalk)', () => {
    const band = (confidence: string, score: number) =>
      toAkritesExternalContactDetail(
        baseRow({
          securityContacts: [
            {
              channel: 'email',
              value: 'x@y.z',
              role: 'maintainer',
              confidence: confidence as never,
              score,
            },
          ],
        }),
      ).overallConfidenceBand
    expect(band('SECONDARY', 0.6)).toBe('SECONDARY')
    expect(band('FALLBACK', 0.4)).toBe('FALLBACK')
  })

  it('returns NONE band and an empty array when there are no contacts', () => {
    const nullContacts = toAkritesExternalContactDetail(baseRow({ securityContacts: null }))
    expect(nullContacts.contacts).toEqual([])
    expect(nullContacts.overallConfidenceBand).toBe('NONE')

    const emptyContacts = toAkritesExternalContactDetail(baseRow({ securityContacts: [] }))
    expect(emptyContacts.overallConfidenceBand).toBe('NONE')
  })

  it('always returns the reserved fields as null', () => {
    const result = toAkritesExternalContactDetail(baseRow())
    expect(result.targetOrganizationName).toBeNull()
    expect(result.bugBountyProgramFlag).toBeNull()
    expect(result.reportingMethods).toBeNull()
    expect(result.reportingGuidelines).toBeNull()
    expect(result.integrationHints).toBeNull()
  })

  it('passes through the repo-sourced policy fields', () => {
    const result = toAkritesExternalContactDetail(
      baseRow({ vulnerabilityReportingUrl: 'https://example.org/report', pvrEnabled: false }),
    )
    expect(result.securityPolicyUrl).toBe('https://example.org/SECURITY.md')
    expect(result.vulnerabilityReportingUrl).toBe('https://example.org/report')
    expect(result.pvrEnabled).toBe(false)
  })

  it('passes through the repo provenance fields when present', () => {
    const result = toAkritesExternalContactDetail(baseRow())
    expect(result.declaredRepositoryUrl).toBe('https://github.com/lodash/lodash.git')
    expect(result.resolvedRepositoryUrl).toBe('https://github.com/lodash/lodash')
    expect(result.repoMappingConfidence).toBe(0.9)
  })

  it('casts repoMappingConfidence from a numeric string (pg-promise numeric type)', () => {
    const result = toAkritesExternalContactDetail(
      baseRow({ repoMappingConfidence: '0.9' as unknown as number }),
    )
    expect(result.repoMappingConfidence).toBe(0.9)
  })

  it('returns resolvedRepositoryUrl and repoMappingConfidence as null when there is no repo link', () => {
    const result = toAkritesExternalContactDetail(
      baseRow({ resolvedRepositoryUrl: null, repoMappingConfidence: null }),
    )
    expect(result.resolvedRepositoryUrl).toBeNull()
    expect(result.repoMappingConfidence).toBeNull()
  })

  it('returns declaredRepositoryUrl as null when the package has no declared repository', () => {
    const result = toAkritesExternalContactDetail(baseRow({ declaredRepositoryUrl: null }))
    expect(result.declaredRepositoryUrl).toBeNull()
  })
})
