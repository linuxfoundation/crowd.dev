import { describe, expect, it } from 'vitest'

import type { ReportingProtocolRow } from '@crowd/data-access-layer'

import { toAkritesExternalProjectProfiling } from './akritesExternalProjectProfiling'

function baseRow(overrides: Partial<ReportingProtocolRow> = {}): ReportingProtocolRow {
  return {
    purl: 'pkg:npm/lodash',
    declared: true,
    methods: [
      {
        type: 'github-pvr',
        status: 'preferred',
        endpoint: 'https://github.com/o/r/security/advisories/new',
        condition: null,
        confidence: 'declared',
        provenance: { api: 'pvr-flag' },
      },
    ],
    guidelines: null,
    sources: [{ api: 'pvr-flag' }],
    assembledAt: '2026-07-29 16:17:37.374211+00',
    ...overrides,
  }
}

describe('toAkritesExternalProjectProfiling', () => {
  it('maps a declared protocol and normalizes assembledAt to ISO 8601', () => {
    const result = toAkritesExternalProjectProfiling(baseRow())
    expect(result.purl).toBe('pkg:npm/lodash')
    expect(result.declared).toBe(true)
    expect(result.methods).toHaveLength(1)
    expect(result.methods[0]).toMatchObject({
      type: 'github-pvr',
      status: 'preferred',
      confidence: 'declared',
    })
    expect(result.sources).toEqual([{ api: 'pvr-flag' }])
    expect(result.assembledAt).toBe('2026-07-29T16:17:37.374Z')
  })

  it('passes guidelines through when present', () => {
    const guidelines = { generalPrinciples: ['coordinate disclosure'], avoid: [], recommend: [] }
    const result = toAkritesExternalProjectProfiling(baseRow({ guidelines }))
    expect(result.guidelines).toEqual(guidelines)
  })

  it('defaults null/absent jsonb collections and unparseable assembledAt', () => {
    const result = toAkritesExternalProjectProfiling(
      baseRow({
        declared: false,
        methods: null as unknown as ReportingProtocolRow['methods'],
        sources: null as unknown as ReportingProtocolRow['sources'],
        assembledAt: null,
      }),
    )
    expect(result.declared).toBe(false)
    expect(result.methods).toEqual([])
    expect(result.sources).toEqual([])
    expect(result.guidelines).toBeNull()
    expect(result.assembledAt).toBeNull()
  })

  it('returns null for a non-null but unparseable assembledAt', () => {
    const result = toAkritesExternalProjectProfiling(baseRow({ assembledAt: 'not-a-timestamp' }))
    expect(result.assembledAt).toBeNull()
  })
})
