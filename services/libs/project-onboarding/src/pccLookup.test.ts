import { describe, expect, it, vi } from 'vitest'

import { PCC_CANDIDATES_QUERY, createPccCandidatesLookup } from './pccLookup'

describe('createPccCandidatesLookup', () => {
  it('binds the lowercased trimmed name for both the name and slug comparison', async () => {
    const runQuery = vi.fn().mockResolvedValue([])

    await createPccCandidatesLookup(runQuery)('  Open Telemetry ')

    expect(runQuery).toHaveBeenCalledWith(PCC_CANDIDATES_QUERY, [
      'open telemetry',
      'open telemetry',
    ])
  })

  it('maps rows to candidates and normalizes the score to the 0-1 range', async () => {
    const runQuery = vi
      .fn()
      .mockResolvedValue([{ PROJECT_ID: 'pcc-1', NAME: 'Kubernetes', SLUG: 'k8s', SCORE: 97 }])

    const candidates = await createPccCandidatesLookup(runQuery)('kubernetes')

    expect(candidates).toEqual([
      { projectId: 'pcc-1', name: 'Kubernetes', slug: 'k8s', score: 0.97 },
    ])
  })

  it('excludes internal projects', () => {
    expect(PCC_CANDIDATES_QUERY).toContain('NOT IS_INTERNAL_PROJECT')
  })
})
