import type { Request, Response } from 'express'
import { describe, expect, it, vi } from 'vitest'

import { getBlastRadiusJob } from './getBlastRadiusJob'

const { getAnalysisDetail, getVerdictResults } = vi.hoisted(() => ({
  getAnalysisDetail: vi.fn(),
  getVerdictResults: vi.fn(),
}))

vi.mock('@crowd/data-access-layer/src/packages/blastRadius', () => ({
  getAnalysisDetail,
  getVerdictResults,
}))

vi.mock('@/db/packagesDb', () => ({
  getPackagesQx: vi.fn().mockResolvedValue({}),
}))

const ANALYSIS_ID = '11111111-1111-4111-8111-111111111111'

function mockReqRes(params: unknown) {
  getAnalysisDetail.mockClear()
  getVerdictResults.mockClear()

  const req = { params } as unknown as Request

  const json = vi.fn()
  const status = vi.fn().mockReturnValue({ json })
  const res = { status, json } as unknown as Response

  return { req, res, status, json }
}

describe('getBlastRadiusJob', () => {
  it('returns a pending analysis with no results/summary', async () => {
    getAnalysisDetail.mockResolvedValue({
      id: ANALYSIS_ID,
      advisory_osv_id: 'GHSA-jf85-cpcp-j695',
      package_name: null,
      ecosystem: 'npm',
      status: 'pending',
      error: null,
      candidates_considered: null,
      started_at: '2026-07-01T00:00:00.000Z',
      completed_at: null,
    })

    const { req, res, json } = mockReqRes({ analysisId: ANALYSIS_ID })

    await getBlastRadiusJob(req, res)

    expect(getVerdictResults).not.toHaveBeenCalled()
    expect(json).toHaveBeenCalledWith(
      expect.objectContaining({
        analysisId: ANALYSIS_ID,
        status: 'pending',
        summary: null,
        results: null,
      }),
    )
  })

  it('returns summary and results for a done analysis', async () => {
    getAnalysisDetail.mockResolvedValue({
      id: ANALYSIS_ID,
      advisory_osv_id: 'GHSA-jf85-cpcp-j695',
      package_name: 'lodash',
      ecosystem: 'npm',
      status: 'done',
      error: null,
      candidates_considered: 10,
      started_at: '2026-07-01T00:00:00.000Z',
      completed_at: '2026-07-01T01:00:00.000Z',
    })
    getVerdictResults.mockResolvedValue([
      {
        name: 'benchmark.js',
        version: '2.1.4',
        downloads: 500000,
        reachable_verdict: 'affected',
        confidence: 0.9,
        evidence: [{ file: 'index.js', line: 10, snippet: 'require("lodash").merge' }],
        reasoning: 'uses merge',
      },
      {
        name: 'other-pkg',
        version: '1.0.0',
        downloads: 100,
        reachable_verdict: 'not_affected',
        confidence: 0.5,
        evidence: null,
        reasoning: 'unused',
      },
    ])

    const { req, res, json } = mockReqRes({ analysisId: ANALYSIS_ID })

    await getBlastRadiusJob(req, res)

    expect(json).toHaveBeenCalledWith(
      expect.objectContaining({
        status: 'done',
        summary: expect.objectContaining({
          totalDependentsInRange: 10,
          dependentsAnalyzed: 2,
          dependentsExcludedUpfront: 8,
          dependentsAffected: 1,
          affectedPercentage: 50,
          affectedDependents: ['pkg:npm/benchmark.js'],
        }),
        results: [
          expect.objectContaining({
            dependent: 'pkg:npm/benchmark.js',
            affected: true,
            confidence: 'high',
          }),
          expect.objectContaining({
            dependent: 'pkg:npm/other-pkg',
            affected: false,
            confidence: 'medium',
          }),
        ],
      }),
    )
  })

  it('404s when the analysis does not exist', async () => {
    getAnalysisDetail.mockResolvedValue(null)

    const { req, res } = mockReqRes({ analysisId: ANALYSIS_ID })

    await expect(getBlastRadiusJob(req, res)).rejects.toThrow()
  })

  it('rejects a non-uuid analysisId', async () => {
    const { req, res } = mockReqRes({ analysisId: 'not-a-uuid' })

    await expect(getBlastRadiusJob(req, res)).rejects.toThrow()
    expect(getAnalysisDetail).not.toHaveBeenCalled()
  })
})
