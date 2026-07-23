import type { Request, Response } from 'express'
import { describe, expect, it, vi } from 'vitest'

import { getBlastRadiusJobBatch } from './getBlastRadiusJobBatch'

const { getAnalysisDetailsByIds, getVerdictResultsBatch, getDependentsExcludedByRangeCountBatch } =
  vi.hoisted(() => ({
    getAnalysisDetailsByIds: vi.fn(),
    getVerdictResultsBatch: vi.fn(),
    getDependentsExcludedByRangeCountBatch: vi.fn(),
  }))

vi.mock('@crowd/data-access-layer/src/packages/blastRadius', () => ({
  getAnalysisDetailsByIds,
  getVerdictResultsBatch,
  getDependentsExcludedByRangeCountBatch,
}))

vi.mock('@/db/packagesDb', () => ({
  getPackagesQx: vi.fn().mockResolvedValue({}),
}))

const PENDING_ID = '11111111-1111-4111-8111-111111111111'
const DONE_ID = '22222222-2222-4222-8222-222222222222'
const MISSING_ID = '33333333-3333-4333-8333-333333333333'

function mockReqRes(body: unknown) {
  getAnalysisDetailsByIds.mockClear()
  getVerdictResultsBatch.mockClear()
  getDependentsExcludedByRangeCountBatch.mockClear()
  getVerdictResultsBatch.mockResolvedValue([])
  getDependentsExcludedByRangeCountBatch.mockResolvedValue([])

  const req = { body } as unknown as Request

  const json = vi.fn()
  const status = vi.fn().mockReturnValue({ json })
  const res = { status, json } as unknown as Response

  return { req, res, json }
}

describe('getBlastRadiusJobBatch', () => {
  it('returns found/not-found entries in request order with page metadata', async () => {
    getAnalysisDetailsByIds.mockResolvedValue([
      {
        id: PENDING_ID,
        advisory_osv_id: 'GHSA-jf85-cpcp-j695',
        package_name: null,
        ecosystem: 'npm',
        status: 'pending',
        error: null,
        candidates_considered: null,
        started_at: '2026-07-01T00:00:00.000Z',
        completed_at: null,
      },
    ])

    const { req, res, json } = mockReqRes({
      analysisIds: [PENDING_ID, MISSING_ID],
    })

    await getBlastRadiusJobBatch(req, res)

    expect(json).toHaveBeenCalledWith(
      expect.objectContaining({
        page: 1,
        pageSize: expect.any(Number),
        total: 2,
        results: [
          expect.objectContaining({
            requestedAnalysisId: PENDING_ID,
            found: true,
            analysis: expect.objectContaining({ analysisId: PENDING_ID, status: 'pending' }),
          }),
          { requestedAnalysisId: MISSING_ID, found: false, analysis: null },
        ],
      }),
    )
  })

  it('only fetches verdicts/excluded counts for done analyses', async () => {
    const { req, res, json } = mockReqRes({ analysisIds: [PENDING_ID, DONE_ID] })

    getAnalysisDetailsByIds.mockResolvedValue([
      {
        id: PENDING_ID,
        advisory_osv_id: 'GHSA-jf85-cpcp-j695',
        package_name: null,
        ecosystem: 'npm',
        status: 'pending',
        error: null,
        candidates_considered: null,
        started_at: '2026-07-01T00:00:00.000Z',
        completed_at: null,
      },
      {
        id: DONE_ID,
        advisory_osv_id: 'GHSA-652q-gvq3-74qv',
        package_name: 'lodash',
        ecosystem: 'npm',
        status: 'done',
        error: null,
        candidates_considered: 10,
        started_at: '2026-07-01T00:00:00.000Z',
        completed_at: '2026-07-01T01:00:00.000Z',
      },
    ])
    getVerdictResultsBatch.mockResolvedValue([
      {
        analysisId: DONE_ID,
        name: 'benchmark.js',
        version: '2.1.4',
        downloads: 500000,
        reachable_verdict: 'affected',
        confidence: 0.9,
        evidence: null,
        reasoning: 'uses merge',
      },
    ])
    getDependentsExcludedByRangeCountBatch.mockResolvedValue([{ analysisId: DONE_ID, count: 8 }])

    await getBlastRadiusJobBatch(req, res)

    expect(getVerdictResultsBatch).toHaveBeenCalledWith(expect.anything(), [DONE_ID])
    expect(getDependentsExcludedByRangeCountBatch).toHaveBeenCalledWith(expect.anything(), [
      DONE_ID,
    ])

    const [{ results }] = json.mock.calls[0]
    expect(results[0]).toMatchObject({ requestedAnalysisId: PENDING_ID, found: true })
    expect(results[0].analysis).toMatchObject({ status: 'pending', summary: null, results: null })
    expect(results[1]).toMatchObject({ requestedAnalysisId: DONE_ID, found: true })
    expect(results[1].analysis).toMatchObject({
      status: 'done',
      summary: expect.objectContaining({ dependentsExcludedUpfront: 8 }),
    })
  })

  it('rejects a batch with a malformed uuid without querying the database', async () => {
    const { req, res } = mockReqRes({ analysisIds: [PENDING_ID, 'not-a-uuid'] })

    await expect(getBlastRadiusJobBatch(req, res)).rejects.toThrow()
    expect(getAnalysisDetailsByIds).not.toHaveBeenCalled()
  })

  it('rejects a batch with more than 100 analysisIds', async () => {
    const analysisIds = Array.from(
      { length: 101 },
      (_, i) => `44444444-4444-4444-8444-${String(i).padStart(12, '0')}`,
    )
    const { req, res } = mockReqRes({ analysisIds })

    await expect(getBlastRadiusJobBatch(req, res)).rejects.toThrow()
    expect(getAnalysisDetailsByIds).not.toHaveBeenCalled()
  })
})
