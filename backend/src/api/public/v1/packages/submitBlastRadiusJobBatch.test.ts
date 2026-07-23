import type { Request, Response } from 'express'
import { describe, expect, it, vi } from 'vitest'

import { submitBlastRadiusJobBatch } from './submitBlastRadiusJobBatch'

const { start, createAnalysis, failAnalysis } = vi.hoisted(() => ({
  start: vi.fn().mockResolvedValue(undefined),
  createAnalysis: vi.fn().mockResolvedValue(undefined),
  failAnalysis: vi.fn().mockResolvedValue(undefined),
}))

vi.mock('@/db/packagesTemporal', () => ({
  getPackagesTemporalClient: vi.fn().mockResolvedValue({ workflow: { start } }),
}))

vi.mock('@/db/packagesDb', () => ({
  getPackagesQx: vi.fn().mockResolvedValue({}),
}))

vi.mock('@crowd/data-access-layer/src/packages/blastRadius', () => ({
  createAnalysis,
  failAnalysis,
}))

function mockReqRes(body: unknown) {
  start.mockClear()
  createAnalysis.mockClear()
  failAnalysis.mockClear()

  const req = { body } as unknown as Request

  const json = vi.fn()
  const status = vi.fn().mockReturnValue({ json })
  const res = { status } as unknown as Response

  return { req, res, start, status, json }
}

describe('submitBlastRadiusJobBatch', () => {
  it('starts one workflow per job and responds 202 with results in request order', async () => {
    const { req, res, status, json } = mockReqRes({
      jobs: [
        { advisoryId: 'GHSA-jf85-cpcp-j695', ecosystem: 'npm' },
        { advisoryId: 'GHSA-652q-gvq3-74qv', package: 'pkg:npm/lodash', ecosystem: 'npm' },
      ],
    })

    await submitBlastRadiusJobBatch(req, res)

    expect(createAnalysis).toHaveBeenCalledTimes(2)
    expect(start).toHaveBeenCalledTimes(2)
    expect(status).toHaveBeenCalledWith(202)

    const [{ results }] = json.mock.calls[0]
    expect(results).toHaveLength(2)
    expect(results[0]).toMatchObject({
      advisoryId: 'GHSA-jf85-cpcp-j695',
      package: null,
      ecosystem: 'npm',
      status: 'pending',
    })
    expect(results[1]).toMatchObject({
      advisoryId: 'GHSA-652q-gvq3-74qv',
      package: 'pkg:npm/lodash',
      ecosystem: 'npm',
      status: 'pending',
    })
    expect(typeof results[0].analysisId).toBe('string')
    expect(typeof results[1].analysisId).toBe('string')
  })

  it('isolates a per-job workflow.start failure to that job only', async () => {
    const { req, res, json } = mockReqRes({
      jobs: [
        { advisoryId: 'GHSA-jf85-cpcp-j695', ecosystem: 'npm' },
        { advisoryId: 'GHSA-652q-gvq3-74qv', ecosystem: 'npm' },
      ],
    })
    start.mockResolvedValueOnce(undefined).mockRejectedValueOnce(new Error('temporal unreachable'))

    await submitBlastRadiusJobBatch(req, res)

    const [{ results }] = json.mock.calls[0]
    expect(results).toHaveLength(2)
    expect(results[0]).toMatchObject({ advisoryId: 'GHSA-jf85-cpcp-j695', status: 'pending' })
    expect(results[1]).toMatchObject({ advisoryId: 'GHSA-652q-gvq3-74qv', status: 'failed' })

    expect(failAnalysis).toHaveBeenCalledTimes(1)
    const [, , errorMessage] = failAnalysis.mock.calls[0]
    expect(errorMessage).toBe('temporal unreachable')
  })

  it('rejects a batch containing an unsupported ecosystem without submitting any job', async () => {
    const { req, res, start } = mockReqRes({
      jobs: [
        { advisoryId: 'GHSA-jf85-cpcp-j695', ecosystem: 'npm' },
        { advisoryId: 'GHSA-652q-gvq3-74qv', ecosystem: 'maven' },
      ],
    })

    await expect(submitBlastRadiusJobBatch(req, res)).rejects.toThrow()
    expect(start).not.toHaveBeenCalled()
    expect(createAnalysis).not.toHaveBeenCalled()
  })

  it('rejects a batch with more than 20 jobs without submitting any job', async () => {
    const jobs = Array.from({ length: 21 }, () => ({
      advisoryId: 'GHSA-jf85-cpcp-j695',
      ecosystem: 'npm',
    }))
    const { req, res, start } = mockReqRes({ jobs })

    await expect(submitBlastRadiusJobBatch(req, res)).rejects.toThrow()
    expect(start).not.toHaveBeenCalled()
    expect(createAnalysis).not.toHaveBeenCalled()
  })

  it('rejects an empty jobs array', async () => {
    const { req, res, start } = mockReqRes({ jobs: [] })

    await expect(submitBlastRadiusJobBatch(req, res)).rejects.toThrow()
    expect(start).not.toHaveBeenCalled()
  })
})
