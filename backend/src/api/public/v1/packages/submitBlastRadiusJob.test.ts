import type { Request, Response } from 'express'
import { describe, expect, it, vi } from 'vitest'

import { submitBlastRadiusJob } from './submitBlastRadiusJob'

const { start, createAnalysis, failAnalysis, getRecentDoneAnalysis } = vi.hoisted(() => ({
  start: vi.fn().mockResolvedValue(undefined),
  createAnalysis: vi.fn().mockResolvedValue(undefined),
  failAnalysis: vi.fn().mockResolvedValue(undefined),
  getRecentDoneAnalysis: vi.fn(),
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
  getRecentDoneAnalysis,
}))

function mockReqRes(body: unknown) {
  start.mockClear()
  createAnalysis.mockClear()
  failAnalysis.mockClear()
  getRecentDoneAnalysis.mockClear()
  getRecentDoneAnalysis.mockResolvedValue(null)

  const req = { body } as unknown as Request

  const json = vi.fn()
  const status = vi.fn().mockReturnValue({ json })
  const res = { status } as unknown as Response

  return { req, res, start, status, json }
}

describe('submitBlastRadiusJob', () => {
  it('starts analyzeBlastRadius on the blast-radius-worker task queue and responds 202 pending', async () => {
    const { req, res, start, status, json } = mockReqRes({
      advisoryId: 'GHSA-jf85-cpcp-j695',
      ecosystem: 'npm',
    })

    await submitBlastRadiusJob(req, res)

    expect(createAnalysis).toHaveBeenCalledTimes(1)
    expect(createAnalysis.mock.calls[0][1]).toMatchObject({
      advisoryOsvId: 'GHSA-jf85-cpcp-j695',
      packageName: null,
      ecosystem: 'npm',
      force: false,
    })

    expect(start).toHaveBeenCalledTimes(1)
    const [workflowType, options] = start.mock.calls[0]
    expect(workflowType).toBe('analyzeBlastRadius')
    expect(options.taskQueue).toBe('blast-radius-worker')
    expect(options.workflowId).toMatch(/^blast-radius-analysis\//)
    expect(options.args[0]).toMatchObject({
      advisoryId: 'GHSA-jf85-cpcp-j695',
      package: null,
      ecosystem: 'npm',
      force: false,
    })
    expect(typeof options.args[0].analysisId).toBe('string')

    expect(status).toHaveBeenCalledWith(202)
    expect(json).toHaveBeenCalledWith(
      expect.objectContaining({
        advisoryId: 'GHSA-jf85-cpcp-j695',
        package: null,
        ecosystem: 'npm',
        status: 'pending',
      }),
    )
  })

  it('passes package/ecosystem/force through to the workflow args and response', async () => {
    const { req, res, start, json } = mockReqRes({
      advisoryId: 'GHSA-jf85-cpcp-j695',
      package: 'pkg:npm/lodash',
      ecosystem: 'npm',
      force: true,
    })

    await submitBlastRadiusJob(req, res)

    const [, options] = start.mock.calls[0]
    expect(options.args[0]).toMatchObject({
      package: 'pkg:npm/lodash',
      ecosystem: 'npm',
      force: true,
    })
    expect(json).toHaveBeenCalledWith(
      expect.objectContaining({ package: 'pkg:npm/lodash', ecosystem: 'npm' }),
    )
  })

  it('rejects a request missing advisoryId without starting a workflow', async () => {
    const { req, res, start } = mockReqRes({ ecosystem: 'npm' })

    await expect(submitBlastRadiusJob(req, res)).rejects.toThrow()
    expect(start).not.toHaveBeenCalled()
    expect(createAnalysis).not.toHaveBeenCalled()
  })

  it('rejects an unsupported ecosystem without starting a workflow', async () => {
    const { req, res, start } = mockReqRes({
      advisoryId: 'GHSA-jf85-cpcp-j695',
      ecosystem: 'pypi',
    })

    await expect(submitBlastRadiusJob(req, res)).rejects.toThrow(/not supported/)
    expect(start).not.toHaveBeenCalled()
    expect(createAnalysis).not.toHaveBeenCalled()
  })

  it('rejects a missing ecosystem without starting a workflow', async () => {
    const { req, res, start } = mockReqRes({ advisoryId: 'GHSA-jf85-cpcp-j695' })

    await expect(submitBlastRadiusJob(req, res)).rejects.toThrow(/not supported/)
    expect(start).not.toHaveBeenCalled()
    expect(createAnalysis).not.toHaveBeenCalled()
  })

  it('rejects an advisoryId that is not a GHSA or CVE identifier without starting a workflow', async () => {
    const { req, res, start } = mockReqRes({ advisoryId: 'foo', ecosystem: 'npm' })

    await expect(submitBlastRadiusJob(req, res)).rejects.toThrow()
    expect(start).not.toHaveBeenCalled()
    expect(createAnalysis).not.toHaveBeenCalled()
  })

  it('marks the analysis failed and rethrows when workflow.start fails', async () => {
    const { req, res } = mockReqRes({
      advisoryId: 'GHSA-jf85-cpcp-j695',
      ecosystem: 'npm',
    })
    start.mockRejectedValueOnce(new Error('temporal unreachable'))

    await expect(submitBlastRadiusJob(req, res)).rejects.toThrow('temporal unreachable')

    expect(failAnalysis).toHaveBeenCalledTimes(1)
    const [, input, errorMessage] = failAnalysis.mock.calls[0]
    expect(input).toMatchObject({
      advisoryOsvId: 'GHSA-jf85-cpcp-j695',
      packageName: null,
      ecosystem: 'npm',
      force: false,
    })
    expect(errorMessage).toBe('temporal unreachable')
  })

  it('reuses a recent done analysis instead of starting a workflow', async () => {
    const { req, res, start, status, json } = mockReqRes({
      advisoryId: 'GHSA-jf85-cpcp-j695',
      ecosystem: 'npm',
    })
    getRecentDoneAnalysis.mockResolvedValue({
      id: 'cached-analysis-id',
      advisory_osv_id: 'GHSA-jf85-cpcp-j695',
      package_name: null,
      ecosystem: 'npm',
      status: 'done',
      error: null,
      candidates_considered: 5,
      started_at: '2026-07-01T00:00:00.000Z',
      completed_at: '2026-07-01T01:00:00.000Z',
    })

    await submitBlastRadiusJob(req, res)

    expect(createAnalysis).not.toHaveBeenCalled()
    expect(start).not.toHaveBeenCalled()
    expect(status).toHaveBeenCalledWith(202)
    expect(json).toHaveBeenCalledWith(
      expect.objectContaining({
        analysisId: 'cached-analysis-id',
        advisoryId: 'GHSA-jf85-cpcp-j695',
        package: null,
        ecosystem: 'npm',
        status: 'done',
      }),
    )
  })

  it('bypasses the cache and starts a new workflow when force is true, even with a recent done analysis', async () => {
    const { req, res, start } = mockReqRes({
      advisoryId: 'GHSA-jf85-cpcp-j695',
      ecosystem: 'npm',
      force: true,
    })
    getRecentDoneAnalysis.mockResolvedValue({
      id: 'cached-analysis-id',
      advisory_osv_id: 'GHSA-jf85-cpcp-j695',
      package_name: null,
      ecosystem: 'npm',
      status: 'done',
      error: null,
      candidates_considered: 5,
      started_at: '2026-07-01T00:00:00.000Z',
      completed_at: '2026-07-01T01:00:00.000Z',
    })

    await submitBlastRadiusJob(req, res)

    expect(getRecentDoneAnalysis).not.toHaveBeenCalled()
    expect(createAnalysis).toHaveBeenCalledTimes(1)
    expect(start).toHaveBeenCalledTimes(1)
  })
})
