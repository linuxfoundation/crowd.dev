import type { Request, Response } from 'express'
import { describe, expect, it, vi } from 'vitest'

import { submitBlastRadiusJob } from './submitBlastRadiusJob'

const { start, createAnalysis } = vi.hoisted(() => ({
  start: vi.fn().mockResolvedValue(undefined),
  createAnalysis: vi.fn().mockResolvedValue(undefined),
}))

vi.mock('@/db/packagesTemporal', () => ({
  getPackagesTemporalClient: vi.fn().mockResolvedValue({ workflow: { start } }),
}))

vi.mock('@/db/packagesDb', () => ({
  getPackagesQx: vi.fn().mockResolvedValue({}),
}))

vi.mock('@crowd/data-access-layer/src/packages/blastRadius', () => ({
  createAnalysis,
}))

function mockReqRes(body: unknown) {
  start.mockClear()
  createAnalysis.mockClear()

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
})
