import type { Request, Response } from 'express'
import { describe, expect, it, vi } from 'vitest'

import projectEvaluationExec from './projectEvaluationExec'

function mockReqRes(body: unknown) {
  const req = { body } as unknown as Request

  const json = vi.fn()
  const status = vi.fn().mockReturnValue({ json })
  const res = { status } as unknown as Response

  return { req, res, status, json }
}

describe('projectEvaluationExec', () => {
  it('responds 200 with an unsure not-implemented outcome for a valid request', async () => {
    const { req, res, status, json } = mockReqRes({
      id: 'catalog-1',
      repoUrl: 'https://github.com/foo/bar',
      repoName: 'bar',
      projectSlug: 'foo',
      lfCriticalityScore: null,
      source: null,
    })

    await projectEvaluationExec(req, res)

    expect(status).toHaveBeenCalledWith(200)
    expect(json).toHaveBeenCalledWith({
      outcome: 'unsure',
      evaluationResult: 'not_implemented',
      evaluationReason: 'Evaluation decision logic is not implemented yet',
      metrics: null,
    })
  })

  it('rejects a request missing required fields', async () => {
    const { req, res } = mockReqRes({ repoUrl: 'https://github.com/foo/bar' })

    await expect(projectEvaluationExec(req, res)).rejects.toThrow()
  })

  it('rejects an invalid repoUrl', async () => {
    const { req, res } = mockReqRes({
      id: 'catalog-1',
      repoUrl: 'not-a-url',
      repoName: 'bar',
      projectSlug: 'foo',
      lfCriticalityScore: null,
      source: null,
    })

    await expect(projectEvaluationExec(req, res)).rejects.toThrow()
  })
})
