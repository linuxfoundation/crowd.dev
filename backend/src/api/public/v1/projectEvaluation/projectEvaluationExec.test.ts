import type { Request, Response } from 'express'
import { describe, expect, it, vi } from 'vitest'

import { IProjectEvaluationResponse } from '@crowd/data-access-layer/src/project-catalog/types'

import { evaluateProject } from './evaluateProject'
import projectEvaluationExec from './projectEvaluationExec'

vi.mock('./evaluateProject', () => ({
  evaluateProject: vi.fn(),
}))

function mockReqRes(body: unknown) {
  const req = {
    body,
    database: { sequelize: {} },
    log: { warn: vi.fn(), error: vi.fn() },
  } as unknown as Request

  const json = vi.fn()
  const status = vi.fn().mockReturnValue({ json })
  const res = { status } as unknown as Response

  return { req, res, status, json }
}

describe('projectEvaluationExec', () => {
  it('validates the request and forwards it to evaluateProject', async () => {
    const evaluationResponse: IProjectEvaluationResponse = {
      outcome: 'onboard',
      evaluationResult: 'true',
      evaluationReason: null,
      metrics: { model: 'test-model', inputTokens: 10, outputTokens: 5, seconds: 1 },
    }
    vi.mocked(evaluateProject).mockResolvedValue(evaluationResponse)

    const { req, res, status, json } = mockReqRes({
      id: 'catalog-1',
      repoUrl: 'https://github.com/foo/bar',
      repoName: 'bar',
      projectSlug: 'foo',
      lfCriticalityScore: null,
      source: null,
    })

    await projectEvaluationExec(req, res)

    expect(evaluateProject).toHaveBeenCalledWith(
      {
        id: 'catalog-1',
        repoUrl: 'https://github.com/foo/bar',
        repoName: 'bar',
        projectSlug: 'foo',
        lfCriticalityScore: null,
        source: null,
      },
      expect.anything(),
      expect.objectContaining({
        accessKeyId: process.env.CROWD_AWS_BEDROCK_ACCESS_KEY_ID,
        secretAccessKey: process.env.CROWD_AWS_BEDROCK_SECRET_ACCESS_KEY,
      }),
      req.log,
    )
    expect(status).toHaveBeenCalledWith(200)
    expect(json).toHaveBeenCalledWith(evaluationResponse)
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
