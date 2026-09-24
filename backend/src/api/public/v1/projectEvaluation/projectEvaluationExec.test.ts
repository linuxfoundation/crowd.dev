import type { Request, Response } from 'express'
import { describe, expect, it, vi } from 'vitest'

import { IProjectEvaluationResponse } from '@crowd/data-access-layer/src/project-catalog/types'

import { evaluateProject } from './evaluateProject'
import projectEvaluationExec from './projectEvaluationExec'

vi.mock('./evaluateProject', () => ({
  evaluateProject: vi.fn(),
}))

function mockReqRes(body: unknown, actorId = 'test-actor', apiKeyId?: string) {
  const req = {
    body,
    database: { sequelize: {} },
    log: { warn: vi.fn(), error: vi.fn() },
    actor: { type: 'service', id: actorId, scopes: [], apiKeyId },
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
      expect.any(Function),
    )
    expect(status).toHaveBeenCalledWith(200)
    expect(json).toHaveBeenCalledWith(evaluationResponse)
  })

  it('keys the daily LLM reservation on the actor id', async () => {
    vi.mocked(evaluateProject).mockImplementation(async (_input, _qx, _creds, _log, reserve) => {
      reserve?.()
      return {
        outcome: 'onboard',
        evaluationResult: 'true',
        evaluationReason: null,
        metrics: null,
      }
    })

    const { req, res } = mockReqRes(
      {
        id: 'catalog-1',
        repoUrl: 'https://github.com/foo/bar',
        repoName: 'bar',
        projectSlug: 'foo',
        lfCriticalityScore: null,
        source: null,
      },
      'projects-evaluation-worker',
    )

    process.env.CROWD_PROJECT_EVALUATION_DAILY_LLM_CAP = '1'
    try {
      await projectEvaluationExec(req, res)
      await expect(projectEvaluationExec(req, res)).rejects.toThrow()

      const { req: otherReq, res: otherRes } = mockReqRes(req.body, 'a-different-actor')
      await expect(projectEvaluationExec(otherReq, otherRes)).resolves.not.toThrow()
    } finally {
      delete process.env.CROWD_PROJECT_EVALUATION_DAILY_LLM_CAP
    }
  })

  it('isolates the reservation counter by apiKeyId even when names collide', async () => {
    vi.mocked(evaluateProject).mockImplementation(async (_input, _qx, _creds, _log, reserve) => {
      reserve?.()
      return {
        outcome: 'onboard',
        evaluationResult: 'true',
        evaluationReason: null,
        metrics: null,
      }
    })

    const body = {
      id: 'catalog-1',
      repoUrl: 'https://github.com/foo/bar',
      repoName: 'bar',
      projectSlug: 'foo',
      lfCriticalityScore: null,
      source: null,
    }

    process.env.CROWD_PROJECT_EVALUATION_DAILY_LLM_CAP = '1'
    try {
      const { req, res } = mockReqRes(body, 'shared-name', 'key-id-a')
      await projectEvaluationExec(req, res)
      await expect(projectEvaluationExec(req, res)).rejects.toThrow()

      const { req: otherReq, res: otherRes } = mockReqRes(body, 'shared-name', 'key-id-b')
      await expect(projectEvaluationExec(otherReq, otherRes)).resolves.not.toThrow()
    } finally {
      delete process.env.CROWD_PROJECT_EVALUATION_DAILY_LLM_CAP
    }
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
