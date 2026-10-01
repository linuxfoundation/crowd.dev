import type { Request, Response } from 'express'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { PRECHECK_SKIP_REASONS } from '@crowd/data-access-layer/src/project-catalog/precheck'
import { IProjectEvaluationResponse } from '@crowd/data-access-layer/src/project-catalog/types'

import { evaluateProject } from './evaluateProject'
import projectEvaluationExec from './projectEvaluationExec'
import { runPrecheck } from './runPrecheck'

vi.mock('./evaluateProject', () => ({
  evaluateProject: vi.fn(),
}))

vi.mock('./runPrecheck', async (importOriginal) => ({
  ...(await importOriginal<typeof import('./runPrecheck')>()),
  runPrecheck: vi.fn(),
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

  describe('precheck flag', () => {
    const body = {
      id: 'catalog-1',
      repoUrl: 'https://github.com/foo/bar',
      repoName: 'bar',
      projectSlug: 'foo',
      lfCriticalityScore: null,
      source: null,
    }
    const onboardResponse: IProjectEvaluationResponse = {
      outcome: 'onboard',
      evaluationResult: 'true',
      evaluationReason: null,
      metrics: null,
    }

    beforeEach(() => {
      vi.mocked(evaluateProject).mockReset().mockResolvedValue(onboardResponse)
      vi.mocked(runPrecheck).mockReset()
    })

    it('does not run the pre-check when the flag is absent', async () => {
      const { req, res, json } = mockReqRes(body)

      await projectEvaluationExec(req, res)

      expect(runPrecheck).not.toHaveBeenCalled()
      expect(evaluateProject).toHaveBeenCalledTimes(1)
      expect(json).toHaveBeenCalledWith(onboardResponse)
    })

    it('does not run the pre-check when the flag is false', async () => {
      const { req, res } = mockReqRes({ ...body, precheck: false })

      await projectEvaluationExec(req, res)

      expect(runPrecheck).not.toHaveBeenCalled()
      expect(evaluateProject).toHaveBeenCalledTimes(1)
    })

    it('skips without calling the LLM when the pre-check matches', async () => {
      vi.mocked(runPrecheck).mockResolvedValue(PRECHECK_SKIP_REASONS.lfOwner)
      const { req, res, status, json } = mockReqRes({ ...body, precheck: true })

      await projectEvaluationExec(req, res)

      expect(runPrecheck).toHaveBeenCalledWith(expect.anything(), body.repoUrl)
      expect(evaluateProject).not.toHaveBeenCalled()
      expect(status).toHaveBeenCalledWith(200)
      expect(json).toHaveBeenCalledWith({
        outcome: 'skip',
        evaluationResult: 'false',
        evaluationReason: PRECHECK_SKIP_REASONS.lfOwner,
        metrics: null,
      })
    })

    it('falls through to the LLM evaluation when the pre-check does not match', async () => {
      vi.mocked(runPrecheck).mockResolvedValue(null)
      const { req, res, json } = mockReqRes({ ...body, precheck: true })

      await projectEvaluationExec(req, res)

      expect(runPrecheck).toHaveBeenCalledTimes(1)
      expect(evaluateProject).toHaveBeenCalledTimes(1)
      expect(json).toHaveBeenCalledWith(onboardResponse)
    })

    it('lets a pre-check failure propagate instead of masking it as a result', async () => {
      vi.mocked(runPrecheck).mockRejectedValue(new Error('db down'))
      const { req, res } = mockReqRes({ ...body, precheck: true })

      await expect(projectEvaluationExec(req, res)).rejects.toThrow('db down')
      expect(evaluateProject).not.toHaveBeenCalled()
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
