import type { Request, Response } from 'express'
import { beforeEach, describe, expect, it, vi } from 'vitest'

vi.mock('@crowd/project-onboarding', () => ({
  onboardProject: vi.fn(async () => ({
    outcome: 'onboarded',
    segmentId: 'segment-1',
    error: null,
  })),
}))

import { onboardProject } from '@crowd/project-onboarding'

import projectOnboardingExec from './projectOnboardingExec'

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

describe('projectOnboardingExec', () => {
  const body = {
    repoUrl: 'https://github.com/foo/bar',
    repoName: 'bar',
    projectSlug: 'foo',
  }

  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('keys the daily reservation on the actor id', async () => {
    const { req, res } = mockReqRes(body, 'projects-onboarding-worker')

    process.env.CROWD_PROJECT_ONBOARDING_DAILY_CAP = '1'
    try {
      await projectOnboardingExec(req, res)
      await expect(projectOnboardingExec(req, res)).rejects.toThrow()

      const { req: otherReq, res: otherRes } = mockReqRes(body, 'a-different-actor')
      await expect(projectOnboardingExec(otherReq, otherRes)).resolves.not.toThrow()
    } finally {
      delete process.env.CROWD_PROJECT_ONBOARDING_DAILY_CAP
    }
  })

  it('isolates the reservation counter by apiKeyId even when names collide', async () => {
    process.env.CROWD_PROJECT_ONBOARDING_DAILY_CAP = '1'
    try {
      const { req, res } = mockReqRes(body, 'shared-name', 'key-id-a')
      await projectOnboardingExec(req, res)
      await expect(projectOnboardingExec(req, res)).rejects.toThrow()

      const { req: otherReq, res: otherRes } = mockReqRes(body, 'shared-name', 'key-id-b')
      await expect(projectOnboardingExec(otherReq, otherRes)).resolves.not.toThrow()
    } finally {
      delete process.env.CROWD_PROJECT_ONBOARDING_DAILY_CAP
    }
  })

  it('reserves budget before calling onboardProject', async () => {
    const { req, res } = mockReqRes(body, 'projects-onboarding-worker-2')

    process.env.CROWD_PROJECT_ONBOARDING_DAILY_CAP = '1'
    try {
      await projectOnboardingExec(req, res)
      await expect(projectOnboardingExec(req, res)).rejects.toThrow()

      expect(onboardProject).toHaveBeenCalledTimes(1)
    } finally {
      delete process.env.CROWD_PROJECT_ONBOARDING_DAILY_CAP
    }
  })
})
