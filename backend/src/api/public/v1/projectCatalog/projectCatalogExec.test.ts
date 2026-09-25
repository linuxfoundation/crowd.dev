import type { Request, Response } from 'express'
import { beforeEach, describe, expect, it, vi } from 'vitest'

vi.mock('@crowd/data-access-layer', () => ({
  deriveProjectIdentityFromRepoUrl: vi.fn(() => ({ projectSlug: 'foo', repoName: 'bar' })),
  upsertProjectCatalogManualAction: vi.fn(async () => ({ repoUrl: 'https://github.com/foo/bar' })),
}))

import { upsertProjectCatalogManualAction } from '@crowd/data-access-layer'

import projectCatalogExec from './projectCatalogExec'

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

describe('projectCatalogExec', () => {
  const body = { repoUrl: 'https://github.com/foo/bar', action: 'evaluate' as const }

  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('keys the daily reservation on the actor id', async () => {
    const { req, res } = mockReqRes(body, 'projects-catalog-worker')

    process.env.CROWD_PROJECT_CATALOG_DAILY_CAP = '1'
    try {
      await projectCatalogExec(req, res)
      await expect(projectCatalogExec(req, res)).rejects.toThrow()

      const { req: otherReq, res: otherRes } = mockReqRes(body, 'a-different-actor')
      await expect(projectCatalogExec(otherReq, otherRes)).resolves.not.toThrow()
    } finally {
      delete process.env.CROWD_PROJECT_CATALOG_DAILY_CAP
    }
  })

  it('isolates the reservation counter by apiKeyId even when names collide', async () => {
    process.env.CROWD_PROJECT_CATALOG_DAILY_CAP = '1'
    try {
      const { req, res } = mockReqRes(body, 'shared-name', 'key-id-a')
      await projectCatalogExec(req, res)
      await expect(projectCatalogExec(req, res)).rejects.toThrow()

      const { req: otherReq, res: otherRes } = mockReqRes(body, 'shared-name', 'key-id-b')
      await expect(projectCatalogExec(otherReq, otherRes)).resolves.not.toThrow()
    } finally {
      delete process.env.CROWD_PROJECT_CATALOG_DAILY_CAP
    }
  })

  it('reserves budget before writing to the catalog', async () => {
    const { req, res } = mockReqRes(body, 'projects-catalog-worker-2')

    process.env.CROWD_PROJECT_CATALOG_DAILY_CAP = '1'
    try {
      await projectCatalogExec(req, res)
      await expect(projectCatalogExec(req, res)).rejects.toThrow()

      expect(upsertProjectCatalogManualAction).toHaveBeenCalledTimes(1)
    } finally {
      delete process.env.CROWD_PROJECT_CATALOG_DAILY_CAP
    }
  })
})
