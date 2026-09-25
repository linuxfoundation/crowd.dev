import type { Request, Response } from 'express'

import { optionsQx } from '@/database/sequelizeQueryExecutor'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'
import { BadRequestError, ConflictError, canonicalizeGithubRepoUrl } from '@crowd/common'
import {
  deriveProjectIdentityFromRepoUrl,
  upsertProjectCatalogManualAction,
} from '@crowd/data-access-layer'

import { projectCatalogUpsertRequestSchema } from './types'

export default async (req: Request, res: Response): Promise<void> => {
  const parsed = validateOrThrow(projectCatalogUpsertRequestSchema, req.body)

  const repoUrl = canonicalizeGithubRepoUrl(parsed.repoUrl)
  if (!repoUrl) {
    throw new BadRequestError(`Invalid GitHub repo URL: ${parsed.repoUrl}`)
  }

  const identity = deriveProjectIdentityFromRepoUrl(repoUrl)
  if (!identity) {
    throw new BadRequestError(`Unable to derive project identity from repo URL: ${repoUrl}`)
  }

  const payload = await upsertProjectCatalogManualAction(optionsQx(req), {
    ...identity,
    repoUrl,
    action: parsed.action,
  })

  if (!payload) {
    throw new ConflictError(
      `Project catalog entry for ${repoUrl} is already onboarded or onboarding`,
    )
  }

  ok(res, payload)
}
