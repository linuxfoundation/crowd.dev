import { Error400, Error409 } from '@crowd/common'
import {
  deriveProjectIdentityFromRepoUrl,
  upsertProjectCatalogManualAction,
} from '@crowd/data-access-layer'

import SequelizeRepository from '@/database/repositories/sequelizeRepository'

import Permissions from '../../security/permissions'
import PermissionChecker from '../../services/user/permissionChecker'

const MANUALLY_ACCEPTABLE_ACTIONS = ['auto', 'evaluate', 'onboard'] as const
type ManualProjectCatalogAction = (typeof MANUALLY_ACCEPTABLE_ACTIONS)[number]

function isManualProjectCatalogAction(value: unknown): value is ManualProjectCatalogAction {
  return MANUALLY_ACCEPTABLE_ACTIONS.includes(value as ManualProjectCatalogAction)
}

function canonicalizeGithubRepoUrl(repoUrl: unknown): string | null {
  if (typeof repoUrl !== 'string') {
    return null
  }

  let url: URL
  try {
    url = new URL(repoUrl.replace(/^git@github\.com:/, 'https://github.com/'))
  } catch {
    return null
  }

  if (url.hostname !== 'github.com') {
    return null
  }

  const parts = url.pathname
    .replace(/^\//, '')
    .replace(/\/$/, '')
    .replace(/\.git$/, '')
    .split('/')

  if (parts.length !== 2 || !parts[0] || !parts[1]) {
    return null
  }

  return `https://github.com/${parts[0]}/${parts[1]}`
}

/**
 * POST /project-catalog
 * @summary Manually upsert a projectCatalog row for any pipeline phase
 * @tag Project Catalog
 * @security Bearer
 * @description Upserts a projectCatalog row with source 'manual' and the
 * given action, so it gets picked up by the corresponding pipeline stage
 * (discovery, evaluation, or onboarding) on its next run.
 * @bodyContent {string} repoUrl - GitHub repo URL (e.g. https://github.com/owner/repo)
 * @bodyContent {string} action - One of: auto, evaluate, onboard
 * @response 200 - Ok
 * @response 400 - Bad request
 * @response 401 - Unauthorized
 * @response 409 - Already onboarded or onboarding in progress
 */
export default async (req, res) => {
  new PermissionChecker(req).validateHas(Permissions.values.projectCatalogEdit)

  const repoUrl = canonicalizeGithubRepoUrl(req.body?.repoUrl)
  if (!repoUrl) {
    return req.responseHandler.error(req, res, new Error400(req.language))
  }

  const action = req.body?.action
  if (!isManualProjectCatalogAction(action)) {
    return req.responseHandler.error(req, res, new Error400(req.language))
  }

  const identity = deriveProjectIdentityFromRepoUrl(repoUrl)
  if (!identity) {
    return req.responseHandler.error(req, res, new Error400(req.language))
  }

  const qx = SequelizeRepository.getQueryExecutor(req)

  const payload = await upsertProjectCatalogManualAction(qx, {
    ...identity,
    repoUrl,
    action,
  })

  if (!payload) {
    return req.responseHandler.error(
      req,
      res,
      new Error409(req.language, 'errors.alreadyOnboarded', repoUrl),
    )
  }

  return req.responseHandler.success(req, res, payload)
}
