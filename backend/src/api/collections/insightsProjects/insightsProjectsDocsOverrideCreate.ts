import { z } from 'zod'

import { CollectionService } from '@/services/collectionService'
import { validateOrThrow } from '@/utils/validation'

import Permissions from '../../../security/permissions'
import PermissionChecker from '../../../services/user/permissionChecker'

const bodySchema = z.object({
  docsUrl: z.string().url(),
})

/**
 * POST /collections/insights-projects/{id}/docs-override
 * @summary Set a manual documentation URL override for an insights project
 * @tag Collections
 * @security Bearer
 * @description Sets an authoritative documentation URL for a project, skipping automatic
 *   discovery, and starts a docs readiness run for the project.
 * @pathParam {string} id - The ID of the insights project
 * @bodyContent {object} application/json
 * @response 200 - Ok
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
export default async (req, res) => {
  new PermissionChecker(req).validateHas(Permissions.values.collectionEdit)

  const { docsUrl } = validateOrThrow(bodySchema, req.body)

  const service = new CollectionService(req)
  const payload = await service.createInsightsProjectDocOverride(req.params.id, docsUrl)

  await req.responseHandler.success(req, res, payload)
}
