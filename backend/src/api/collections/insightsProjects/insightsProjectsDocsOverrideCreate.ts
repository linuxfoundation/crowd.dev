import { z } from 'zod'

import { CollectionService } from '@/services/collectionService'
import { validateOrThrow } from '@/utils/validation'

import Permissions from '../../../security/permissions'
import PermissionChecker from '../../../services/user/permissionChecker'

const bodySchema = z
  .object({
    docsUrl: z.string().url().optional(),
    noDocs: z.literal(true).optional(),
  })
  .refine((body) => (body.docsUrl === undefined) !== (body.noDocs === undefined), {
    message: 'Provide exactly one of docsUrl or noDocs: true',
  })

/**
 * POST /collections/insights-projects/{id}/docs-override
 * @summary Set a manual documentation override for an insights project
 * @tag Collections
 * @security Bearer
 * @description Sets an authoritative documentation URL for a project, or records that the
 *   project has no docs, skipping automatic discovery, and starts a docs readiness run for
 *   the project. The body must contain exactly one of `docsUrl` (a valid URL) or
 *   `noDocs: true`.
 * @pathParam {string} id - The ID of the insights project
 * @bodyContent {object} application/json
 * @response 200 - Ok
 * @response 400 - Invalid body: both, neither, or an invalid docsUrl / noDocs value
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
export default async (req, res) => {
  new PermissionChecker(req).validateHas(Permissions.values.collectionEdit)

  const { docsUrl } = validateOrThrow(bodySchema, req.body)

  const service = new CollectionService(req)
  // NULL (never '') is what marks "no docs" in the docs-readiness sweep.
  const payload = await service.createInsightsProjectDocOverride(req.params.id, docsUrl ?? null)

  await req.responseHandler.success(req, res, payload)
}
