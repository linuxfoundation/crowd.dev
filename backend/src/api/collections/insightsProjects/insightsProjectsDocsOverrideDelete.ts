import { CollectionService } from '@/services/collectionService'

import Permissions from '../../../security/permissions'
import PermissionChecker from '../../../services/user/permissionChecker'

/**
 * DELETE /collections/insights-projects/{id}/docs-override
 * @summary Revert the manual documentation URL override for an insights project
 * @tag Collections
 * @security Bearer
 * @description Deactivates the active documentation URL override for a project and starts a
 *   docs readiness run so it re-discovers a URL naturally.
 * @pathParam {string} id - The ID of the insights project
 * @response 200 - Ok
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
export default async (req, res) => {
  new PermissionChecker(req).validateHas(Permissions.values.collectionEdit)

  const service = new CollectionService(req)
  const payload = await service.revertInsightsProjectDocOverride(req.params.id)

  await req.responseHandler.success(req, res, payload)
}
