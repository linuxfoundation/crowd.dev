import { deleteFakeOrganizationSuggestion } from '@crowd/data-access-layer/src/organizations'

import SequelizeRepository from '@/database/repositories/sequelizeRepository'

import Permissions from '../../security/permissions'
import PermissionChecker from '../../services/user/permissionChecker'

/**
 * DELETE /organization/{organizationId}/fake-suggestion
 * @summary Dismiss a fake organization suggestion
 * @tag Organizations
 * @security Bearer
 * @pathParam {string} organizationId
 * @response 200 - Ok
 * @response 401 - Unauthorized
 * @response 429 - Too many requests
 */
export default async (req, res) => {
  new PermissionChecker(req).validateHas(Permissions.values.organizationEdit)

  await deleteFakeOrganizationSuggestion(
    SequelizeRepository.getQueryExecutor(req),
    req.params.organizationId,
  )

  await req.responseHandler.success(req, res, true)
}
