import { Error400 } from '@crowd/common'
import { findFakeOrganizationSuggestions } from '@crowd/data-access-layer/src/organizations'

import SequelizeRepository from '@/database/repositories/sequelizeRepository'

import Permissions from '../../security/permissions'
import PermissionChecker from '../../services/user/permissionChecker'

/**
 * GET /organization/fake-suggestions
 * @summary List fake organization suggestions
 * @tag Organizations
 * @security Bearer
 * @queryParam {number} [offset]
 * @queryParam {number} [limit]
 * @response 200 - Ok
 * @response 401 - Unauthorized
 * @response 429 - Too many requests
 */
export default async (req, res) => {
  new PermissionChecker(req).validateHas(Permissions.values.organizationRead)

  const segmentId = SequelizeRepository.getSegmentIds(req)[0]
  if (!segmentId) {
    throw new Error400(req.language, 'member.segmentsRequired')
  }

  const payload = await findFakeOrganizationSuggestions(
    SequelizeRepository.getQueryExecutor(req),
    segmentId,
    Number(req.query.limit) || 20,
    Number(req.query.offset) || 0,
  )

  await req.responseHandler.success(req, res, payload)
}
