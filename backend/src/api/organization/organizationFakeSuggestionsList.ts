import { Error400 } from '@crowd/common'
import { findFakeOrganizationSuggestions } from '@crowd/data-access-layer/src/organizations'

import SequelizeRepository from '@/database/repositories/sequelizeRepository'

import Permissions from '../../security/permissions'
import OrganizationService from '../../services/organizationService'
import PermissionChecker from '../../services/user/permissionChecker'

/**
 * GET /organization/fake-suggestions
 * @summary List fake organization suggestions
 * @tag Organizations
 * @security Bearer
 * @queryParam {number} [offset]
 * @queryParam {number} [limit]
 * @queryParam {boolean} [detail] - When true, include the full organization profile for each row.
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

  const qx = SequelizeRepository.getQueryExecutor(req)
  const detail = req.query.detail === 'true'

  const payload = await findFakeOrganizationSuggestions(
    qx,
    segmentId,
    Number(req.query.limit ?? 20),
    Number(req.query.offset ?? 0),
  )

  if (detail && payload.rows.length > 0) {
    const organizationService = new OrganizationService(req)
    payload.rows = await Promise.all(
      payload.rows.map(async (row) => ({
        ...row,
        organization: await organizationService.findById(row.organizationId, segmentId),
      })),
    )
  }

  await req.responseHandler.success(req, res, payload)
}
