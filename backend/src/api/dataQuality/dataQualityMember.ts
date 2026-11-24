import { getRequestedSegmentIds } from '@/middlewares/segmentMiddleware'
import DataQualityService from '@/services/dataQualityService'

import Permissions from '../../security/permissions'
import PermissionChecker from '../../services/user/permissionChecker'

/**
 * GET /data-quality/member
 * @summary Find a member data issues
 * @tag Data Quality
 * @security Bearer
 * @description Find a data quality issues for members
 * @response 200 - Ok
 * @responseContent {DataQualityResponse} 200.application/json
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
export default async (req, res) => {
  new PermissionChecker(req).validateHas(Permissions.values.memberRead)

  const segmentId = getRequestedSegmentIds(req)[0]
  const payload = await new DataQualityService(req).findMemberIssues(req.query, segmentId)

  await req.responseHandler.success(req, res, payload)
}
