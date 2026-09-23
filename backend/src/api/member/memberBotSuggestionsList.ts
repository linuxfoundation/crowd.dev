import SequelizeRepository from '@/database/repositories/sequelizeRepository'
import { Error400 } from '@crowd/common'
import {
  fetchMemberBotSuggestionsBySegment,
  fetchMemberProfile,
} from '@crowd/data-access-layer/src/members'

import Permissions from '../../security/permissions'
import PermissionChecker from '../../services/user/permissionChecker'

/**
 * GET /member/bot-suggestions
 * @summary List member bot suggestions
 * @tag Members
 * @security Bearer
 * @description List member bot suggestions with pagination
 * @queryParam {number} [offset] - Skip the first n results. Default 0.
 * @queryParam {number} [limit] - Limit the number of results. Default 20.
 * @queryParam {boolean} [detail] - When true, include the full member profile for each row.
 * @response 200 - Ok
 * @responseContent {MemberList} 200.application/json
 * @response 401 - Unauthorized
 * @response 429 - Too many requests
 */
export default async (req, res) => {
  new PermissionChecker(req).validateHas(Permissions.values.memberRead)

  const segmentId = SequelizeRepository.getSegmentIds(req)[0]
  if (!segmentId) {
    throw new Error400(req.language, 'member.segmentsRequired')
  }

  const qx = SequelizeRepository.getQueryExecutor(req)
  const detail = String(req.query.detail) === 'true'

  const payload = await fetchMemberBotSuggestionsBySegment(
    qx,
    segmentId,
    Number(req.query.limit ?? 20),
    Number(req.query.offset ?? 0),
  )

  if (detail && payload.rows.length > 0) {
    payload.rows = await Promise.all(
      payload.rows.map(async (row) => ({
        ...row,
        member: await fetchMemberProfile(qx, row.memberId),
      })),
    )
  }

  await req.responseHandler.success(req, res, payload)
}
