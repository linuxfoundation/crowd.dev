import Permissions from '../../security/permissions'
import MemberService from '../../services/memberService'
import PermissionChecker from '../../services/user/permissionChecker'

/**
 * GET /member/{id}
 * @summary Find a member
 * @tag Members
 * @security Bearer
 * @description Find a single member by ID.
 * @pathParam {string} id - The ID of the member
 * @response 200 - Ok
 * @responseContent {MemberResponse} 200.application/json
 * @responseExample {MemberFind} 200.application/json.Member
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
export default async (req, res) => {
  new PermissionChecker(req).validateHas(Permissions.values.memberRead)

  const segmentId = req.query.segments?.length > 0 ? req.query.segments[0] : null
  const includeAllAttributes =
    req.query.includeAllAttributes === 'true' || req.query.includeAllAttributes === true

  if (!segmentId) {
    await req.responseHandler.error(req, res, {
      code: 400,
      message: 'Segment ID is required',
    })
    return
  }

  const payload = await new MemberService(req).findById(
    req.params.id,
    segmentId,
    req.query.include,
    includeAllAttributes,
  )

  await req.responseHandler.success(req, res, payload)
}
