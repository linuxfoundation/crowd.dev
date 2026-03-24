import Permissions from '../../security/permissions'
import MemberService from '../../services/memberService'
import PermissionChecker from '../../services/user/permissionChecker'

/**
 * PUT /member/{id}
 * @summary Update a member
 * @tag Members
 * @security Bearer
 * @description Update a member
 * @pathParam {string} id - The ID of the member
 * @bodyContent {MemberUpdateInput} application/json
 * @response 200 - Ok
 * @responseContent {Member} 200.application/json
 * @responseExample {MemberUpsert} 200.application/json.Member
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
export default async (req, res) => {
  new PermissionChecker(req).validateHas(Permissions.values.memberEdit)

  const { invalidateCache, ...data } = req.body

  const payload = await new MemberService(req).update(req.params.id, data, {
    syncToOpensearch: true,
    manualChange: true,
    invalidateCache: invalidateCache ?? false,
  })

  await req.responseHandler.success(req, res, payload)
}
