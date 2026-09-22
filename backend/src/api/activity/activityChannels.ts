import Permissions from '../../security/permissions'
import ActivityService from '../../services/activityService'
import PermissionChecker from '../../services/user/permissionChecker'

/**
 * GET /activity/channel
 * @summary List activity channels
 * @tag Activities
 * @security Bearer
 * @description Find all activity channels
 * @response 200 - Ok
 * @responseContent {ActivityResponse} 200.application/json
 * @responseExample {ActivityFind} 200.application/json.Activity
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
export default async (req, res) => {
  new PermissionChecker(req).validateHas(Permissions.values.activityRead)

  const payload = await new ActivityService(req).findActivityChannels()

  await req.responseHandler.success(req, res, payload)
}
