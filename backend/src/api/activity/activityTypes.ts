import Permissions from '../../security/permissions'
import ActivityService from '../../services/activityService'
import PermissionChecker from '../../services/user/permissionChecker'

export default async (req, res) => {
  new PermissionChecker(req).validateHas(Permissions.values.activityRead)

  const payload = await new ActivityService(req).findActivityTypes()

  await req.responseHandler.success(req, res, payload)
}
