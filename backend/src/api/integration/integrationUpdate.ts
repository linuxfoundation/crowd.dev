import Permissions from '../../security/permissions'
import IntegrationService from '../../services/integrationService'
import PermissionChecker from '../../services/user/permissionChecker'

export default async (req, res) => {
  new PermissionChecker(req).validateHas(Permissions.values.integrationEdit)

  const payload = await new IntegrationService(req).update(req.params.id, req.body)

  await req.responseHandler.success(req, res, payload)
}
