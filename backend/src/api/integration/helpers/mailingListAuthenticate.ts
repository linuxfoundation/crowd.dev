import Permissions from '../../../security/permissions'
import IntegrationService from '../../../services/integrationService'
import PermissionChecker from '../../../services/user/permissionChecker'

export default async (req, res) => {
  new PermissionChecker(req).validateHas(Permissions.values.tenantEdit)
  const integrationData = {
    ...req.body,
    lists: req.body.lists || [],
  }

  const payload = await new IntegrationService(req).mailingListConnectOrUpdate(integrationData)
  await req.responseHandler.success(req, res, payload)
}
