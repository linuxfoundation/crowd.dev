import Permissions from '../../security/permissions'
import IntegrationService from '../../services/integrationService'
import PermissionChecker from '../../services/user/permissionChecker'
import { assertNoAutoManagedIntegration } from './autoManagedGuard'

export default async (req, res) => {
  new PermissionChecker(req).validateHas(Permissions.values.integrationDestroy)

  await assertNoAutoManagedIntegration(req, req.query.ids)

  await new IntegrationService(req).destroyAll(req.query.ids)

  const payload = true

  await req.responseHandler.success(req, res, payload)
}
