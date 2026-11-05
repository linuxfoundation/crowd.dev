import Permissions from '../../security/permissions'
import IntegrationService from '../../services/integrationService'
import PermissionChecker from '../../services/user/permissionChecker'
import { assertNoAutoManagedIntegration, assertNoAutoManagedPlatform } from './autoManagedGuard'

export default async (req, res) => {
  new PermissionChecker(req).validateHas(Permissions.values.integrationEdit)

  assertNoAutoManagedPlatform(req.language, [req.body?.platform])
  await assertNoAutoManagedIntegration(req, req.params.id)

  const payload = await new IntegrationService(req).update(req.params.id, req.body)

  await req.responseHandler.success(req, res, payload)
}
