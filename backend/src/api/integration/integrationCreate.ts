import Permissions from '../../security/permissions'
import IntegrationService from '../../services/integrationService'
import PermissionChecker from '../../services/user/permissionChecker'
import { assertNoAutoManagedPlatform } from './autoManagedGuard'

export default async (req, res) => {
  new PermissionChecker(req).validateHas(Permissions.values.integrationCreate)

  assertNoAutoManagedPlatform(req.language, [req.body?.platform])

  const payload = await new IntegrationService(req).create(req.body)

  await req.responseHandler.success(req, res, payload)
}
