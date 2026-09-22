import Permissions from '@/security/permissions'
import PermissionChecker from '@/services/user/permissionChecker'
import { GithubIntegrationService } from '@crowd/common_services'

export default async (req, res) => {
  new PermissionChecker(req).validateHas(Permissions.values.integrationEdit)

  const payload = await new GithubIntegrationService(req.log).findGithubRepos(
    req.query.query,
    req.query.limit,
    req.query.offset,
  )
  await req.responseHandler.success(req, res, payload)
}
