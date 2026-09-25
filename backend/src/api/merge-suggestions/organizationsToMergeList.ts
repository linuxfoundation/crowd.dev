import Permissions from '../../security/permissions'
import OrganizationService from '../../services/organizationService'
import PermissionChecker from '../../services/user/permissionChecker'

export default async (req, res) => {
  new PermissionChecker(req).validateHas(Permissions.values.organizationRead)

  const payload = await new OrganizationService(req).findOrganizationsWithMergeSuggestions({
    ...req.body,
    segmentId: req.body.segments?.[0],
  })

  await req.responseHandler.success(req, res, payload)
}
