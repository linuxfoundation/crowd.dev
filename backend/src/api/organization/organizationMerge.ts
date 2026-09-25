import OrganizationService from '@/services/organizationService'

import Permissions from '../../security/permissions'
import track from '../../segment/track'
import PermissionChecker from '../../services/user/permissionChecker'

export default async (req, res) => {
  new PermissionChecker(req).validateHas(Permissions.values.organizationEdit)

  const primaryOrgId = req.params.organizationId
  const secondaryOrgId = req.body.organizationToMerge
  const segmentId = req.body.segments?.[0]

  const requestPayload = {
    primary: primaryOrgId,
    secondary: secondaryOrgId,
    segmentId,
  }

  await new OrganizationService(req).mergeSync(primaryOrgId, secondaryOrgId, segmentId)

  track('Merge organizations', requestPayload, { ...req })

  await req.responseHandler.success(req, res, requestPayload)
}
