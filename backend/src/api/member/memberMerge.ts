import { CommonMemberService, invalidateMemberQueryCache } from '@crowd/common_services'
import { optionsQx } from '@crowd/data-access-layer'

import Permissions from '../../security/permissions'
import track from '../../segment/track'
import PermissionChecker from '../../services/user/permissionChecker'

export default async (req, res) => {
  new PermissionChecker(req).validateHas(Permissions.values.memberEdit)

  const { memberId } = req.params
  const { memberToMerge } = req.body

  const service = new CommonMemberService(optionsQx(req), req.temporal, req.log)

  const payload = await service.merge(memberId, memberToMerge, req)

  try {
    await invalidateMemberQueryCache(req.redis, [memberId, memberToMerge])
  } catch (error) {
    req.log.warn({ error }, 'Cache invalidation failed after member merge')
  }

  track('Merge members', { memberId, memberToMergeId: memberToMerge }, req)

  return req.responseHandler.success(req, res, payload, payload.status ?? 200)
}
