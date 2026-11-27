import SequelizeRepository from '@/database/repositories/sequelizeRepository'
import { listActiveSubprojectsForMapping } from '@crowd/data-access-layer/src/segments'
import { listSessionizeEventsForReview } from '@crowd/data-access-layer/src/sessionize'

import Permissions from '../../security/permissions'
import PermissionChecker from '../../services/user/permissionChecker'
import { buildEventMappingRows } from './reviewRows'

export default async (req, res) => {
  new PermissionChecker(req).validateHas(Permissions.values.eventMappingRead)

  const qx = SequelizeRepository.getQueryExecutor(req)
  const [events, subprojects] = await Promise.all([
    listSessionizeEventsForReview(qx),
    listActiveSubprojectsForMapping(qx),
  ])

  await req.responseHandler.success(req, res, buildEventMappingRows(events, subprojects))
}
