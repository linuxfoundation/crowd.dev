import DataIssueService from '@/services/dataIssueService'
import { DataIssueEntity } from '@crowd/types'

import Permissions from '../../security/permissions'
import PermissionChecker from '../../services/user/permissionChecker'

export default async (req, res) => {
  new PermissionChecker(req).validateHas(Permissions.values.dataIssueCreate)

  const payload = await new DataIssueService(req).createDataIssue(
    { ...req.body, entity: DataIssueEntity.PERSON },
    req.params.id,
  )

  await req.responseHandler.success(req, res, payload)
}
