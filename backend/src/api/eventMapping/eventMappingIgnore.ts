import { validateOrThrow } from '@/utils/validation'
import { ignoreSessionizeEvent } from '@crowd/data-access-layer/src/sessionize'

import Permissions from '../../security/permissions'
import PermissionChecker from '../../services/user/permissionChecker'
import { getEventMappingQx } from './eventMappingQx'
import { eventReferenceSchema } from './eventSchema'

export default async (req, res) => {
  new PermissionChecker(req).validateHas(Permissions.values.eventMappingEdit)

  const { eventKey } = validateOrThrow(eventReferenceSchema, req.body)

  await ignoreSessionizeEvent(await getEventMappingQx(), {
    eventKey,
    reviewedBy: req.currentUser.id,
  })

  await req.responseHandler.success(req, res, { eventKey })
}
