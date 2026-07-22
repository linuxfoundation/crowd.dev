import { z } from 'zod'

import Permissions from '../../../security/permissions'
import IntegrationService from '../../../services/integrationService'
import PermissionChecker from '../../../services/user/permissionChecker'
import { validateOrThrow } from '../../../utils/validation'

const MAX_LIST_NAME_LENGTH = 255

// `name` becomes a single filesystem path component for the mailing list
// mirror (see mirror_service.py's list_mirror_dir), so it must not contain a
// path separator or resolve to "." / ".." when used alone.
const isSafeListName = (name: string): boolean =>
  name.length > 0 &&
  name.length <= MAX_LIST_NAME_LENGTH &&
  !name.includes('/') &&
  !name.includes('\0') &&
  name !== '.' &&
  name !== '..'

const bodySchema = z.object({
  lists: z
    .array(
      z.object({
        name: z.string().trim().min(1).refine(isSafeListName, {
          message: 'Invalid mailing list name',
        }),
        sourceUrl: z.string().trim().min(1),
      }),
    )
    .default([]),
})

export default async (req, res) => {
  new PermissionChecker(req).validateHas(Permissions.values.tenantEdit)
  const integrationData = validateOrThrow(bodySchema, req.body)

  const payload = await new IntegrationService(req).mailingListConnectOrUpdate(integrationData)
  await req.responseHandler.success(req, res, payload)
}
