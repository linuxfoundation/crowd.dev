import { z } from 'zod'

import Permissions from '../../../security/permissions'
import IntegrationService from '../../../services/integrationService'
import PermissionChecker from '../../../services/user/permissionChecker'
import { validateOrThrow } from '../../../utils/validation'

const bodySchema = z.object({
  lists: z
    .array(
      z.object({
        name: z.string().trim().min(1),
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
