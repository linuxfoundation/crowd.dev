import type { Request, Response } from 'express'
import { z } from 'zod'

import { optionsQx } from '@/database/sequelizeQueryExecutor'
import { created } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'
import { captureApiChange, organizationCreateAction } from '@crowd/audit-logs'
import {
  BadRequestError,
  InternalError,
  isPlaceholderEmployerName,
  normalizeHostname,
} from '@crowd/common'
import { markOrganizationsChanged } from '@crowd/common_services'
import { findOrCreateOrganization } from '@crowd/data-access-layer'
import { OrganizationAttributeSource, OrganizationIdentityType } from '@crowd/types'

const bodySchema = z.object({
  name: z
    .string()
    .trim()
    .min(1)
    .refine(
      (name) => !isPlaceholderEmployerName(name),
      'Name is a placeholder, not an organization',
    ),
  domain: z.string().trim().min(1),
  source: z.string().trim().min(1),
  logo: z.string().trim().min(1).optional(),
})

export async function createOrganization(req: Request, res: Response): Promise<void> {
  const { name, domain: rawDomain, source, logo } = validateOrThrow(bodySchema, req.body)

  const domain = normalizeHostname(rawDomain, false)

  if (!domain) {
    throw new BadRequestError(`Invalid domain: ${rawDomain}`)
  }

  const qx = optionsQx(req)

  const { organizationId, changed } = await qx.tx(async (tx) => {
    const orgSource = OrganizationAttributeSource.LFX_SERVE

    const result = await findOrCreateOrganization(tx, orgSource, {
      displayName: name,
      logo,
      identities: [
        {
          value: domain,
          type: OrganizationIdentityType.PRIMARY_DOMAIN,
          verified: true,
          platform: orgSource,
          source,
        },
      ],
    })

    if (!result) {
      throw new InternalError('Failed to create organization')
    }

    const organizationId = result.id

    await captureApiChange(
      req,
      organizationCreateAction(organizationId, async (captureState) => {
        captureState({
          id: organizationId,
          displayName: name,
          identities: [
            {
              value: domain,
              type: OrganizationIdentityType.PRIMARY_DOMAIN,
            },
          ],
        })
      }),
    )

    return { organizationId, changed: result.changed }
  })

  if (changed) {
    await markOrganizationsChanged(req.redis, [organizationId], req.log)
  }

  created(res, { id: organizationId, name, logo, domain })
}
