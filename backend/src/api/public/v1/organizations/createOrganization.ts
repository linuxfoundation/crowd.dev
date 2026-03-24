import type { Request, Response } from 'express'
import { z } from 'zod'

import { captureApiChange, organizationCreateAction } from '@crowd/audit-logs'
import { InternalError } from '@crowd/common'
import { findOrCreateOrganization, optionsQx } from '@crowd/data-access-layer'
import { OrganizationAttributeSource, OrganizationIdentityType } from '@crowd/types'

import { created } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

const bodySchema = z.object({
  name: z.string().trim().min(1),
  domain: z.string().trim().min(1),
  source: z.string().trim().min(1),
})

export async function createOrganization(req: Request, res: Response): Promise<void> {
  const { name, domain, source } = validateOrThrow(bodySchema, req.body)

  const qx = optionsQx(req)

  let organizationId: string | undefined

  await qx.tx(async (tx) => {
    const orgSource = OrganizationAttributeSource.CUSTOM

    organizationId = await findOrCreateOrganization(tx, orgSource, {
      displayName: name,
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

    if (!organizationId) {
      throw new InternalError('Failed to create organization')
    }

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
  })

  created(res, { id: organizationId, name })
}
