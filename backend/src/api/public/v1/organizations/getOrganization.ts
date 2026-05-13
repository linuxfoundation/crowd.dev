import type { Request, Response } from 'express'
import { z } from 'zod'

import { NotFoundError, normalizeHostname } from '@crowd/common'
import {
  OrgIdentityField,
  OrganizationField,
  findOrgAttributes,
  findOrgById,
  optionsQx,
  queryOrgIdentities,
} from '@crowd/data-access-layer'
import { OrganizationIdentityType } from '@crowd/types'

import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

const querySchema = z.object({
  domain: z.string().trim().min(1),
})

export async function getOrganization(req: Request, res: Response): Promise<void> {
  const { domain } = validateOrThrow(querySchema, req.query)

  const qx = optionsQx(req)

  const results = await queryOrgIdentities(qx, {
    fields: [OrgIdentityField.ORGANIZATION_ID],
    filter: {
      and: [
        { value: { eq: normalizeHostname(domain, false) } },
        { type: { eq: OrganizationIdentityType.PRIMARY_DOMAIN } },
        { verified: { eq: true } },
      ],
    },
  })

  const organizationId = results[0]?.organizationId

  if (!organizationId) {
    throw new NotFoundError('Organization not found')
  }

  const org = await findOrgById(qx, organizationId, [
    OrganizationField.ID,
    OrganizationField.DISPLAY_NAME,
  ])

  const attributes = await findOrgAttributes(qx, organizationId)
  const logo = attributes.find((a) => a.name === 'logo')?.value

  ok(res, {
    id: org.id,
    name: org.displayName,
    ...(logo ? { logo } : {}),
  })
}
