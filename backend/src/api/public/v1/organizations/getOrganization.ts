import type { Request, Response } from 'express'
import { z } from 'zod'

import { NotFoundError, normalizeHostname } from '@crowd/common'
import {
  OrgIdentityField,
  OrganizationField,
  fetchManyOrganizationVerifiedPrimaryDomains,
  findOrgAttributes,
  findOrgById,
  optionsQx,
  queryOrgIdentities,
  searchOrganizationsByName,
} from '@crowd/data-access-layer'
import { OrganizationIdentityType } from '@crowd/types'

import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

const DEFAULT_PAGE_SIZE = 20
const MAX_PAGE_SIZE = 100

const querySchema = z
  .object({
    domain: z.string().trim().min(1).optional(),
    name: z.string().trim().min(1).optional(),
    page: z.coerce.number().int().min(1).default(1),
    pageSize: z.coerce.number().int().min(1).max(MAX_PAGE_SIZE).default(DEFAULT_PAGE_SIZE),
  })
  .refine((d) => !!(d.domain ?? d.name), {
    message: 'Either domain or name is required',
  })
  .refine((d) => !(d.domain && d.name), {
    message: 'Only one of domain or name may be provided',
  })

export async function getOrganization(req: Request, res: Response): Promise<void> {
  const { domain, name, page, pageSize } = validateOrThrow(querySchema, req.query)

  const qx = optionsQx(req)

  if (domain) {
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
    return
  }

  // name search — fuzzy, paginated
  const offset = (page - 1) * pageSize
  const { rows, total } = await searchOrganizationsByName(qx, name, { limit: pageSize, offset })

  const orgIds = rows.map((r) => r.id)
  const primaryDomains = await fetchManyOrganizationVerifiedPrimaryDomains(qx, orgIds)
  const domainsMap = new Map(primaryDomains.map((d) => [d.orgId, d.domains]))

  const organizations = rows.map((r) => ({
    id: r.id,
    name: r.displayName,
    domains: domainsMap.get(r.id) ?? [],
    ...(r.logo ? { logo: r.logo } : {}),
  }))

  ok(res, { organizations, page, pageSize, total })
}
