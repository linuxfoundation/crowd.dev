import type { Request, Response } from 'express'
import { z } from 'zod'

import { NotFoundError, normalizeHostname } from '@crowd/common'
import { findOrganizationByNameOrDomain, optionsQx } from '@crowd/data-access-layer'

import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

const querySchema = z
  .object({
    name: z.string().trim().min(1).optional(),
    domain: z.string().trim().min(1).optional(),
  })
  .refine((data) => data.name || data.domain, {
    message: 'Either name or domain must be provided',
  })

export async function getOrganization(req: Request, res: Response): Promise<void> {
  const { name, domain } = validateOrThrow(querySchema, req.query)
  const qx = optionsQx(req)

  const organization = await findOrganizationByNameOrDomain(qx, {
    name,
    domain: domain ? normalizeHostname(domain, false) : undefined,
  })

  if (!organization) {
    throw new NotFoundError('Organization not found')
  }

  const { logo, ...rest } = organization
  ok(res, { ...rest, ...(logo ? { logo } : {}) })
}
