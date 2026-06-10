import type { Request, Response } from 'express'
import { z } from 'zod'

import { listPackagesForApi } from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import type { StewardshipStatus } from './types'

const DEFAULT_PAGE_SIZE = 20
const MAX_PAGE_SIZE = 100

const booleanQueryParam = z.preprocess((v) => v === 'true', z.boolean()).default(false)

const lifecycleValues = ['active', 'stable', 'declining', 'abandoned'] as const

const querySchema = z.object({
  page: z.coerce.number().int().min(1).default(1),
  pageSize: z.coerce.number().int().min(1).max(MAX_PAGE_SIZE).default(DEFAULT_PAGE_SIZE),
  ecosystem: z.string().trim().optional(),
  lifecycle: z.enum(lifecycleValues).optional(),
  busFactor1Only: booleanQueryParam,
  staleOnly: booleanQueryParam,
  unstewardedOnly: booleanQueryParam,
  sortBy: z.enum(['name', 'health', 'impact', 'openVulns']).default('name'),
  sortDir: z.enum(['asc', 'desc']).default('asc'),
})

export async function listPackages(req: Request, res: Response): Promise<void> {
  const {
    page,
    pageSize,
    ecosystem,
    lifecycle,
    busFactor1Only,
    staleOnly,
    unstewardedOnly,
    sortBy,
    sortDir,
  } = validateOrThrow(querySchema, req.query)

  const qx = await getPackagesQx()
  const { rows, total } = await listPackagesForApi(qx, {
    page,
    pageSize,
    ecosystem,
    staleOnly,
    unstewardedOnly,
    sortBy,
    sortDir,
  })

  const packages = rows.map((r) => ({
    purl: r.purl,
    name: r.name,
    ecosystem: r.ecosystem,
    health: null,
    impact: r.criticalityScore != null ? Math.round(Number(r.criticalityScore)) : null,
    lifecycle: null,
    maintainerBusFactor: null,
    openVulns: null,
    stewardship: (r.stewardshipStatus ?? 'unassigned') as StewardshipStatus,
    stewards: null,
  }))

  ok(res, {
    page,
    pageSize,
    total,
    filters: {
      ecosystem: ecosystem ?? null,
      lifecycle: lifecycle ?? null,
      busFactor1Only,
      staleOnly,
      unstewardedOnly,
    },
    sort: { by: sortBy, dir: sortDir },
    packages,
  })
}
