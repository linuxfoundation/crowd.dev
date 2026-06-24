import type { Request, Response } from 'express'
import { z } from 'zod'

import {
  computeHealthBand,
  getPackageStatusCounts,
  listPackagesForApi,
} from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import { purlFilterSchema } from './purl'
import { STEWARDSHIP_STATUS_VALUES, type StewardshipStatus } from './types'

const DEFAULT_PAGE_SIZE = 20
const MAX_PAGE_SIZE = 100

const booleanQueryParam = z.preprocess((v) => v === 'true', z.boolean()).default(false)

const lifecycleValues = ['active', 'stable', 'declining', 'abandoned'] as const
const healthBandValues = ['healthy', 'fair', 'concerning', 'critical'] as const
const vulnSeverityValues = ['any', 'high', 'critical', 'none'] as const

const querySchema = z.object({
  page: z.coerce.number().int().min(1).default(1),
  pageSize: z.coerce.number().int().min(1).max(MAX_PAGE_SIZE).default(DEFAULT_PAGE_SIZE),
  ecosystem: z.string().trim().optional(),
  lifecycle: z.enum(lifecycleValues).optional(),
  name: z.string().trim().optional(),
  purl: purlFilterSchema,
  status: z.enum(STEWARDSHIP_STATUS_VALUES).optional(),
  healthBand: z.enum(healthBandValues).optional(),
  vulnSeverity: z.enum(vulnSeverityValues).optional(),
  busFactor1Only: booleanQueryParam,
  staleOnly: booleanQueryParam,
  unstewardedOnly: booleanQueryParam,
  sortBy: z.enum(['name', 'health', 'impact', 'openVulns', 'risk']).default('name'),
  sortDir: z.enum(['asc', 'desc']).default('asc'),
})

export async function listPackages(req: Request, res: Response): Promise<void> {
  const {
    page,
    pageSize,
    ecosystem,
    lifecycle,
    name,
    purl,
    status,
    healthBand,
    vulnSeverity,
    busFactor1Only,
    staleOnly,
    unstewardedOnly,
    sortBy,
    sortDir,
  } = validateOrThrow(querySchema, req.query)

  const filterOpts = {
    ecosystem,
    lifecycle,
    name,
    purl,
    healthBand,
    vulnSeverity,
    staleOnly,
    unstewardedOnly,
    busFactor1Only,
  }

  const qx = await getPackagesQx()
  const [{ rows, total }, statusCounts] = await Promise.all([
    listPackagesForApi(qx, {
      page,
      pageSize,
      status,
      sortBy,
      sortDir,
      ...filterOpts,
      includeStewards: true,
    }),
    getPackageStatusCounts(qx, filterOpts),
  ])

  const packages = rows.map((r) => ({
    purl: r.purl,
    name: r.name,
    ecosystem: r.ecosystem,
    health: {
      score: r.scorecardScore != null ? Math.round(Number(r.scorecardScore) * 10) : null,
      label: r.healthLabel ?? computeHealthBand(r.scorecardScore),
    },
    impact: r.criticalityScore != null ? Math.round(Number(r.criticalityScore) * 100) : null,
    lifecycle: r.lifecycleLabel,
    maintainerBusFactor: r.maintainerCount,
    openVulns: r.openVulns,
    stewardshipId: r.stewardshipId ?? null,
    stewardship: (r.stewardshipStatus ?? 'unassigned') as StewardshipStatus,
    stewards: r.stewards ?? [],
  }))

  ok(res, {
    page,
    pageSize,
    total,
    statusCounts,
    filters: {
      ecosystem: ecosystem ?? null,
      lifecycle: lifecycle ?? null,
      name: name ?? null,
      purl: purl ?? null,
      status: status ?? null,
      healthBand: healthBand ?? null,
      vulnSeverity: vulnSeverity ?? null,
      busFactor1Only,
      staleOnly,
      unstewardedOnly,
    },
    sort: { by: sortBy, dir: sortDir },
    packages,
  })
}
