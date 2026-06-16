import { z } from 'zod'

import { computeHealthBand, listPackagesForApi } from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'
import { validateOrThrow } from '@/utils/validation'

const MAX_PAGE_SIZE = 250

const boolParam = z.preprocess((v) => v === 'true', z.boolean()).default(false)

const querySchema = z.object({
  page: z.coerce.number().int().min(1).default(1),
  pageSize: z.coerce.number().int().min(1).max(MAX_PAGE_SIZE).default(25),
  ecosystem: z.string().trim().optional(),
  lifecycle: z.enum(['active', 'stable', 'declining', 'abandoned']).optional(),
  name: z.string().trim().optional(),
  status: z
    .enum([
      'unassigned',
      'open',
      'assessing',
      'active',
      'needs_attention',
      'escalated',
      'blocked',
      'inactive',
    ])
    .optional(),
  healthBand: z.enum(['healthy', 'fair', 'concerning', 'critical']).optional(),
  vulnSeverity: z.enum(['any', 'high', 'critical', 'none']).optional(),
  staleOnly: boolParam,
  unstewardedOnly: boolParam,
  busFactor1Only: boolParam,
  sortBy: z.enum(['name', 'risk', 'impact', 'openVulns', 'health']).default('risk'),
  sortDir: z.enum(['asc', 'desc']).default('desc'),
})

export default async (req, res) => {
  const params = validateOrThrow(querySchema, req.query)

  const qx = await getPackagesQx()
  const { rows, total } = await listPackagesForApi(qx, {
    ...params,
    includeStewards: true,
  })

  const mappedRows = rows.map((r) => ({
    purl: r.purl,
    name: r.name,
    ecosystem: r.ecosystem,
    criticalityScore: r.criticalityScore,
    stewardshipId: r.stewardshipId ?? null,
    stewardshipStatus: r.stewardshipStatus ?? null,
    openVulns: r.openVulns,
    maxVulnSeverity: r.maxVulnSeverity ?? null,
    maintainerCount: r.maintainerCount,
    scorecardScore: r.scorecardScore,
    healthBand: computeHealthBand(r.scorecardScore != null ? Number(r.scorecardScore) : null),
    latestReleaseAt: r.latestReleaseAt ? r.latestReleaseAt.toISOString() : null,
    lastActivity: r.lastActivityAt
      ? {
          type: r.lastActivityType,
          content: r.lastActivityContent,
          at: r.lastActivityAt.toISOString(),
        }
      : null,
    stewards: r.stewards ?? [],
  }))

  await req.responseHandler.success(req, res, {
    rows: mappedRows,
    total,
    page: params.page,
    pageSize: params.pageSize,
  })
}
