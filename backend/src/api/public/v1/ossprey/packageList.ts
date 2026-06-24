import type { Request, Response } from 'express'
import { z } from 'zod'

import {
  computeHealthBand,
  getPackageStatusCounts,
  listPackagesForApi,
  translateActivityContent,
} from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import { purlFilterSchema } from '../packages/purl'
import { LIFECYCLE_VALUES } from '../packages/types'

const MAX_PAGE_SIZE = 250
const LIFECYCLE_SET = new Set<string>(LIFECYCLE_VALUES)

const boolParam = z.preprocess((v) => v === 'true', z.boolean()).default(false)

const querySchema = z.object({
  page: z.coerce.number().int().min(1).default(1),
  pageSize: z.coerce.number().int().min(1).max(MAX_PAGE_SIZE).default(25),
  ecosystem: z.string().trim().optional(),
  lifecycle: z.enum(LIFECYCLE_VALUES).optional(),
  name: z.string().trim().optional(),
  purl: purlFilterSchema,
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
  healthBand: z.enum(['excellent', 'healthy', 'fair', 'concerning', 'critical']).optional(),
  vulnSeverity: z.enum(['any', 'high', 'critical', 'none']).optional(),
  staleOnly: boolParam,
  unstewardedOnly: boolParam,
  busFactor1Only: boolParam,
  sortBy: z.enum(['name', 'risk', 'impact', 'openVulns', 'health']).default('risk'),
  sortDir: z.enum(['asc', 'desc']).default('desc'),
})

export async function packageListHandler(req: Request, res: Response): Promise<void> {
  const params = validateOrThrow(querySchema, req.query)

  const filterOpts = {
    ecosystem: params.ecosystem,
    lifecycle: params.lifecycle,
    name: params.name,
    purl: params.purl,
    healthBand: params.healthBand,
    vulnSeverity: params.vulnSeverity,
    staleOnly: params.staleOnly,
    unstewardedOnly: params.unstewardedOnly,
    busFactor1Only: params.busFactor1Only,
  }

  const qx = await getPackagesQx()
  const [{ rows, total }, statusCounts] = await Promise.all([
    listPackagesForApi(qx, { ...params, includeStewards: true, includeLastActivity: true }),
    getPackageStatusCounts(qx, filterOpts),
  ])

  ok(res, {
    rows: rows.map((r) => ({
      purl: r.purl,
      name: r.name,
      ecosystem: r.ecosystem,
      criticalityScore: r.criticalityScore,
      impact: r.criticalityScore != null ? Math.round(r.criticalityScore * 100) : null,
      stewardshipId: r.stewardshipId ?? null,
      stewardshipStatus: r.stewardshipStatus ?? null,
      openVulns: r.openVulns,
      maxVulnSeverity: r.maxVulnSeverity ?? null,
      maintainerCount: r.maintainerCount,
      scorecardScore: r.scorecardScore,
      health: {
        score:
          r.healthScore ?? (r.scorecardScore != null ? Math.round(r.scorecardScore * 10) : null),
        label: r.healthLabel ?? computeHealthBand(r.scorecardScore),
      },
      lifecycle:
        r.lifecycleLabel != null && LIFECYCLE_SET.has(r.lifecycleLabel) ? r.lifecycleLabel : null,
      latestReleaseAt: r.latestReleaseAt ? r.latestReleaseAt.toISOString() : null,
      lastActivity: r.lastActivityAt
        ? {
            type: r.lastActivityType,
            content: translateActivityContent(
              r.lastActivityContent ?? null,
              r.lastActivityType,
              r.lastActivityMetadata,
            ),
            at: r.lastActivityAt.toISOString(),
          }
        : null,
      stewards: r.stewards ?? [],
    })),
    total,
    page: params.page,
    pageSize: params.pageSize,
    statusCounts,
  })
}
