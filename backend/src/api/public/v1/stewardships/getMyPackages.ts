import type { Request, Response } from 'express'
import { z } from 'zod'

import {
  computeHealthBand,
  listMyPackages,
  translateActivityContent,
} from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

const querySchema = z.object({
  page: z.coerce.number().int().min(1).default(1),
  pageSize: z.coerce.number().int().min(1).max(100).default(25),
  status: z.enum(['assessing', 'active', 'needs_attention', 'escalated', 'blocked']).optional(),
  search: z.string().trim().optional(),
  ecosystem: z.string().trim().optional(),
  healthBand: z.enum(['healthy', 'fair', 'concerning', 'critical']).optional(),
  vulnSeverity: z.enum(['high', 'critical']).optional(),
  sortBy: z.enum(['risk', 'health', 'vulns', 'name', 'last_activity']).default('risk'),
  sortDir: z.enum(['asc', 'desc']).default('desc'),
})

export async function getMyPackagesHandler(req: Request, res: Response): Promise<void> {
  const params = validateOrThrow(querySchema, req.query)

  const qx = await getPackagesQx()
  const { rows, total, statusCounts } = await listMyPackages(qx, {
    userId: req.actor.id,
    ...params,
  })

  ok(res, {
    data: rows.map((r) => ({
      purl: r.purl,
      name: r.name,
      ecosystem: r.ecosystem,
      lifecycle: r.lifecycle,
      healthScore: r.scorecardScore != null ? Math.round(r.scorecardScore * 10) : null,
      healthBand: computeHealthBand(r.scorecardScore),
      openVulns: r.openVulns,
      vulnSeverity: r.maxVulnSeverity,
      lastActivityDescription: translateActivityContent(
        r.lastActivityContent,
        r.lastActivityType,
        r.lastActivityMetadata,
      ),
      lastActivityAt: r.lastActivityAt ? r.lastActivityAt.toISOString() : null,
      stewardshipId: r.stewardshipId,
      stewardshipStatus: r.stewardshipStatus,
      myRole: r.myRole,
    })),
    meta: {
      total,
      page: params.page,
      pageSize: params.pageSize,
      statusCounts,
    },
  })
}
