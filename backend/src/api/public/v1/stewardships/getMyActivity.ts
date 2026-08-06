import type { Request, Response } from 'express'
import { z } from 'zod'

import { listMyActivity } from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

const VALID_STATUSES = [
  'assessing',
  'active',
  'needs_attention',
  'escalated',
  'blocked',
  'unassigned',
  'open',
  'inactive',
] as const

const querySchema = z.object({
  page: z.coerce.number().int().min(1).default(1),
  pageSize: z.coerce.number().int().min(1).max(100).default(3),
  status: z
    .string()
    .optional()
    .transform((v) => {
      if (!v) return undefined
      const parts = v
        .split(',')
        .map((s) => s.trim())
        .filter(Boolean)
      return parts.length > 0 ? parts : undefined
    })
    .pipe(z.array(z.enum(VALID_STATUSES)).optional()),
})

const SUGGESTED_ACTIONS: Record<string, string> = {
  needs_attention: 'Review & respond',
  blocked: 'Resolve blocker',
  escalated: 'Add escalation context',
  assessing: 'Continue assessment',
  active: 'View stewardship',
}

export async function getMyActivityHandler(req: Request, res: Response): Promise<void> {
  const params = validateOrThrow(querySchema, req.query)

  const qx = await getPackagesQx()
  const { rows, total } = await listMyActivity(qx, {
    userId: req.actor.id,
    page: params.page,
    pageSize: params.pageSize,
    status: params.status,
  })

  ok(res, {
    data: rows.map((r) => ({
      stewardshipId: r.stewardshipId,
      packageName: r.packageName,
      purl: r.packagePurl,
      packageEcosystem: r.packageEcosystem,
      stewardshipStatus: r.stewardshipStatus,
      activityType: r.activityType,
      description: r.content,
      actor: r.actor,
      createdAt: r.createdAt,
      suggestedAction: SUGGESTED_ACTIONS[r.currentStewardshipStatus] ?? null,
    })),
    meta: {
      total,
      page: params.page,
      pageSize: params.pageSize,
    },
  })
}
