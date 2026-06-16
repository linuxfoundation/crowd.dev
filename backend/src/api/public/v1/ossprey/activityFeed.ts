import type { Request, Response } from 'express'
import { z } from 'zod'

import { listStewardshipActivity } from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

const querySchema = z.object({
  page: z.coerce.number().int().min(1).default(1),
  pageSize: z.coerce.number().int().min(1).max(100).default(25),
})

export async function activityFeedHandler(req: Request, res: Response): Promise<void> {
  const { page, pageSize } = validateOrThrow(querySchema, req.query)

  const qx = await getPackagesQx()
  const { rows, total } = await listStewardshipActivity(qx, { page, pageSize })

  ok(res, {
    rows: rows.map((r) => ({
      id: r.id,
      stewardshipId: r.stewardshipId,
      packagePurl: r.packagePurl,
      packageName: r.packageName,
      packageEcosystem: r.packageEcosystem,
      actorUserId: r.actorUserId,
      actorName: r.actorUserId, // TODO: resolve display name from crowd.dev users/members table by actorUserId
      actorType: r.actorType,
      activityType: r.activityType,
      content: r.content,
      metadata: r.metadata,
      stewardshipStatus: r.stewardshipStatus,
      createdAt: r.createdAt,
    })),
    total,
    page,
    pageSize,
  })
}
