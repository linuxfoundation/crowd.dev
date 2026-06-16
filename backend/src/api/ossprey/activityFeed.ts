import { z } from 'zod'

import { listStewardshipActivity } from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'
import { validateOrThrow } from '@/utils/validation'

const querySchema = z.object({
  page: z.coerce.number().int().min(1).default(1),
  pageSize: z.coerce.number().int().min(1).max(100).default(25),
})

export default async (req, res) => {
  const { page, pageSize } = validateOrThrow(querySchema, req.query)

  const qx = await getPackagesQx()
  const { rows, total } = await listStewardshipActivity(qx, { page, pageSize })

  await req.responseHandler.success(req, res, {
    rows: rows.map((r) => ({
      id: r.id,
      stewardshipId: r.stewardshipId,
      packagePurl: r.packagePurl,
      packageName: r.packageName,
      packageEcosystem: r.packageEcosystem,
      actorUserId: r.actorUserId,
      actorName: null, // TODO: resolve display name from crowd.dev users/members table by actorUserId
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
