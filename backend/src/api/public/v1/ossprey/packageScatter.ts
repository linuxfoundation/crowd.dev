import type { Request, Response } from 'express'
import { z } from 'zod'

import { listPackagesForScatter } from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import { STEWARDSHIP_STATUS_VALUES } from '../packages/types'

const scatterQuerySchema = z.object({
  status: z.enum(STEWARDSHIP_STATUS_VALUES).optional(),
})

export async function packageScatterHandler(req: Request, res: Response): Promise<void> {
  const { status } = validateOrThrow(scatterQuerySchema, req.query)
  const qx = await getPackagesQx()
  const points = await listPackagesForScatter(qx, { status })
  ok(res, { points, total: points.length })
}
