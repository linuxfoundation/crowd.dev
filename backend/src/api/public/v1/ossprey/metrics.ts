import type { Request, Response } from 'express'

import { getOsspreyMetrics } from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'

export async function metricsHandler(req: Request, res: Response): Promise<void> {
  const qx = await getPackagesQx()
  const metrics = await getOsspreyMetrics(qx)
  ok(res, metrics)
}
