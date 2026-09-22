import type { Request, Response } from 'express'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'
import { getPackageMetrics } from '@crowd/data-access-layer'

export async function getPackagesMetrics(req: Request, res: Response): Promise<void> {
  const qx = await getPackagesQx()
  const metrics = await getPackageMetrics(qx)
  ok(res, metrics)
}
