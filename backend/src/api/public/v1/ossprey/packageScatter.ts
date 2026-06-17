import type { Request, Response } from 'express'

import { listPackagesForScatter } from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'

export async function packageScatterHandler(req: Request, res: Response): Promise<void> {
  const qx = await getPackagesQx()
  const points = await listPackagesForScatter(qx)
  ok(res, { points, total: points.length })
}
