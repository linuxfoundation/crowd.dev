import type { Request, Response } from 'express'

import { ok } from '@/utils/api'

// TODO: replace with real DB queries once packages DB is wired into the backend
export async function getPackagesMetrics(req: Request, res: Response): Promise<void> {
  ok(res, {
    totalPackages: 0,
    criticalPackages: 0,
  })
}
