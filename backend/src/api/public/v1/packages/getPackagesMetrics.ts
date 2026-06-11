import type { Request, Response } from 'express'

import { ok } from '@/utils/api'

import { MOCK_METRICS } from './mockData'

// TODO: replace with real DB queries once packages DB is wired into the backend
export async function getPackagesMetrics(req: Request, res: Response): Promise<void> {
  ok(res, MOCK_METRICS)
}
