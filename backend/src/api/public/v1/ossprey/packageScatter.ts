import type { Request, Response } from 'express'
import { z } from 'zod'

import { listPackagesForScatter } from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import { STEWARDSHIP_STATUS_VALUES } from '../packages/types'

const statusEnum = z.enum(STEWARDSHIP_STATUS_VALUES)

function normalizeToArray(v: unknown): unknown[] | undefined {
  if (v === undefined) return undefined
  if (Array.isArray(v)) return v
  if (typeof v === 'string' && v.includes(','))
    return v
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean)
  return [v]
}

const scatterQuerySchema = z.object({
  status: z.preprocess(normalizeToArray, z.array(statusEnum).min(1)).optional(),
  ecosystem: z.string().optional(),
})

export async function packageScatterHandler(req: Request, res: Response): Promise<void> {
  const { status, ecosystem } = validateOrThrow(scatterQuerySchema, req.query)
  const qx = await getPackagesQx()
  const points = await listPackagesForScatter(qx, { status, ecosystem })
  ok(res, { points, total: points.length })
}
