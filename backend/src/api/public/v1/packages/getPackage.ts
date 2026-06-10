import type { Request, Response } from 'express'
import { z } from 'zod'

import { NotFoundError } from '@crowd/common'

import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import { MOCK_DETAILS } from './mockData'

const paramsSchema = z.object({
  purl: z.string().trim().min(1),
})

// TODO: replace with real DB queries once packages DB is wired into the backend
export async function getPackage(req: Request, res: Response): Promise<void> {
  // Express already decodes route params once; do not call decodeURIComponent again
  // as it would mutate canonical purls (e.g. %40scope → @scope).
  const { purl } = validateOrThrow(paramsSchema, req.params)

  if (!purl.startsWith('pkg:')) {
    throw new NotFoundError()
  }

  const detail = MOCK_DETAILS[purl]
  if (!detail) {
    throw new NotFoundError()
  }

  ok(res, detail)
}
