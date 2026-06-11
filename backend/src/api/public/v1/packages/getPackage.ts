import type { Request, Response } from 'express'
import { z } from 'zod'

import { BadRequestError, NotFoundError } from '@crowd/common'

import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import { MOCK_DETAILS } from './mockData'

const querySchema = z.object({
  purl: z.string().trim().min(1),
})

// TODO: replace with real DB queries once packages DB is wired into the backend
export async function getPackage(req: Request, res: Response): Promise<void> {
  const { purl } = validateOrThrow(querySchema, req.query)

  if (!purl.startsWith('pkg:')) {
    throw new BadRequestError('Invalid purl format: must start with pkg:')
  }

  const detail = MOCK_DETAILS[purl]
  if (!detail) {
    throw new NotFoundError()
  }

  ok(res, detail)
}
