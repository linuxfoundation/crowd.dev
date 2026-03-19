import type { Request, Response } from 'express'
import { z } from 'zod'

import { optionsQx } from '@crowd/data-access-layer'

import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import { findAffiliationsByGithubHandles } from './queries'

const MAX_HANDLES = 1000

const bodySchema = z.object({
  githubHandles: z
    .array(z.string().min(1))
    .min(1)
    .max(MAX_HANDLES, `Maximum ${MAX_HANDLES} handles per request`),
})

export async function getAffiliations(req: Request, res: Response): Promise<void> {
  const { githubHandles } = validateOrThrow(bodySchema, req.body)
  const qx = optionsQx(req)

  const { contributors, notFound } = await findAffiliationsByGithubHandles(qx, githubHandles)

  ok(res, {
    total_found: contributors.length,
    contributors,
    notFound,
  })
}
