import type { Request, Response } from 'express'
import { z } from 'zod'

import { NotFoundError } from '@crowd/common'
import * as blastRadiusDal from '@crowd/data-access-layer/src/packages/blastRadius'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import { toBlastRadiusAnalysis } from './blastRadiusAnalysis'

const paramsSchema = z.object({
  analysisId: z.uuid(),
})

// 2b — poll a blast-radius analysis job. results/summary are only populated once
// status is 'done' — see toBlastRadiusAnalysis.
export async function getBlastRadiusJob(req: Request, res: Response): Promise<void> {
  const { analysisId } = validateOrThrow(paramsSchema, req.params)

  const qx = await getPackagesQx()
  const analysis = await blastRadiusDal.getAnalysisDetail(qx, analysisId)
  if (!analysis) {
    throw new NotFoundError()
  }

  const verdictRows =
    analysis.status === 'done' ? await blastRadiusDal.getVerdictResults(qx, analysisId) : []

  ok(res, toBlastRadiusAnalysis(analysis, verdictRows))
}
