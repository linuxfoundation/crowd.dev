import type { Request, Response } from 'express'

import * as blastRadiusDal from '@crowd/data-access-layer/src/packages/blastRadius'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import { toBlastRadiusAnalysis } from './blastRadiusAnalysis'
import {
  type BlastRadiusAnalysisBulkEntry,
  blastRadiusJobPollBatchRequestSchema,
  paginateAnalysisIds,
} from './blastRadiusBatch'

// 2b bulk — poll multiple blast-radius analyses in one request. Same
// found/not-found echo shape as the other batch endpoints (packages, advisories,
// contacts): an unknown analysisId comes back { found: false, analysis: null }
// instead of 404ing the whole request. Read-only, so it stays behind the
// regular rateLimiter, not the strict blastRadiusRateLimiter.
export async function getBlastRadiusJobBatch(req: Request, res: Response): Promise<void> {
  const { page, pageSize, total, pagedAnalysisIds } = paginateAnalysisIds(
    validateOrThrow(blastRadiusJobPollBatchRequestSchema, req.body),
  )

  const qx = await getPackagesQx()

  const results: BlastRadiusAnalysisBulkEntry[] = await Promise.all(
    pagedAnalysisIds.map((requestedAnalysisId) => pollOneAnalysis(qx, requestedAnalysisId)),
  )

  ok(res, { page, pageSize, total, results })
}

async function pollOneAnalysis(
  qx: Awaited<ReturnType<typeof getPackagesQx>>,
  requestedAnalysisId: string,
): Promise<BlastRadiusAnalysisBulkEntry> {
  const analysis = await blastRadiusDal.getAnalysisDetail(qx, requestedAnalysisId)
  if (!analysis) {
    return { requestedAnalysisId, found: false, analysis: null }
  }

  const done = analysis.status === 'done'
  const [verdictRows, excludedByRangeCount] = done
    ? await Promise.all([
        blastRadiusDal.getVerdictResults(qx, requestedAnalysisId),
        blastRadiusDal.getDependentsExcludedByRangeCount(qx, requestedAnalysisId),
      ])
    : [[], 0]

  return {
    requestedAnalysisId,
    found: true,
    analysis: toBlastRadiusAnalysis(analysis, verdictRows, excludedByRangeCount),
  }
}
