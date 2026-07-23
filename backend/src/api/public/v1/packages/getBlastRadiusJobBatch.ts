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

  const analysisRows = await blastRadiusDal.getAnalysisDetailsByIds(qx, pagedAnalysisIds)
  const analysisById = new Map(analysisRows.map((row) => [row.id, row]))

  const doneIds = analysisRows.filter((row) => row.status === 'done').map((row) => row.id)
  const [verdictRows, excludedByRangeCounts] = await Promise.all([
    blastRadiusDal.getVerdictResultsBatch(qx, doneIds),
    blastRadiusDal.getDependentsExcludedByRangeCountBatch(qx, doneIds),
  ])

  const verdictsByAnalysisId = new Map<string, typeof verdictRows>()
  for (const row of verdictRows) {
    const bucket = verdictsByAnalysisId.get(row.analysisId)
    if (bucket) {
      bucket.push(row)
    } else {
      verdictsByAnalysisId.set(row.analysisId, [row])
    }
  }
  const excludedByRangeCountByAnalysisId = new Map(
    excludedByRangeCounts.map(({ analysisId, count }) => [analysisId, count]),
  )

  const results: BlastRadiusAnalysisBulkEntry[] = pagedAnalysisIds.map((requestedAnalysisId) => {
    const analysis = analysisById.get(requestedAnalysisId)
    if (!analysis) {
      return { requestedAnalysisId, found: false, analysis: null }
    }

    return {
      requestedAnalysisId,
      found: true,
      analysis: toBlastRadiusAnalysis(
        analysis,
        verdictsByAnalysisId.get(requestedAnalysisId) ?? [],
        excludedByRangeCountByAnalysisId.get(requestedAnalysisId) ?? 0,
      ),
    }
  })

  ok(res, { page, pageSize, total, results })
}
