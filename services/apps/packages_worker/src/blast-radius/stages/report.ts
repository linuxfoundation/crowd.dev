import * as blastRadiusDal from '@crowd/data-access-layer/src/packages/blastRadius'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

// Stage 4: aggregate verdicts and finalize analysis.
// This stage does not render markdown (that's for a future read endpoint).
// It just aggregates metrics and finalizes the analysis row.

export async function runReportStage(qx: QueryExecutor, analysisId: string): Promise<void> {
  const startTime = Date.now()

  try {
    // Check if already done — avoid clobbering a succeeded stage_run's status/started_at
    // on a redundant re-invocation (startStageRun's ON CONFLICT always overwrites status).
    const existingStatus = await blastRadiusDal.getStageRunStatus(qx, analysisId, 'report')
    if (existingStatus === 'succeeded') {
      return
    }

    // Start stage run record
    await blastRadiusDal.startStageRun(qx, {
      analysisId,
      stage: 'report',
      status: 'running',
      model: null,
    })

    // Aggregate cost. Every stage (intel, dependents, reachability, report) records
    // its own cost exactly once via completeStageRun — reachability's stage_run cost
    // is already the sum of its per-dependent verdict costs, so summing verdicts here
    // too would double-count it.
    const finalCostUsd = await blastRadiusDal.getStageRunsCost(qx, analysisId)

    // Finalize analysis
    await blastRadiusDal.finalizeAnalysis(qx, analysisId, finalCostUsd)

    const duration = Date.now() - startTime
    await blastRadiusDal.completeStageRun(qx, analysisId, 'report', duration, 0)
  } catch (err) {
    const duration = Date.now() - startTime
    const errorMsg = err instanceof Error ? err.message : String(err)
    await blastRadiusDal.failStageRun(qx, analysisId, 'report', duration, errorMsg)
    throw err
  }
}
