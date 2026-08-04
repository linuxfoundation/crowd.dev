import { ApplicationFailure, log, proxyActivities, rootCause } from '@temporalio/workflow'

import type { ITriggerBlastRadiusAnalysis } from '@crowd/types'

import type * as activities from './activities'
import { buildEcosystemNotSupportedFailure } from './ecosystemSupport'

const SUPPORTED_ECOSYSTEMS = ['npm', 'go']

const { blastRadiusStart, blastRadiusFail } = proxyActivities<typeof activities>({
  startToCloseTimeout: '2 minutes',
  retry: { maximumAttempts: 3 },
})

// Intel runs an Opus agent over the downloaded package source (up to 15 turns,
// 10-minute agent timeout in runAnalysisAgent) — give it headroom past that.
const { blastRadiusIntel } = proxyActivities<typeof activities>({
  startToCloseTimeout: '20 minutes',
  heartbeatTimeout: '5 minutes',
  retry: { maximumAttempts: 2 },
})

// Load-tested at SCAN_CONCURRENCY=8: avg 27:18 at 8 concurrent jobs, up to 33:31 at
// 20 — 15 minutes routinely timed out mid-scan, and the scan has no cancellation
// awareness, so the timed-out attempt kept running as a zombie while the retry
// started a duplicate scan of the same analysis. Raised well past the observed
// worst case; the scan itself is now cancellation-aware too (see dependentsScan.ts).
const { blastRadiusDependents } = proxyActivities<typeof activities>({
  startToCloseTimeout: '45 minutes',
  heartbeatTimeout: '3 minutes',
  retry: { maximumAttempts: 2 },
})

// Reachability downloads and analyzes up to 25 dependents (4 at a time, each with
// up to 3 agent attempts) — the slowest stage, so it gets the largest ceiling.
const { blastRadiusReachability } = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 hour',
  heartbeatTimeout: '5 minutes',
  retry: { maximumAttempts: 2 },
})

// No heartbeatTimeout — runReportStage doesn't heartbeat until after it completes
// (see activities.ts), so any run taking over a minute would otherwise always
// heartbeat-timeout and retry before its first heartbeat.
const { blastRadiusReport } = proxyActivities<typeof activities>({
  startToCloseTimeout: '2 minutes',
  retry: { maximumAttempts: 3 },
})

// 2a's on-demand trigger (see submitBlastRadiusJob in the backend akrites-external
// API). Each stage is independently resumable (guarded on its own stage_run status —
// see runIntelStage etc.), so a retried workflow (new analysisId reusing the same
// row via force, or a workflow-level retry) skips whatever already succeeded.
export async function analyzeBlastRadius(input: ITriggerBlastRadiusAnalysis): Promise<void> {
  log.info('analyzeBlastRadius received', { ...input })

  if (!SUPPORTED_ECOSYSTEMS.includes(input.ecosystem)) {
    throw buildEcosystemNotSupportedFailure(input.ecosystem)
  }

  try {
    await blastRadiusStart({
      analysisId: input.analysisId,
      advisoryOsvId: input.advisoryId,
      packageName: input.package,
      ecosystem: input.ecosystem,
      force: input.force,
    })

    await blastRadiusIntel({ analysisId: input.analysisId, advisoryOsvId: input.advisoryId })
    if (input.stopAfterStage === 'intel') {
      log.info('analyzeBlastRadius stopped after intel (stopAfterStage)', {
        analysisId: input.analysisId,
      })
      return
    }

    await blastRadiusDependents({ analysisId: input.analysisId, advisoryOsvId: input.advisoryId })
    if (input.stopAfterStage === 'dependents') {
      log.info('analyzeBlastRadius stopped after dependents (stopAfterStage)', {
        analysisId: input.analysisId,
      })
      return
    }

    await blastRadiusReachability({
      analysisId: input.analysisId,
      advisoryOsvId: input.advisoryId,
    })
    if (input.stopAfterStage === 'reachability') {
      log.info('analyzeBlastRadius stopped after reachability (stopAfterStage)', {
        analysisId: input.analysisId,
      })
      return
    }

    await blastRadiusReport({ analysisId: input.analysisId, advisoryOsvId: input.advisoryId })
  } catch (err) {
    // rootCause unwraps Temporal's ActivityFailure wrapper (whose own .message is a
    // generic "Activity task failed") down to the underlying stage error, so poll's
    // errorMessage reflects what actually broke rather than Temporal's wrapper text.
    const errorMessage = rootCause(err) ?? (err instanceof Error ? err.message : String(err))
    await blastRadiusFail({
      analysisId: input.analysisId,
      advisoryOsvId: input.advisoryId,
      packageName: input.package,
      ecosystem: input.ecosystem,
      force: input.force,
      error: errorMessage,
    })
    throw ApplicationFailure.nonRetryable(errorMessage, 'BLAST_RADIUS_STAGE_FAILED')
  }

  log.info('analyzeBlastRadius complete', { analysisId: input.analysisId })
}
