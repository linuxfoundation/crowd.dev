import {
  CancellationScope,
  isCancellation,
  log,
  proxyActivities,
  rootCause,
  workflowInfo,
} from '@temporalio/workflow'

import * as activities from '../activities'
import { IProcessProjectDocsReadinessArgs } from '../types'

const { startRun, finishRun, recordFailure } = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
  retry: { maximumAttempts: 3 },
})

const { resolveDocsUrl } = proxyActivities<typeof activities>({
  startToCloseTimeout: '5 minutes',
  retry: { maximumAttempts: 2 },
})

const { scoreProject } = proxyActivities<typeof activities>({
  startToCloseTimeout: '30 minutes',
  retry: { maximumAttempts: 2 },
})

export async function processProjectDocsReadiness(
  args: IProcessProjectDocsReadinessArgs,
): Promise<void> {
  const { projectId } = args
  const ownsRun = !args.runId
  const info = workflowInfo()

  const runId =
    args.runId ??
    (await CancellationScope.nonCancellable(() =>
      startRun({
        trigger: 'on-demand',
        scope: 'lf',
        workflowId: info.workflowId,
        temporalRunId: info.runId,
      }),
    ))

  let discovered = 0
  let scored = 0
  let failed = 0
  let runStatus: 'completed' | 'failed' = 'failed'
  let errorMessage: string | undefined

  try {
    const resolved = await resolveDocsUrl(projectId)

    if (!resolved.docsUrl) {
      failed = 1
      await recordFailure(projectId, runId, resolved, 'no-docs-url')
      runStatus = 'completed'
      return
    }

    discovered = 1

    try {
      await scoreProject(projectId, runId, resolved)
      scored = 1
    } catch (err) {
      if (isCancellation(err)) {
        throw err
      }

      failed = 1
      const reason = rootCause(err) ?? String(err)
      log.warn('docs readiness scoring failed, recording failure', {
        projectId,
        runId,
        error: reason,
      })
      await recordFailure(projectId, runId, resolved, reason)
    }

    runStatus = 'completed'
  } catch (err) {
    errorMessage = rootCause(err) ?? String(err)
    throw err
  } finally {
    if (ownsRun) {
      await CancellationScope.nonCancellable(() =>
        finishRun(runId, {
          status: runStatus,
          totalProjects: 1,
          discovered,
          scored,
          failed,
          errorMessage,
        }),
      )
    }
  }
}
