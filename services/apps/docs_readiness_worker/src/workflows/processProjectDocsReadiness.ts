import { log, proxyActivities, workflowInfo } from '@temporalio/workflow'

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
    (await startRun({
      trigger: 'on-demand',
      scope: 'lf',
      workflowId: info.workflowId,
      temporalRunId: info.runId,
    }))

  let discovered = 0
  let scored = 0
  let failed = 0
  let runStatus: 'completed' | 'failed' = 'failed'

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
      failed = 1
      log.warn('docs readiness scoring failed, recording failure', {
        projectId,
        runId,
        error: (err as Error).message,
      })
      await recordFailure(projectId, runId, resolved, (err as Error).message)
    }

    runStatus = 'completed'
  } finally {
    if (ownsRun) {
      await finishRun(runId, { status: runStatus, totalProjects: 1, discovered, scored, failed })
    }
  }
}
