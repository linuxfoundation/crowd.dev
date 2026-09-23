import {
  CancellationScope,
  continueAsNew,
  executeChild,
  isCancellation,
  log,
  proxyActivities,
  workflowInfo,
} from '@temporalio/workflow'

import * as activities from '../activities'
import { DEFAULT_SWEEP_CONCURRENCY, IRunDocsReadinessSweepArgs } from '../types'
import { processProjectDocsReadiness } from './processProjectDocsReadiness'

const { startRun, finishRun, findProjectsForSweep } = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
  retry: { maximumAttempts: 3 },
})

const PAGE_SIZE = 200

export async function runDocsReadinessSweep(args: IRunDocsReadinessSweepArgs): Promise<void> {
  const { mode, scope } = args
  const concurrency =
    Number.isSafeInteger(args.concurrency) && args.concurrency > 0
      ? args.concurrency
      : DEFAULT_SWEEP_CONCURRENCY
  const info = workflowInfo()

  const runId =
    args.runId ??
    (await CancellationScope.nonCancellable(() =>
      startRun({
        trigger: mode === 'full' ? 'scheduled-full' : 'scheduled-incremental',
        scope,
        workflowId: info.workflowId,
        temporalRunId: info.runId,
      }),
    ))

  let totalProjects = args.counters?.totalProjects ?? 0
  let failed = args.counters?.failed ?? 0
  let lastProjectId = args.afterId
  let pageFull = false

  try {
    const projects = await findProjectsForSweep({
      mode,
      scope,
      afterId: args.afterId ?? null,
      limit: PAGE_SIZE,
    })

    totalProjects += projects.length
    pageFull = projects.length === PAGE_SIZE
    if (projects.length > 0) {
      lastProjectId = projects[projects.length - 1].id
    }

    for (let i = 0; i < projects.length; i += concurrency) {
      const window = projects.slice(i, i + concurrency)
      const results = await Promise.allSettled(
        window.map((project) =>
          executeChild(processProjectDocsReadiness, {
            workflowId: `${runId}-${project.id}`,
            args: [{ projectId: project.id, runId }],
          }),
        ),
      )

      for (const result of results) {
        if (result.status === 'rejected') {
          if (isCancellation(result.reason)) {
            throw result.reason
          }

          failed++
          log.warn('docs readiness child workflow failed', {
            error: (result.reason as Error)?.message ?? result.reason,
          })
        }
      }
    }
  } catch (err) {
    await CancellationScope.nonCancellable(() =>
      finishRun(runId, {
        status: 'failed',
        totalProjects,
        failed,
        errorMessage: (err as Error).message,
      }),
    )
    throw err
  }

  if (pageFull) {
    await continueAsNew<typeof runDocsReadinessSweep>({
      ...args,
      runId,
      afterId: lastProjectId,
      counters: { totalProjects, failed },
    })
    return
  }

  await CancellationScope.nonCancellable(() =>
    finishRun(runId, { status: 'completed', totalProjects, failed }),
  )
}
