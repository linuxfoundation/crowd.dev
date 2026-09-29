import { WorkflowNotFoundError } from '@temporalio/client'

import {
  IDbDocReadinessRun,
  finishDocReadinessRun,
  findStaleRunningDocReadinessRuns,
} from '@crowd/data-access-layer'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { SlackChannel, SlackPersona, sendSlackNotificationAsync } from '@crowd/slack'

import { svc } from '../main'

type StrandedRun = Pick<IDbDocReadinessRun, 'id' | 'workflowId' | 'temporalRunId'>

const CLOSED_STATUSES = new Set(['COMPLETED', 'FAILED', 'CANCELLED', 'TERMINATED', 'TIMED_OUT'])
const MAX_LISTED_RUNS = 20

async function describeStatus(workflowId: string, runId?: string): Promise<string | null> {
  try {
    const { status } = await svc.temporal.workflow.getHandle(workflowId, runId).describe()
    return status.name
  } catch (err) {
    if (err instanceof WorkflowNotFoundError) {
      return null
    }
    throw err
  }
}

// Anything that cannot be classified counts as alive, so a live run is never closed.
async function closedExecutionStatus(run: StrandedRun): Promise<string | null> {
  const own = await describeStatus(run.workflowId, run.temporalRunId ?? undefined)
  if (own !== null && own !== 'CONTINUED_AS_NEW') {
    return CLOSED_STATUSES.has(own) ? own : null
  }

  // A sweep row keeps the first run's id: the chain lives while the workflow's latest run does.
  const latest = await describeStatus(run.workflowId)
  if (latest !== null && !CLOSED_STATUSES.has(latest)) {
    return null
  }

  return own === null ? 'NOT_FOUND' : (latest ?? 'NOT_FOUND')
}

export async function closeStrandedRuns(): Promise<void> {
  const readerQx = pgpQx(svc.postgres.reader.connection())
  const writerQx = pgpQx(svc.postgres.writer.connection())

  const runningRuns = await findStaleRunningDocReadinessRuns(readerQx, new Date())

  const closed: { run: StrandedRun; status: string }[] = []

  for (const run of runningRuns) {
    if (!run.workflowId) {
      continue
    }

    let status: string | null
    try {
      status = await closedExecutionStatus(run)
    } catch (err) {
      svc.log.warn({ runId: run.id, err }, 'could not check docs readiness run against Temporal')
      continue
    }
    if (!status) {
      continue
    }

    try {
      const finished = await finishDocReadinessRun(writerQx, run.id, {
        status: 'failed',
        errorMessage: `run never finished: workflow execution is ${status}`,
      })
      if (finished) {
        closed.push({ run, status })
      }
    } catch (err) {
      svc.log.warn({ runId: run.id, err }, 'could not close stranded docs readiness run')
    }
  }

  if (closed.length === 0) {
    return
  }

  const listed = closed
    .slice(0, MAX_LISTED_RUNS)
    .map(({ run, status }) => `\`${run.id}\` (workflow \`${run.workflowId}\`, ${status})`)
  if (closed.length > MAX_LISTED_RUNS) {
    listed.push(`…and ${closed.length - MAX_LISTED_RUNS} more`)
  }

  await sendSlackNotificationAsync(
    SlackChannel.CDP_ALERTS,
    SlackPersona.WARNING_PROPAGATOR,
    `Docs readiness closed ${closed.length} run(s) that never finished`,
    listed.join('\n'),
  )
}
