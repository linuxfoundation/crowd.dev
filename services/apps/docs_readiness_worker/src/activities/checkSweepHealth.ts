import {
  findLatestDocReadinessRun,
  findLatestProjectDocReadinessUpdatedAt,
  findStaleRunningDocReadinessRuns,
} from '@crowd/data-access-layer'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { SlackChannel, SlackPersona, sendSlackNotificationAsync } from '@crowd/slack'

import { svc } from '../main'

const STALE_AFTER_MS = 36 * 60 * 60 * 1000

async function alert(title: string, detail: string): Promise<void> {
  await sendSlackNotificationAsync(
    SlackChannel.CDP_ALERTS,
    SlackPersona.WARNING_PROPAGATOR,
    title,
    detail,
  )
}

const toIso = (value: string | Date): string => new Date(value).toISOString()

export async function checkIncrementalSweepHealth(): Promise<void> {
  const qx = pgpQx(svc.postgres.reader.connection())
  // The reaper just wrote here; a lagging replica could still list the rows it closed.
  const writerQx = pgpQx(svc.postgres.writer.connection())
  const cutoff = new Date(Date.now() - STALE_AFTER_MS)

  const run = await findLatestDocReadinessRun(qx, 'scheduled-incremental', 'completed')
  const staleRunning = await findStaleRunningDocReadinessRuns(writerQx, cutoff)
  const lastWrite = await findLatestProjectDocReadinessUpdatedAt(qx)

  const runFinishedRecently =
    !!run?.finishedAt && Date.now() - new Date(run.finishedAt).getTime() <= STALE_AFTER_MS

  if (!runFinishedRecently) {
    const detail = run
      ? `Latest completed run \`${run.id}\` started \`${run.startedAt}\`, finishedAt \`${run.finishedAt ?? 'null'}\`.`
      : 'No completed scheduled-incremental run found at all.'

    await alert('Docs readiness incremental sweep hasn’t completed in over 36h', detail)
  }

  if (staleRunning.length > 0) {
    await alert(
      `Docs readiness has ${staleRunning.length} run(s) stuck in running for over 36h`,
      staleRunning
        .map(
          (r) =>
            `\`${r.id}\` (workflow \`${r.workflowId ?? 'n/a'}\`, started \`${toIso(r.startedAt)}\`)`,
        )
        .join('\n'),
    )
  }

  // A healthy incremental sweep with nothing left to re-score writes no rows, so it is not a stall.
  const sweepHadNothingToDo = runFinishedRecently && run.totalProjects === 0

  if ((!lastWrite || lastWrite < cutoff) && !sweepHadNothingToDo) {
    await alert(
      'Docs readiness has not written a readiness row in over 36h',
      `Latest projectDocReadiness updatedAt: \`${lastWrite?.toISOString() ?? 'never'}\`.`,
    )
  }
}
