import { findLatestDocReadinessRun } from '@crowd/data-access-layer'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { SlackChannel, SlackPersona, sendSlackNotificationAsync } from '@crowd/slack'

import { svc } from '../main'

const STALE_AFTER_MS = 36 * 60 * 60 * 1000

export async function checkIncrementalSweepHealth(): Promise<void> {
  const qx = pgpQx(svc.postgres.reader.connection())
  const run = await findLatestDocReadinessRun(qx, 'scheduled-incremental')

  const isStale =
    !run || !run.finishedAt || Date.now() - new Date(run.finishedAt).getTime() > STALE_AFTER_MS

  if (!isStale) {
    return
  }

  const detail = run
    ? `Latest run \`${run.id}\` has status \`${run.status}\`, started \`${run.startedAt}\`, finishedAt \`${run.finishedAt ?? 'null'}\`.`
    : 'No scheduled-incremental run found at all.'

  await sendSlackNotificationAsync(
    SlackChannel.CDP_ALERTS,
    SlackPersona.WARNING_PROPAGATOR,
    'Docs readiness incremental sweep hasn’t completed in over 36h',
    detail,
  )
}
