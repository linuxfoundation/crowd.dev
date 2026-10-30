import { executeChild, log, proxyActivities } from '@temporalio/workflow'

import type * as activities from '../activities/completenessActivities'
import { describeChannelError } from './shadowDiff'
import { validateCompleteness } from './validateCompleteness'

const { dueBackfillChecks, completeBackfillCheck } = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
  retry: { initialInterval: '10 seconds', backoffCoefficient: 2, maximumAttempts: 6 },
})

const BACKFILL_VALIDATION_BATCH_SIZE = 10

interface IBackfillCheck {
  integrationId: string
  channelId: string
  onboardedAt: string
}

async function validateBackfill(check: IBackfillCheck): Promise<boolean> {
  const failed = await runBackfillValidation(check)
  await completeBackfillCheck(check)
  return failed
}

async function runBackfillValidation({
  integrationId,
  channelId,
  onboardedAt,
}: IBackfillCheck): Promise<boolean> {
  try {
    const result = await executeChild(validateCompleteness, {
      workflowId: `validate-completeness-backfill-${integrationId}-${channelId}`,
      workflowExecutionTimeout: '2 hours',
      args: [
        {
          target: { kind: 'channelId', integrationId, channelId },
          window: { kind: 'all-time', to: onboardedAt },
          counterKeys: null,
        },
      ],
    })
    return result.failureCount > 0
  } catch (err) {
    log.error('post-backfill completeness validation failed for repo', {
      integrationId,
      channelId,
      errMsg: describeChannelError(err),
    })
    return true
  }
}

export async function completenessBackfillSweep(): Promise<void> {
  const checks = await dueBackfillChecks()
  let failedCount = 0

  for (let i = 0; i < checks.length; i += BACKFILL_VALIDATION_BATCH_SIZE) {
    const batch = checks.slice(i, i + BACKFILL_VALIDATION_BATCH_SIZE)
    const outcomes = await Promise.all(batch.map(validateBackfill))
    failedCount += outcomes.filter(Boolean).length
  }

  log.info('completeness_backfill_sweep_summary', { dueCount: checks.length, failedCount })
}
