import { executeChild, log, proxyActivities } from '@temporalio/workflow'

import type { CountWindow, IValidationChannel } from '@crowd/data-access-layer/src/connectors'

import type * as activities from '../activities/completenessActivities'
import { previousValidationWeek } from '../completenessWindow'
import { describeChannelError } from './shadowDiff'
import { validateCompleteness } from './validateCompleteness'

const { listValidationChannels } = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
  retry: { maximumAttempts: 3, backoffCoefficient: 2 },
})

const WEEKLY_VALIDATION_PAGE_SIZE = 10

interface IChildOutcome {
  failed: boolean
  resultCount: number
  failureCount: number
}

async function runChannel(
  channel: IValidationChannel,
  window: Extract<CountWindow, { kind: 'range' }>,
): Promise<IChildOutcome> {
  try {
    const result = await executeChild(validateCompleteness, {
      workflowId: `validate-completeness-${channel.integrationId}-${channel.channelId}-${window.from.slice(0, 10)}`,
      workflowExecutionTimeout: '2 hours',
      args: [
        {
          target: {
            kind: 'channel',
            channelName: channel.channelName,
            integrationId: channel.integrationId,
          },
          window,
          counterKeys: null,
        },
      ],
    })
    return { failed: false, resultCount: result.resultCount, failureCount: result.failureCount }
  } catch (err) {
    log.error('weekly completeness validation failed for repo', {
      channelName: channel.channelName,
      integrationId: channel.integrationId,
      errMsg: describeChannelError(err),
    })
    return { failed: true, resultCount: 0, failureCount: 0 }
  }
}

export async function weeklyCompletenessValidation(): Promise<void> {
  const window = previousValidationWeek(new Date())
  let after: { integrationId: string; channelId: string } | undefined
  let repoCount = 0
  let failedRepoCount = 0
  let resultCount = 0
  let failureCount = 0

  for (;;) {
    const channels = await listValidationChannels({ after, limit: WEEKLY_VALIDATION_PAGE_SIZE })
    if (channels.length === 0) {
      break
    }

    const outcomes = await Promise.all(channels.map((channel) => runChannel(channel, window)))
    for (const outcome of outcomes) {
      if (outcome.failed) {
        failedRepoCount++
      }
      resultCount += outcome.resultCount
      failureCount += outcome.failureCount
    }

    repoCount += channels.length
    const last = channels[channels.length - 1]
    after = { integrationId: last.integrationId, channelId: last.channelId }
  }

  log.info('weekly_completeness_validation_summary', {
    repoCount,
    failedRepoCount,
    resultCount,
    failureCount,
  })
}
