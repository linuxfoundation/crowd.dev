import { log, proxyActivities } from '@temporalio/workflow'

import type * as activities from '../activities/shadowDiffActivities'

const activity = proxyActivities<typeof activities>({
  startToCloseTimeout: '5 minutes',
  retry: { maximumAttempts: 3, backoffCoefficient: 2 },
})

const CHANNEL_CONCURRENCY = 10

export function describeChannelError(err: unknown): string {
  if (err instanceof Error) {
    return err.message
  }
  if (typeof err === 'string') {
    return err
  }
  try {
    return JSON.stringify(err) ?? String(err)
  } catch {
    return String(err)
  }
}

async function runChannel(
  channel: activities.IShadowDiffChannel,
  targetDay?: string,
): Promise<activities.IShadowDiffChannelResult> {
  try {
    return await activity.runShadowDiffForChannel(channel, targetDay)
  } catch (err) {
    log.error('shadow diff activity call failed for channel', {
      channelName: channel.channelName,
      err,
    })
    return {
      channelName: channel.channelName,
      integrationId: channel.integrationId,
      status: 'error',
      errorMessage: describeChannelError(err),
    }
  }
}

export async function shadowDiff(targetDay?: string): Promise<void> {
  const channels = await activity.listShadowDiffChannels()

  let okCount = 0
  let mappingMissingCount = 0
  let errorCount = 0

  for (let i = 0; i < channels.length; i += CHANNEL_CONCURRENCY) {
    const batch = channels.slice(i, i + CHANNEL_CONCURRENCY)
    const batchResults = await Promise.all(batch.map((channel) => runChannel(channel, targetDay)))
    for (const result of batchResults) {
      if (result.status === 'ok') {
        okCount++
      } else if (result.status === 'mapping_missing') {
        mappingMissingCount++
      } else {
        errorCount++
      }
    }
  }

  log.info('shadow diff run complete', {
    targetDay,
    channelCount: channels.length,
    okCount,
    mappingMissingCount,
    errorCount,
  })
}
