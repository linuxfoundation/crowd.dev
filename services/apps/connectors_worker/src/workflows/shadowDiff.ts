import { log, proxyActivities } from '@temporalio/workflow'

import type { IShadowDiffChannelCursor } from '@crowd/data-access-layer/src/connectors'

import type * as activities from '../activities/shadowDiffActivities'

const activity = proxyActivities<typeof activities>({
  startToCloseTimeout: '5 minutes',
  retry: { maximumAttempts: 3, backoffCoefficient: 2 },
})

export const CHANNEL_CONCURRENCY = 50

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
  let after: IShadowDiffChannelCursor | undefined
  let channelCount = 0
  let okCount = 0
  let mappingMissingCount = 0
  let errorCount = 0

  for (;;) {
    const channels = await activity.listShadowDiffChannels({ after, limit: CHANNEL_CONCURRENCY })
    if (channels.length === 0) {
      break
    }

    const batchResults = await Promise.all(
      channels.map((channel) => runChannel(channel, targetDay)),
    )
    for (const result of batchResults) {
      if (result.status === 'ok') {
        okCount++
      } else if (result.status === 'mapping_missing') {
        mappingMissingCount++
      } else {
        errorCount++
      }
    }

    channelCount += channels.length
    const last = channels[channels.length - 1]
    after = { channelName: last.channelName, integrationId: last.integrationId }
  }

  log.info('shadow diff run complete', {
    targetDay,
    channelCount,
    okCount,
    mappingMissingCount,
    errorCount,
  })
}
