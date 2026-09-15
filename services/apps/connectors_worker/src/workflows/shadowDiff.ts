import { continueAsNew, log, proxyActivities } from '@temporalio/workflow'

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
): Promise<activities.IShadowDiffChannelResult> {
  try {
    return await activity.runShadowDiffForChannel(channel)
  } catch (err) {
    log.error('shadow diff activity call failed for channel', {
      channelName: channel.channelName,
      err,
    })
    return {
      channelName: channel.channelName,
      integrationId: channel.integrationId,
      status: 'error',
      mismatches: [],
      totalMismatchCount: 0,
      errorMessage: describeChannelError(err),
    }
  }
}

export async function shadowDiff(startIndex = 0): Promise<void> {
  const channels = await activity.listShadowDiffChannels()

  if (startIndex >= channels.length) {
    return
  }

  const batch = channels.slice(startIndex, startIndex + CHANNEL_CONCURRENCY)
  const results = await Promise.all(batch.map(runChannel))
  await activity.reportShadowDiffResults(results)

  const nextIndex = startIndex + CHANNEL_CONCURRENCY
  if (nextIndex < channels.length) {
    await continueAsNew<typeof shadowDiff>(nextIndex)
  }
}
