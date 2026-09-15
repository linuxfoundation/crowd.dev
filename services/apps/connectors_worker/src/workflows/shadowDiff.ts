import { log, proxyActivities } from '@temporalio/workflow'

import type * as activities from '../activities/shadowDiffActivities'

const activity = proxyActivities<typeof activities>({
  startToCloseTimeout: '5 minutes',
  retry: { maximumAttempts: 3, backoffCoefficient: 2 },
})

export async function shadowDiff(): Promise<void> {
  const channels = await activity.listShadowDiffChannels()

  const results: activities.IShadowDiffChannelResult[] = []
  for (const channel of channels) {
    try {
      results.push(await activity.runShadowDiffForChannel(channel))
    } catch (err) {
      log.error('shadow diff activity call failed for channel', {
        channelName: channel.channelName,
        err,
      })
      results.push({ channelName: channel.channelName, status: 'error', mismatches: [] })
    }
  }

  await activity.reportShadowDiffResults(results)
}
