import { getServiceLogger } from '@crowd/logging'

import { SlackChannel, SlackChannelConfig } from './types'

const log = getServiceLogger()

const CHANNEL_WEBHOOK_URLS: Record<SlackChannel, string | undefined> = {
  [SlackChannel.CDP_ALERTS]: process.env.CDP_ALERTS_SLACK_WEBHOOK_URL,
  [SlackChannel.CDP_CRITICAL_ALERTS]: process.env.CDP_CRITICAL_ALERTS_SLACK_WEBHOOK_URL,
  [SlackChannel.CDP_DATA_QUALITY_ALERTS]: process.env.CDP_DATA_QUALITY_ALERTS_SLACK_WEBHOOK_URL,
  [SlackChannel.CDP_INTEGRATIONS_ALERTS]: process.env.CDP_INTEGRATIONS_ALERTS_SLACK_WEBHOOK_URL,
  [SlackChannel.CDP_PROJECTS_ALERTS]: process.env.CDP_PROJECTS_ALERTS_SLACK_WEBHOOK_URL,
  [SlackChannel.INSIGHTS_ALERTS]: process.env.INSIGHTS_ALERTS_SLACK_WEBHOOK_URL,
  [SlackChannel.INSIGHTS_CRITICAL_ALERTS]: process.env.INSIGHTS_CRITICAL_ALERTS_SLACK_WEBHOOK_URL,
}

// Check for missing webhook URLs on initialization
function checkChannelConfigurations(): void {
  const channels = Object.values(SlackChannel)
  const missingChannels: string[] = []

  for (const channel of channels) {
    if (!CHANNEL_WEBHOOK_URLS[channel]) {
      missingChannels.push(channel)
    }
  }

  if (missingChannels.length > 0) {
    log.warn(
      { missingChannels },
      `Slack webhook URLs not configured for channels: ${missingChannels.join(', ')}. Set the following env vars: ${missingChannels.map((c) => `${c}_SLACK_WEBHOOK_URL`).join(', ')}.`,
    )
  } else {
    log.debug('All Slack channel webhook URLs are configured')
  }
}

// Run initialization check on module load
checkChannelConfigurations()

export function getChannelConfig(channel: SlackChannel): SlackChannelConfig {
  return {
    webhookUrl: CHANNEL_WEBHOOK_URLS[channel],
  }
}
