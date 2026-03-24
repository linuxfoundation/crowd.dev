import { IncomingWebhook } from '@slack/webhook'

import { getServiceLogger } from '@crowd/logging'

import { getChannelConfig } from './channels'
import { SlackChannel } from './types'

const log = getServiceLogger()

const webhookClients: Partial<Record<SlackChannel, IncomingWebhook>> = {}

export function getWebhookClient(channel: SlackChannel): IncomingWebhook | null {
  // Return cached client if available
  const cachedClient = webhookClients[channel]
  if (cachedClient) {
    return cachedClient
  }

  const config = getChannelConfig(channel)

  if (!config.webhookUrl) {
    log.warn(
      { channel },
      `Slack webhook URL not configured for channel ${channel}. Set ${channel}_SLACK_WEBHOOK_URL environment variable.`,
    )
    return null
  }

  try {
    const client = new IncomingWebhook(config.webhookUrl)
    webhookClients[channel] = client
    return client
  } catch (error) {
    log.error({ error, channel }, `Failed to create Slack webhook client for channel ${channel}`)
    return null
  }
}
