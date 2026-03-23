import { getServiceChildLogger } from '@crowd/logging'
import { SlackChannel, SlackPersona, sendSlackNotificationAsync } from '@crowd/slack'
import telemetry from '@crowd/telemetry'

const log = getServiceChildLogger('activity-interceptor')

async function telemetryDistribution(
  name: string,
  value: number,
  tags?: Record<string, string | number>,
) {
  telemetry.distribution(name, value, tags)
}

async function telemetryIncrement(
  name: string,
  value: number,
  tags?: Record<string, string | number>,
) {
  telemetry.increment(name, value, tags)
}

async function slackNotify(message: string, persona: SlackPersona | string, channel?: string) {
  // Accept strings to allow workflow code to pass string literals without importing enums
  const slackChannel = channel ? (channel as SlackChannel) : SlackChannel.CDP_ALERTS
  await sendSlackNotificationAsync(slackChannel, persona as SlackPersona, 'Temporal Alert', message)
  log.info('Slack notification sent from Temporal activity')
}

export { telemetryDistribution, telemetryIncrement, slackNotify }
