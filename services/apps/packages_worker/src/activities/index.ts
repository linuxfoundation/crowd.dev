import { getServiceChildLogger } from '@crowd/logging'
import { SlackChannel, SlackPersona, sendSlackNotificationAsync } from '@crowd/slack'

const log = getServiceChildLogger('activity-interceptor')

async function slackNotify(message: string, persona: SlackPersona | string) {
  await sendSlackNotificationAsync(
    SlackChannel.CDP_AKRITES_ALERTS,
    persona as SlackPersona,
    'Temporal Alert',
    message,
  )
  log.info('Slack notification sent from Temporal activity')
}

export { slackNotify }
