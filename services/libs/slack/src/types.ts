export enum SlackChannel {
  CDP_ALERTS = 'CDP_ALERTS',
  CDP_CRITICAL_ALERTS = 'CDP_CRITICAL_ALERTS',
  CDP_DATA_QUALITY_ALERTS = 'CDP_DATA_QUALITY_ALERTS',
  CDP_INTEGRATIONS_ALERTS = 'CDP_INTEGRATIONS_ALERTS',
  CDP_PROJECTS_ALERTS = 'CDP_PROJECTS_ALERTS',
  INSIGHTS_ALERTS = 'INSIGHTS_ALERTS',
  INSIGHTS_CRITICAL_ALERTS = 'INSIGHTS_CRITICAL_ALERTS',
}

export enum SlackPersona {
  ERROR_REPORTER = 'ERROR_REPORTER',
  INFO_NOTIFIER = 'INFO_NOTIFIER',
  WARNING_PROPAGATOR = 'WARNING_PROPAGATOR',
  SUCCESS_ANNOUNCER = 'SUCCESS_ANNOUNCER',
  DEBUG_TRACKER = 'DEBUG_TRACKER',
  CRITICAL_ALERTER = 'CRITICAL_ALERTER',
}

export interface SlackPersonaConfig {
  name: string
  icon: string
}

export interface SlackMessageSection {
  title: string
  text: string
}

export interface SlackMessage {
  channel: SlackChannel
  persona: SlackPersona
  title: string
  content: string | SlackMessageSection[]
}

export interface SlackChannelConfig {
  webhookUrl: string | undefined
}
