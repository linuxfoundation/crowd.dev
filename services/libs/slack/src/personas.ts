import { SlackPersona, SlackPersonaConfig } from './types'

export const PERSONA_CONFIGS: Record<SlackPersona, SlackPersonaConfig> = {
  [SlackPersona.ERROR_REPORTER]: {
    name: 'Error Reporter',
    icon: ':rotating_light:',
  },
  [SlackPersona.INFO_NOTIFIER]: {
    name: 'Info Notifier',
    icon: ':information_source:',
  },
  [SlackPersona.WARNING_PROPAGATOR]: {
    name: 'Warning Propagator',
    icon: ':warning:',
  },
  [SlackPersona.SUCCESS_ANNOUNCER]: {
    name: 'Success Announcer',
    icon: ':white_check_mark:',
  },
  [SlackPersona.DEBUG_TRACKER]: {
    name: 'Debug Tracker',
    icon: ':bug:',
  },
  [SlackPersona.CRITICAL_ALERTER]: {
    name: 'Critical Alerter',
    icon: ':fire:',
  },
  [SlackPersona.SUMMARY_REPORTER]: {
    name: 'Summary Reporter',
    icon: ':briefcase:',
  },
  [SlackPersona.METRICS_REPORTER]: {
    name: 'Metrics Reporter',
    icon: ':chart_with_upwards_trend:',
  },
}

export function getPersonaConfig(persona: SlackPersona): SlackPersonaConfig {
  return PERSONA_CONFIGS[persona]
}
