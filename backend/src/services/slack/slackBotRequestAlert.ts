import { IDbProjectCatalog } from '@crowd/data-access-layer/src/project-catalog/types'
import {
  SlackChannel,
  SlackMessageSection,
  SlackPersona,
  sendSlackNotificationAsync,
} from '@crowd/slack'

import { IServiceOptions } from '../IServiceOptions'

const MAX_REASON_LENGTH = 500

export type SlackBotRequestAlertKind = 'skipped' | 'errored'

type AlertProject = Pick<IDbProjectCatalog, 'repoName' | 'repoUrl' | 'sourceUrl'>

const ALERT_CONFIG: Record<
  SlackBotRequestAlertKind,
  { persona: SlackPersona; titlePrefix: string; reasonTitle: string }
> = {
  skipped: {
    persona: SlackPersona.WARNING_PROPAGATOR,
    titlePrefix: 'Skipped',
    reasonTitle: 'Skip reason',
  },
  errored: {
    persona: SlackPersona.ERROR_REPORTER,
    titlePrefix: 'Onboarding failed',
    reasonTitle: 'Error',
  },
}

function truncateReason(reason: string): string {
  return reason.length > MAX_REASON_LENGTH ? `${reason.slice(0, MAX_REASON_LENGTH)}…` : reason
}

export function buildSlackBotRequestAlert(
  kind: SlackBotRequestAlertKind,
  project: AlertProject,
  { reason, actorId }: { reason: string; actorId: string },
): SlackMessageSection[] {
  const requestedIn = project.sourceUrl
    ? `<${project.sourceUrl}|Slack request>`
    : '_source Slack message not recorded_'

  return [
    {
      title: '',
      text: [
        `*${project.repoName}*`,
        project.repoUrl,
        `Requested in: ${requestedIn}`,
        `Requested by: <@${actorId}>`,
      ].join('\n'),
    },
    {
      title: ALERT_CONFIG[kind].reasonTitle,
      text: ['```', truncateReason(reason), '```'].join('\n'),
    },
  ]
}

export async function notifySlackBotRequest(
  kind: SlackBotRequestAlertKind,
  project: AlertProject,
  { reason, actorId }: { reason: string; actorId: string },
  log: IServiceOptions['log'],
): Promise<void> {
  const { persona, titlePrefix } = ALERT_CONFIG[kind]

  try {
    await sendSlackNotificationAsync(
      SlackChannel.CDP_PROJECT_CATALOG_SKIP_ALERTS,
      persona,
      `${titlePrefix} — ${project.repoName}`,
      buildSlackBotRequestAlert(kind, project, { reason, actorId }),
    )
  } catch (err) {
    log.warn(err, 'Failed to send Slack-bot request alert.')
  }
}
