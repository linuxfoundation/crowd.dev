import { IDbProjectCatalog } from '@crowd/data-access-layer/src/project-catalog/types'
import { SlackMessageSection } from '@crowd/slack'

const MAX_REASON_LENGTH = 500

function truncateReason(reason: string): string {
  return reason.length > MAX_REASON_LENGTH ? `${reason.slice(0, MAX_REASON_LENGTH)}…` : reason
}

export function buildSkippedDiscussionAlert(
  project: Pick<IDbProjectCatalog, 'repoName' | 'repoUrl' | 'sourceUrl'>,
  reason: string,
): SlackMessageSection[] {
  const requestedIn = project.sourceUrl
    ? `<${project.sourceUrl}|${project.sourceUrl}>`
    : '_source discussion not recorded_'

  return [
    {
      title: '',
      text: [`*${project.repoName}*`, project.repoUrl, `Requested in: ${requestedIn}`].join('\n'),
    },
    {
      title: 'Skip reason',
      text: ['```', truncateReason(reason), '```'].join('\n'),
    },
  ]
}
