import {
  IDbProjectCatalog,
  isReviewAlertProvenance,
  isSlackBotProvenance,
} from '@crowd/data-access-layer/src/project-catalog/types'
import { deriveProjectSlug } from '@crowd/project-onboarding'
import { SlackMessageSection } from '@crowd/slack'

const INSIGHTS_PROJECT_URL_BASE = 'https://insights.linuxfoundation.org/project'
const MAX_REASON_LENGTH = 500

export function isGithubDiscussionRequest(project: Pick<IDbProjectCatalog, 'provenance'>): boolean {
  return project.provenance === 'github-discussion'
}

export function isReviewAlertRequest(project: Pick<IDbProjectCatalog, 'provenance'>): boolean {
  return isReviewAlertProvenance(project.provenance)
}

function formatRequestedIn(
  project: Pick<IDbProjectCatalog, 'sourceUrl'> & Partial<Pick<IDbProjectCatalog, 'provenance'>>,
): string {
  const fromSlack = isSlackBotProvenance(project.provenance ?? null)

  if (!project.sourceUrl) {
    return fromSlack ? '_source Slack message not recorded_' : '_source discussion not recorded_'
  }

  return `<${project.sourceUrl}|${fromSlack ? 'Slack request' : project.sourceUrl}>`
}

function buildDiscussionRequestHeader(
  project: Pick<IDbProjectCatalog, 'repoName' | 'repoUrl' | 'sourceUrl'> &
    Partial<Pick<IDbProjectCatalog, 'provenance'>>,
): SlackMessageSection {
  const requestedIn = formatRequestedIn(project)

  return {
    title: '',
    text: [`*${project.repoName}*`, project.repoUrl, `Requested in: ${requestedIn}`].join('\n'),
  }
}

function truncateReason(reason: string): string {
  return reason.length > MAX_REASON_LENGTH ? `${reason.slice(0, MAX_REASON_LENGTH)}…` : reason
}

export function buildOnboardedDiscussionReply(
  project: Pick<IDbProjectCatalog, 'repoName' | 'projectSlug'>,
): string {
  const slug = deriveProjectSlug(project.projectSlug)
  return `Thanks for the suggestion! ${project.repoName} is now onboarded to LFX Insights — data collection has started and the first metrics can take a few hours to show up. You can follow it here: ${INSIGHTS_PROJECT_URL_BASE}/${slug}`
}

export function buildOnboardedDiscussionAlert(
  project: Pick<IDbProjectCatalog, 'repoName' | 'repoUrl' | 'projectSlug' | 'sourceUrl'>,
): SlackMessageSection[] {
  return [
    buildDiscussionRequestHeader(project),
    {
      title: 'Suggested reply',
      text: ['```', buildOnboardedDiscussionReply(project), '```'].join('\n'),
    },
  ]
}

export function buildErroredDiscussionAlert(
  project: Pick<IDbProjectCatalog, 'repoName' | 'repoUrl' | 'sourceUrl'> &
    Partial<Pick<IDbProjectCatalog, 'provenance'>>,
  reason: string,
): SlackMessageSection[] {
  return [
    buildDiscussionRequestHeader(project),
    {
      title: 'Error',
      text: ['```', truncateReason(reason), '```'].join('\n'),
    },
  ]
}
