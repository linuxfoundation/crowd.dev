import {
  IDbProjectCatalog,
  isReviewAlertProvenance,
  isSlackBotProvenance,
  isSlackPermalink,
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

  if (fromSlack) {
    return project.sourceUrl && isSlackPermalink(project.sourceUrl)
      ? `<${project.sourceUrl}|Slack request>`
      : '_source Slack message not recorded_'
  }

  return project.sourceUrl
    ? `<${project.sourceUrl}|${project.sourceUrl}>`
    : '_source discussion not recorded_'
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

type OnboardedDiscussionProject = Pick<
  IDbProjectCatalog,
  'repoName' | 'repoUrl' | 'projectSlug' | 'sourceUrl'
>

export function groupGithubDiscussionRequestsBySource<
  T extends Pick<IDbProjectCatalog, 'id' | 'provenance' | 'sourceUrl'>,
>(projects: T[]): T[][] {
  const groups = new Map<string, T[]>()

  for (const project of projects.filter(isGithubDiscussionRequest)) {
    const key = project.sourceUrl ?? `unrecorded:${project.id}`
    groups.set(key, [...(groups.get(key) ?? []), project])
  }

  return [...groups.values()]
}

export function buildOnboardedDiscussionTitle(projects: OnboardedDiscussionProject[]): string {
  const subject = projects.length === 1 ? projects[0].repoName : `${projects.length} repositories`
  return `Onboarded from GitHub discussion — ${subject}`
}

export function buildOnboardedDiscussionReply(projects: OnboardedDiscussionProject[]): string {
  if (projects.length === 1) {
    const [project] = projects
    const slug = deriveProjectSlug(project.projectSlug)
    return `Thanks for the suggestion! ${project.repoName} is now onboarded to LFX Insights — data collection has started and the first metrics can take a few hours to show up. You can follow it here: ${INSIGHTS_PROJECT_URL_BASE}/${slug}`
  }

  const repoLines = projects.map(
    (project) =>
      `- ${project.repoName}: ${INSIGHTS_PROJECT_URL_BASE}/${deriveProjectSlug(project.projectSlug)}`,
  )

  return [
    'Thanks for the suggestions! The following repositories are now onboarded to LFX Insights — data collection has started and the first metrics can take a few hours to show up:',
    ...repoLines,
  ].join('\n')
}

function buildOnboardedDiscussionHeader(
  projects: OnboardedDiscussionProject[],
): SlackMessageSection {
  if (projects.length === 1) {
    return buildDiscussionRequestHeader(projects[0])
  }

  const repoLines = projects.map((project) => `• *${project.repoName}* ${project.repoUrl}`)

  return {
    title: '',
    text: [
      `*${projects.length} repositories onboarded*`,
      ...repoLines,
      `Requested in: ${formatRequestedIn(projects[0])}`,
    ].join('\n'),
  }
}

export function buildOnboardedDiscussionAlert(
  projects: OnboardedDiscussionProject[],
): SlackMessageSection[] {
  return [
    buildOnboardedDiscussionHeader(projects),
    {
      title: 'Suggested reply',
      text: ['```', buildOnboardedDiscussionReply(projects), '```'].join('\n'),
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
