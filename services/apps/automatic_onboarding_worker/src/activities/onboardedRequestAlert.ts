import { IDbProjectCatalog } from '@crowd/data-access-layer/src/project-catalog/types'
import { SlackMessageSection } from '@crowd/slack'

import { deriveProjectSlug } from '../onboarder/onboarder'

const INSIGHTS_PROJECT_URL_BASE = 'https://insights.linuxfoundation.org/project'

export function isGithubDiscussionRequest(project: Pick<IDbProjectCatalog, 'provenance'>): boolean {
  return project.provenance === 'github-discussion'
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
  const requestedIn = project.sourceUrl
    ? `<${project.sourceUrl}|${project.sourceUrl}>`
    : '_source discussion not recorded_'

  return [
    {
      title: '',
      text: [`*${project.repoName}*`, project.repoUrl, `Requested in: ${requestedIn}`].join('\n'),
    },
    {
      title: 'Suggested reply',
      text: ['```', buildOnboardedDiscussionReply(project), '```'].join('\n'),
    },
  ]
}
