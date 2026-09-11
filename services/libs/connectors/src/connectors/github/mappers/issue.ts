import { GITHUB_GRID, GithubActivityType } from '@crowd/integrations'

import type { IssueNode } from '../graphql/issues'
import type { GithubActivity } from '../schemas'

import { toMember } from './member'

const DEFAULT_TIMESTAMP = '1970-01-01T00:00:00Z'

export function toIssueActivities(issue: IssueNode): GithubActivity[] {
  const activities: GithubActivity[] = [
    {
      type: GithubActivityType.ISSUE_OPENED,
      timestamp: issue.createdAt ?? DEFAULT_TIMESTAMP,
      sourceId: issue.id,
      score: GITHUB_GRID[GithubActivityType.ISSUE_OPENED].score,
      title: issue.title || '',
      body: issue.bodyText || '',
      url: issue.url,
      attributes: { state: 'open', issueNumber: issue.number },
      member: toMember(issue.author),
    },
  ]

  const closedEvent = issue.timelineItems.nodes.find((node) => node?.__typename === 'ClosedEvent')
  if (issue.state === 'CLOSED' && closedEvent) {
    const closedAt = closedEvent.createdAt ?? issue.updatedAt ?? DEFAULT_TIMESTAMP
    activities.push({
      type: GithubActivityType.ISSUE_CLOSED,
      timestamp: closedAt,
      sourceId: `gen-CE_${issue.id}_${closedEvent.actor?.login ?? issue.author?.login}_${new Date(
        closedAt,
      ).toISOString()}`,
      sourceParentId: issue.id,
      score: GITHUB_GRID[GithubActivityType.ISSUE_CLOSED].score,
      title: issue.title || '',
      body: issue.bodyText || '',
      url: issue.url,
      attributes: {
        state: 'closed',
        issueNumber: issue.number,
        ...(closedEvent.actor ? { closedBy: closedEvent.actor.login } : {}),
      },
      member: toMember(closedEvent.actor ?? issue.author),
    })
  }

  return activities
}
