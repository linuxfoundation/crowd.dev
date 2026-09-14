import { GITHUB_GRID, GithubActivityType } from '@crowd/integrations'

import type { PrTimelineItem, PullRequestNode } from '../graphql/pullRequests'
import type { GithubActivity } from '../schemas'

import { toMember } from './member'

const DEFAULT_TIMESTAMP = '1970-01-01T00:00:00Z'

function prAttributes(pullRequest: PullRequestNode): Record<string, unknown> {
  return {
    additions: pullRequest.additions,
    deletions: pullRequest.deletions,
    changedFiles: pullRequest.changedFiles,
    authorAssociation: pullRequest.authorAssociation,
    labels: pullRequest.labels?.nodes?.map((label) => label.name) ?? [],
  }
}

function toTimelineActivity(
  item: PrTimelineItem,
  pullRequest: PullRequestNode,
): GithubActivity | null {
  switch (item.__typename) {
    case 'AssignedEvent': {
      if (!item.assignee || !item.id) {
        return null
      }
      const timestamp = item.createdAt ?? DEFAULT_TIMESTAMP
      return {
        type: GithubActivityType.PULL_REQUEST_ASSIGNED,
        timestamp,
        sourceId: `gen-AE_${pullRequest.id}_${item.actor?.login}_${item.assignee?.login}_${new Date(timestamp).toISOString()}`,
        sourceParentId: pullRequest.id,
        score: GITHUB_GRID[GithubActivityType.PULL_REQUEST_ASSIGNED].score,
        title: pullRequest.title ?? '',
        body: pullRequest.body ?? '',
        url: pullRequest.url,
        attributes: { state: pullRequest.state.toLowerCase(), ...prAttributes(pullRequest) },
        member: toMember(item.actor),
        objectMember: toMember(item.assignee),
      }
    }
    case 'ReviewRequestedEvent': {
      if (!item.requestedReviewer?.login || !item.id) {
        return null
      }
      const timestamp = item.createdAt ?? DEFAULT_TIMESTAMP
      return {
        type: GithubActivityType.PULL_REQUEST_REVIEW_REQUESTED,
        timestamp,
        sourceId: `gen-RRE_${pullRequest.id}_${item.actor?.login}_${item.requestedReviewer?.login}_${new Date(timestamp).toISOString()}`,
        sourceParentId: pullRequest.id,
        score: GITHUB_GRID[GithubActivityType.PULL_REQUEST_REVIEW_REQUESTED].score,
        title: pullRequest.title ?? '',
        body: pullRequest.body ?? '',
        url: pullRequest.url,
        attributes: { state: pullRequest.state.toLowerCase(), ...prAttributes(pullRequest) },
        member: toMember(item.actor),
        objectMember: toMember(item.requestedReviewer),
      }
    }
    case 'PullRequestReview': {
      if (!item.id) {
        return null
      }
      const timestamp = item.submittedAt ?? DEFAULT_TIMESTAMP
      return {
        type: GithubActivityType.PULL_REQUEST_REVIEWED,
        timestamp,
        sourceId: `gen-PRR_${pullRequest.id}_${item.author?.login}_${new Date(timestamp).toISOString()}`,
        sourceParentId: pullRequest.id,
        score: GITHUB_GRID[GithubActivityType.PULL_REQUEST_REVIEWED].score,
        title: pullRequest.title ?? '',
        body: item.body ?? pullRequest.body ?? '',
        url: pullRequest.url,
        attributes: {
          reviewState: item.state,
          state: pullRequest.state.toLowerCase(),
          ...prAttributes(pullRequest),
        },
        member: toMember(item.author),
      }
    }
    case 'MergedEvent': {
      if (!item.id) {
        return null
      }
      const timestamp = item.createdAt ?? DEFAULT_TIMESTAMP
      return {
        type: GithubActivityType.PULL_REQUEST_MERGED,
        timestamp,
        sourceId: `gen-ME_${pullRequest.id}_${item.actor?.login}_${new Date(timestamp).toISOString()}`,
        sourceParentId: pullRequest.id,
        score: GITHUB_GRID[GithubActivityType.PULL_REQUEST_MERGED].score,
        title: pullRequest.title ?? '',
        body: pullRequest.body ?? '',
        url: pullRequest.url,
        attributes: { state: 'merged', ...prAttributes(pullRequest) },
        member: toMember(item.actor),
      }
    }
    case 'ClosedEvent': {
      if (!item.id) {
        return null
      }
      const timestamp = item.createdAt ?? DEFAULT_TIMESTAMP
      return {
        type: GithubActivityType.PULL_REQUEST_CLOSED,
        timestamp,
        sourceId: `gen-CE_${pullRequest.id}_${item.actor?.login}_${new Date(timestamp).toISOString()}`,
        sourceParentId: pullRequest.id,
        score: GITHUB_GRID[GithubActivityType.PULL_REQUEST_CLOSED].score,
        title: pullRequest.title ?? '',
        body: pullRequest.body ?? '',
        url: pullRequest.url,
        attributes: { state: 'closed', ...prAttributes(pullRequest) },
        member: toMember(item.actor),
      }
    }
    default:
      return null
  }
}

export function toPullRequestActivities(
  pullRequest: PullRequestNode,
  timelineItems: (PrTimelineItem | null)[],
): GithubActivity[] {
  if (!pullRequest || !pullRequest.id) {
    return []
  }

  const activities: GithubActivity[] = [
    {
      type: GithubActivityType.PULL_REQUEST_OPENED,
      timestamp: pullRequest.createdAt ?? DEFAULT_TIMESTAMP,
      sourceId: pullRequest.id,
      score: GITHUB_GRID[GithubActivityType.PULL_REQUEST_OPENED].score,
      title: pullRequest.title ?? '',
      body: pullRequest.body ?? '',
      url: pullRequest.url,
      attributes: { state: 'open', ...prAttributes(pullRequest) },
      member: toMember(pullRequest.author),
    },
  ]

  for (const item of timelineItems) {
    if (!item) {
      continue
    }
    const activity = toTimelineActivity(item, pullRequest)
    if (activity) {
      activities.push(activity)
    }
  }

  return activities
}
