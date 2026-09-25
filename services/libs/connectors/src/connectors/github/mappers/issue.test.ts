import { describe, expect, it, vi } from 'vitest'

import type { IssueNode } from '../graphql/issues'
import { toIssueActivities } from './issue'

// @crowd/integrations eagerly scans and requires every integration folder on import,
// which fails outside its build environment and is unrelated to what's under test here.
vi.mock('@crowd/integrations', () => ({
  GithubActivityType: { ISSUE_OPENED: 'issue-opened', ISSUE_CLOSED: 'issue-closed' },
  GITHUB_GRID: {
    'issue-opened': { score: 1 },
    'issue-closed': { score: 1 },
  },
}))

function baseIssue(overrides: Partial<IssueNode> = {}): IssueNode {
  return {
    id: 'issue-1',
    number: 62642,
    title: 'some issue',
    url: 'https://github.com/openclaw/openclaw/issues/62642',
    state: 'OPEN',
    createdAt: '2026-07-01T00:00:00Z',
    updatedAt: '2026-07-01T00:00:00Z',
    bodyText: 'body',
    author: { login: 'author', databaseId: 1, __typename: 'User' },
    timelineItems: { nodes: [] },
    ...overrides,
  }
}

describe('toIssueActivities', () => {
  it('emits the latest close event for a reopened and re-closed issue', () => {
    const issue = baseIssue({
      state: 'CLOSED',
      updatedAt: '2026-09-24T10:00:00Z',
      timelineItems: {
        nodes: [
          {
            __typename: 'ClosedEvent',
            createdAt: '2026-09-24T10:00:00Z',
            actor: { login: 'closer', databaseId: 2, __typename: 'User' },
          },
        ],
      },
    })

    const activities = toIssueActivities(issue)

    const closeActivity = activities.find((a) => (a.type as string) === 'issue-closed')
    expect(closeActivity).toBeDefined()
    expect(closeActivity?.timestamp).toBe('2026-09-24T10:00:00Z')
    expect(closeActivity?.attributes).toMatchObject({ closedBy: 'closer' })
  })

  it('emits no close activity for a currently open issue', () => {
    const issue = baseIssue({
      state: 'OPEN',
      timelineItems: { nodes: [] },
    })

    const activities = toIssueActivities(issue)

    expect(activities.some((a) => (a.type as string) === 'issue-closed')).toBe(false)
    expect(activities).toHaveLength(1)
  })
})
