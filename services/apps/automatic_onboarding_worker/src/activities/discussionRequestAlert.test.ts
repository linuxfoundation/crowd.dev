import { describe, expect, it } from 'vitest'

import {
  buildErroredDiscussionAlert,
  buildOnboardedDiscussionAlert,
  buildOnboardedDiscussionReply,
  buildOnboardedDiscussionTitle,
  groupGithubDiscussionRequestsBySource,
  isGithubDiscussionRequest,
  isReviewAlertRequest,
} from './discussionRequestAlert'

describe('isGithubDiscussionRequest', () => {
  it('is true for github-discussion provenance', () => {
    expect(isGithubDiscussionRequest({ provenance: 'github-discussion' })).toBe(true)
  })

  it('is false for slack-tag provenance', () => {
    expect(isGithubDiscussionRequest({ provenance: 'slack-tag' })).toBe(false)
  })

  it('is false for lf-criticality-score provenance', () => {
    expect(isGithubDiscussionRequest({ provenance: 'lf-criticality-score' })).toBe(false)
  })

  it('is false for null provenance', () => {
    expect(isGithubDiscussionRequest({ provenance: null })).toBe(false)
  })
})

describe('buildOnboardedDiscussionReply', () => {
  it('includes the repo name and the derived slug in the project URL', () => {
    const reply = buildOnboardedDiscussionReply([
      {
        repoName: 'obmondo/kubeaid-cli',
        repoUrl: 'https://github.com/obmondo/kubeaid-cli',
        projectSlug: 'KubeAid CLI!!',
        sourceUrl: null,
      },
    ])

    expect(reply).toContain('obmondo/kubeaid-cli')
    expect(reply).toContain('https://insights.linuxfoundation.org/project/kubeaid-cli')
  })

  it('normalizes an already slug-like projectSlug unchanged', () => {
    const reply = buildOnboardedDiscussionReply([
      {
        repoName: 'agentnameservice/ans',
        repoUrl: 'https://github.com/agentnameservice/ans',
        projectSlug: 'agent-name-service',
        sourceUrl: null,
      },
    ])

    expect(reply).toContain('https://insights.linuxfoundation.org/project/agent-name-service')
  })
})

describe('buildOnboardedDiscussionAlert', () => {
  it('links the source discussion when sourceUrl is present', () => {
    const sections = buildOnboardedDiscussionAlert([
      {
        repoName: 'obmondo/kubeaid-cli',
        repoUrl: 'https://github.com/obmondo/kubeaid-cli',
        projectSlug: 'kubeaid-cli',
        sourceUrl: 'https://github.com/linuxfoundation/insights/discussions/123',
      },
    ])

    const summary = sections[0].text
    expect(summary).toContain('obmondo/kubeaid-cli')
    expect(summary).toContain('https://github.com/obmondo/kubeaid-cli')
    expect(summary).toContain('https://github.com/linuxfoundation/insights/discussions/123')

    const reply = sections[1]
    expect(reply.title).toBe('Suggested reply')
    expect(reply.text).toContain('```')
    expect(reply.text).toContain('kubeaid-cli')
  })

  it('falls back to a not-recorded note when sourceUrl is null', () => {
    const sections = buildOnboardedDiscussionAlert([
      {
        repoName: 'obmondo/kubeaid-cli',
        repoUrl: 'https://github.com/obmondo/kubeaid-cli',
        projectSlug: 'kubeaid-cli',
        sourceUrl: null,
      },
    ])

    expect(sections[0].text).toContain('source discussion not recorded')
  })
})

const DISCUSSION_URL = 'https://github.com/linuxfoundation/insights/discussions/2336'

const multiRepoProjects = [
  {
    repoName: 'acme/one',
    repoUrl: 'https://github.com/acme/one',
    projectSlug: 'one',
    sourceUrl: DISCUSSION_URL,
  },
  {
    repoName: 'acme/two',
    repoUrl: 'https://github.com/acme/two',
    projectSlug: 'two',
    sourceUrl: DISCUSSION_URL,
  },
]

describe('buildOnboardedDiscussionAlert for several repositories', () => {
  it('lists every repository and the discussion in a single header', () => {
    const [header] = buildOnboardedDiscussionAlert(multiRepoProjects)

    expect(header.text).toContain('2 repositories onboarded')
    expect(header.text).toContain('https://github.com/acme/one')
    expect(header.text).toContain('https://github.com/acme/two')
    expect(header.text).toContain(DISCUSSION_URL)
  })

  it('suggests one reply covering every repository', () => {
    const sections = buildOnboardedDiscussionAlert(multiRepoProjects)

    expect(sections).toHaveLength(2)
    expect(sections[1].title).toBe('Suggested reply')
    expect(sections[1].text).toContain('https://insights.linuxfoundation.org/project/one')
    expect(sections[1].text).toContain('https://insights.linuxfoundation.org/project/two')
  })
})

describe('buildOnboardedDiscussionTitle', () => {
  it('names the repository for a single project', () => {
    expect(buildOnboardedDiscussionTitle([multiRepoProjects[0]])).toBe(
      'Onboarded from GitHub discussion — acme/one',
    )
  })

  it('counts the repositories for several projects', () => {
    expect(buildOnboardedDiscussionTitle(multiRepoProjects)).toBe(
      'Onboarded from GitHub discussion — 2 repositories',
    )
  })
})

describe('groupGithubDiscussionRequestsBySource', () => {
  const discussion = (id: string, sourceUrl: string | null) => ({
    id,
    provenance: 'github-discussion' as const,
    sourceUrl,
  })

  it('groups projects that share a discussion', () => {
    const groups = groupGithubDiscussionRequestsBySource([
      discussion('1', 'https://github.com/o/r/discussions/1'),
      discussion('2', 'https://github.com/o/r/discussions/2'),
      discussion('3', 'https://github.com/o/r/discussions/1'),
    ])

    expect(groups.map((group) => group.map((project) => project.id))).toEqual([['1', '3'], ['2']])
  })

  it('keeps projects without a recorded source apart', () => {
    const groups = groupGithubDiscussionRequestsBySource([
      discussion('1', null),
      discussion('2', null),
    ])

    expect(groups).toHaveLength(2)
  })

  it('drops projects that did not come from a github discussion', () => {
    const groups = groupGithubDiscussionRequestsBySource([
      { id: '1', provenance: 'slack-bot', sourceUrl: 'https://acme.slack.com/archives/C1/p1' },
      { id: '2', provenance: null, sourceUrl: null },
    ])

    expect(groups).toEqual([])
  })
})

describe('buildErroredDiscussionAlert', () => {
  it('links the source discussion and includes the error reason', () => {
    const sections = buildErroredDiscussionAlert(
      {
        repoName: 'obmondo/kubeaid-cli',
        repoUrl: 'https://github.com/obmondo/kubeaid-cli',
        sourceUrl: 'https://github.com/linuxfoundation/insights/discussions/123',
      },
      'GitHub API rate limit exceeded',
    )

    const summary = sections[0].text
    expect(summary).toContain('obmondo/kubeaid-cli')
    expect(summary).toContain('https://github.com/obmondo/kubeaid-cli')
    expect(summary).toContain('https://github.com/linuxfoundation/insights/discussions/123')

    const error = sections[1]
    expect(error.title).toBe('Error')
    expect(error.text).toContain('```')
    expect(error.text).toContain('GitHub API rate limit exceeded')
  })

  it('falls back to a not-recorded note when sourceUrl is null', () => {
    const sections = buildErroredDiscussionAlert(
      {
        repoName: 'obmondo/kubeaid-cli',
        repoUrl: 'https://github.com/obmondo/kubeaid-cli',
        sourceUrl: null,
      },
      'unknown error',
    )

    expect(sections[0].text).toContain('source discussion not recorded')
  })

  it('truncates a very long error reason', () => {
    const longReason = 'x'.repeat(600)

    const sections = buildErroredDiscussionAlert(
      {
        repoName: 'obmondo/kubeaid-cli',
        repoUrl: 'https://github.com/obmondo/kubeaid-cli',
        sourceUrl: null,
      },
      longReason,
    )

    const errorText = sections[1].text
    expect(errorText).toContain('…')
    expect(errorText.length).toBeLessThan(longReason.length)
  })
})

describe('isReviewAlertRequest', () => {
  it('is true for github-discussion and slack-bot provenance', () => {
    expect(isReviewAlertRequest({ provenance: 'github-discussion' })).toBe(true)
    expect(isReviewAlertRequest({ provenance: 'slack-bot' })).toBe(true)
  })

  it('is false for slack-tag, bulk and null provenance', () => {
    expect(isReviewAlertRequest({ provenance: 'slack-tag' })).toBe(false)
    expect(isReviewAlertRequest({ provenance: 'lf-criticality-score' })).toBe(false)
    expect(isReviewAlertRequest({ provenance: null })).toBe(false)
  })
})

describe('buildErroredDiscussionAlert for slack-bot requests', () => {
  it('links the Slack message', () => {
    const [header] = buildErroredDiscussionAlert(
      {
        repoName: 'foo/bar',
        repoUrl: 'https://github.com/foo/bar',
        sourceUrl: 'https://acme.slack.com/archives/C1/p1',
        provenance: 'slack-bot',
      },
      'boom',
    )

    expect(header.text).toContain('<https://acme.slack.com/archives/C1/p1|Slack request>')
  })

  it('falls back to a Slack-specific note when the message was not recorded', () => {
    const [header] = buildErroredDiscussionAlert(
      {
        repoName: 'foo/bar',
        repoUrl: 'https://github.com/foo/bar',
        sourceUrl: null,
        provenance: 'slack-bot',
      },
      'boom',
    )

    expect(header.text).toContain('source Slack message not recorded')
  })

  it('does not present a retained non-Slack source url as a Slack request', () => {
    const [header] = buildErroredDiscussionAlert(
      {
        repoName: 'foo/bar',
        repoUrl: 'https://github.com/foo/bar',
        sourceUrl: 'https://github.com/foo/bar/discussions/1',
        provenance: 'slack-bot',
      },
      'boom',
    )

    expect(header.text).toContain('source Slack message not recorded')
    expect(header.text).not.toContain('discussions/1')
  })
})
