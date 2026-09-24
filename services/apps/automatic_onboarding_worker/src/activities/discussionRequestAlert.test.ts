import { describe, expect, it } from 'vitest'

import {
  buildErroredDiscussionAlert,
  buildOnboardedDiscussionAlert,
  buildOnboardedDiscussionReply,
  isGithubDiscussionRequest,
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
    const reply = buildOnboardedDiscussionReply({
      repoName: 'obmondo/kubeaid-cli',
      projectSlug: 'KubeAid CLI!!',
    })

    expect(reply).toContain('obmondo/kubeaid-cli')
    expect(reply).toContain('https://insights.linuxfoundation.org/project/kubeaid-cli')
  })

  it('normalizes an already slug-like projectSlug unchanged', () => {
    const reply = buildOnboardedDiscussionReply({
      repoName: 'agentnameservice/ans',
      projectSlug: 'agent-name-service',
    })

    expect(reply).toContain('https://insights.linuxfoundation.org/project/agent-name-service')
  })
})

describe('buildOnboardedDiscussionAlert', () => {
  it('links the source discussion when sourceUrl is present', () => {
    const sections = buildOnboardedDiscussionAlert({
      repoName: 'obmondo/kubeaid-cli',
      repoUrl: 'https://github.com/obmondo/kubeaid-cli',
      projectSlug: 'kubeaid-cli',
      sourceUrl: 'https://github.com/linuxfoundation/insights/discussions/123',
    })

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
    const sections = buildOnboardedDiscussionAlert({
      repoName: 'obmondo/kubeaid-cli',
      repoUrl: 'https://github.com/obmondo/kubeaid-cli',
      projectSlug: 'kubeaid-cli',
      sourceUrl: null,
    })

    expect(sections[0].text).toContain('source discussion not recorded')
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
