import { describe, expect, it } from 'vitest'

import {
  buildOnboardedDiscussionAlert,
  buildOnboardedDiscussionReply,
  isGithubDiscussionRequest,
} from './onboardedRequestAlert'

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
