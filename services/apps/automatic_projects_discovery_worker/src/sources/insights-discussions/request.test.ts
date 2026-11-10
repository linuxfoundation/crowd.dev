import { describe, expect, it } from 'vitest'

import { discussionHints, toDiscussionRequest } from './request'

const discussion = {
  title: 'Onboarding request: Apache HugeGraph',
  body: [
    'Please onboard Apache HugeGraph, slug apache-hugegraph.',
    '- https://github.com/apache/hugegraph',
    '- https://github.com/apache/hugegraph-ai',
    'Two archived repositories, apache/hugegraph-commons and apache/hugegraph-tools, should be included.',
  ].join('\n'),
}

const parsed = {
  ok: true as const,
  request: {
    githubRepoUrls: [
      'https://github.com/apache/hugegraph',
      'https://github.com/apache/hugegraph-ai',
      'https://github.com/apache/hugegraph-commons',
      'https://github.com/apache/hugegraph-tools',
    ],
    nonGithubRepoUrls: [],
    linksToFollow: [],
    projectName: 'Apache HugeGraph',
    declaredLf: false,
    asksAboutHierarchy: false,
    suggestedSlug: 'apache-hugegraph',
    confidence: 0.9,
  },
}

describe('toDiscussionRequest', () => {
  it('keeps every repository the parser read, flags the ones written without a link', () => {
    const request = toDiscussionRequest(discussion, parsed)

    expect(request.repoUrls).toEqual(parsed.request.githubRepoUrls)
    expect(request.bareRepoUrls).toEqual([
      'https://github.com/apache/hugegraph-commons',
      'https://github.com/apache/hugegraph-tools',
    ])
    expect(request.linkedRepoUrls).toBe(2)
    expect(request.unlistedRepoUrls).toEqual([])
    expect(request.projectName).toBe('Apache HugeGraph')
    expect(request.requestText).toBeNull()
    expect(request.parsed).toBe(parsed.request)
  })

  it('names the linked repositories the parser left out', () => {
    const partial = {
      ...parsed,
      request: { ...parsed.request, githubRepoUrls: ['https://github.com/apache/hugegraph'] },
    }

    expect(toDiscussionRequest(discussion, partial).unlistedRepoUrls).toEqual([
      'https://github.com/apache/hugegraph-ai',
    ])
  })

  it('falls back to the linked repositories and keeps the text when the parse failed', () => {
    const noRepositories = { ...parsed, request: { ...parsed.request, githubRepoUrls: [] } }
    for (const failed of [null, { ok: false as const, reason: 'bad shape' }, noRepositories]) {
      const request = toDiscussionRequest(discussion, failed)

      expect(request.repoUrls).toEqual([
        'https://github.com/apache/hugegraph',
        'https://github.com/apache/hugegraph-ai',
      ])
      expect(request.bareRepoUrls).toEqual([])
      expect(request.projectName).toBeNull()
      expect(request.requestText).toContain('Please onboard Apache HugeGraph')
      expect(request.parsed).toBeNull()
    }
  })
})

describe('discussionHints', () => {
  it('groups the repositories that are left after the GitHub check under the project name', () => {
    const kept = parsed.request.githubRepoUrls.slice(1)
    const hints = discussionHints(kept, parsed.request)

    expect(hints.get('https://github.com/apache/hugegraph-tools')).toEqual({
      project: 'Apache HugeGraph',
      mainRepoUrl: 'https://github.com/apache/hugegraph-ai',
      suggestedSlug: 'apache-hugegraph',
      confidence: 0.9,
      declaredLf: false,
    })
    expect(hints.has('https://github.com/apache/hugegraph')).toBe(false)
  })

  it('keeps only the LF declaration as a hint for a single repository', () => {
    const single = { ...parsed.request, declaredLf: true }
    const repoUrl = 'https://github.com/apache/hugegraph'

    expect(discussionHints([repoUrl], single).get(repoUrl)).toEqual({ declaredLf: true })
    expect(discussionHints([repoUrl], { ...single, declaredLf: null }).size).toBe(0)
    expect(discussionHints([repoUrl], null).size).toBe(0)
  })
})

describe('toDiscussionRequest with a hierarchy question', () => {
  it('keeps the text so the classification can answer the question as before', () => {
    const asking = {
      ...parsed,
      request: { ...parsed.request, asksAboutHierarchy: true },
    }

    const request = toDiscussionRequest(discussion, asking)

    expect(request.parsed).toBeNull()
    expect(request.requestText).toContain('Please onboard Apache HugeGraph')
    expect(request.repoUrls).toEqual([
      'https://github.com/apache/hugegraph',
      'https://github.com/apache/hugegraph-ai',
    ])
  })
})
