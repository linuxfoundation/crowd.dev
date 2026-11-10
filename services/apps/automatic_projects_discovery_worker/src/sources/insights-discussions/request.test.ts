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
    expect(request.linkedRepoCount).toBe(2)
    expect(request.unlistedRepoUrls).toEqual([])
    expect(request.projectName).toBe('Apache HugeGraph')
    expect(request.requestText).toBeNull()
    expect(request.parsed).toBe(parsed.request)
  })

  it('names the linked repositories the parser left out and leaves them out too', () => {
    const partial = {
      ...parsed,
      request: { ...parsed.request, githubRepoUrls: ['https://github.com/apache/hugegraph'] },
    }

    const request = toDiscussionRequest(discussion, partial)

    expect(request.unlistedRepoUrls).toEqual(['https://github.com/apache/hugegraph-ai'])
    expect(request.repoUrls).toEqual(['https://github.com/apache/hugegraph'])
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

  it('keeps the group when the GitHub check leaves a single repository of it', () => {
    const two = {
      ...parsed.request,
      githubRepoUrls: [
        'https://github.com/apache/hugegraph-old',
        'https://github.com/apache/hugegraph',
      ],
    }

    expect(
      discussionHints(['https://github.com/apache/hugegraph'], two).get(
        'https://github.com/apache/hugegraph',
      ),
    ).toEqual({
      project: 'Apache HugeGraph',
      mainRepoUrl: 'https://github.com/apache/hugegraph',
      suggestedSlug: 'apache-hugegraph',
      confidence: 0.9,
      declaredLf: false,
    })
  })

  it('keeps only the LF declaration as a hint for a single repository', () => {
    const repoUrl = 'https://github.com/apache/hugegraph'
    const single = { ...parsed.request, githubRepoUrls: [repoUrl], declaredLf: true }

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

describe('toDiscussionRequest with an organization link', () => {
  const orgDiscussion = {
    title: 'Onboarding request: OpenBMC',
    body: 'Please onboard every repository of https://github.com/openbmc, non-LF.',
  }

  it('keeps the parsed request when it points at an organization and no repository', () => {
    const request = toDiscussionRequest(orgDiscussion, {
      ...parsed,
      request: {
        ...parsed.request,
        githubRepoUrls: [],
        linksToFollow: ['https://github.com/openbmc'],
        projectName: 'OpenBMC',
      },
    })

    expect(request.organizations).toEqual(['openbmc'])
    expect(request.repoUrls).toEqual([])
    expect(request.projectName).toBe('OpenBMC')
    expect(request.requestText).toBeNull()
    expect(request.parsed).not.toBeNull()
  })

  it('follows the parser on which links count, and reads the text without a parser', () => {
    const excluded = toDiscussionRequest(orgDiscussion, {
      ...parsed,
      request: { ...parsed.request, githubRepoUrls: [], linksToFollow: [] },
    })
    expect(excluded.organizations).toEqual([])
    expect(excluded.parsed).toBeNull()

    const fallback = toDiscussionRequest(orgDiscussion, {
      ok: false,
      reason: 'bad shape',
    })
    expect(fallback.organizations).toEqual(['openbmc'])
    expect(fallback.requestText).toContain('Please onboard every repository')
  })
})

describe('discussionHints with organizations', () => {
  const organizations = [
    {
      owner: 'openbmc',
      repoUrls: ['https://github.com/openbmc/one', 'https://github.com/openbmc/two'],
    },
  ]

  it('marks the organization repositories and keeps the listed ones in their project group', () => {
    const hints = discussionHints(
      [
        'https://github.com/apache/hugegraph',
        'https://github.com/apache/hugegraph-ai',
        ...organizations[0].repoUrls,
      ],
      parsed.request,
      organizations,
    )

    expect(hints.get('https://github.com/openbmc/one')).toEqual({ organization: 'openbmc' })
    expect(hints.get('https://github.com/openbmc/two')).toEqual({ organization: 'openbmc' })
    expect(hints.get('https://github.com/apache/hugegraph-ai')).toMatchObject({
      project: 'Apache HugeGraph',
      mainRepoUrl: 'https://github.com/apache/hugegraph',
    })
  })

  it('never picks a repository of the organization as the main one of the project group', () => {
    const request = {
      ...parsed.request,
      githubRepoUrls: [
        'https://github.com/openbmc/one',
        'https://github.com/apache/hugegraph',
        'https://github.com/apache/hugegraph-ai',
      ],
    }
    const hints = discussionHints(request.githubRepoUrls, request, organizations)

    expect(hints.get('https://github.com/openbmc/one')).toEqual({ organization: 'openbmc' })
    expect(hints.get('https://github.com/apache/hugegraph')).toMatchObject({
      mainRepoUrl: 'https://github.com/apache/hugegraph',
    })
  })

  it('marks the organization repositories even without a parsed request', () => {
    const hints = discussionHints(organizations[0].repoUrls, null, organizations)

    expect(hints.size).toBe(2)
    expect(hints.get('https://github.com/openbmc/one')).toEqual({ organization: 'openbmc' })
  })
})
