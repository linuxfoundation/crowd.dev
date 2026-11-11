import { beforeEach, describe, expect, it, vi } from 'vitest'

import { GithubPublicClientError } from '@crowd/common'

import { IDiscussionSourceDeps, InsightsDiscussionsSource, collectDiscussionRefs } from './source'

function fakeDeps(): IDiscussionSourceDeps {
  return {
    parseRequest: vi.fn(),
    lookupRepository: vi.fn(async () => true),
    lookupOrganization: vi.fn(),
    noteOrganizationFailures: vi.fn(async () => undefined),
    heartbeat: vi.fn(),
  }
}

const discussion = {
  number: 77,
  url: 'https://github.com/linuxfoundation/insights/discussions/77',
  title: 'Onboarding request: OpenBMC',
  body: 'Please onboard https://github.com/openbmc and https://github.com/acme/tool, non-LF.',
  closed: false,
  updatedAt: '2026-11-10T00:00:00Z',
}

const parsedRequest = {
  githubRepoUrls: ['https://github.com/acme/tool'],
  nonGithubRepoUrls: [],
  linksToFollow: ['https://github.com/openbmc'],
  projectName: 'OpenBMC',
  declaredLf: false,
  asksAboutHierarchy: false,
  suggestedSlug: null,
  confidence: 0.8,
}

describe('collectDiscussionRefs', () => {
  let deps: IDiscussionSourceDeps

  beforeEach(() => {
    deps = fakeDeps()
    vi.mocked(deps.parseRequest).mockResolvedValue({ ok: true, request: parsedRequest })
  })

  it('adds the repositories of a linked organization as one organization group', async () => {
    vi.mocked(deps.lookupOrganization).mockResolvedValue({
      repoUrls: ['https://github.com/openbmc/one', 'https://github.com/openbmc/two'],
      exceededLimit: false,
    })

    const refs = await collectDiscussionRefs(discussion, deps, new Set())

    expect(deps.lookupOrganization).toHaveBeenCalledWith('openbmc', 50)
    expect(refs.map((ref) => ref.repoUrl)).toEqual([
      'https://github.com/acme/tool',
      'https://github.com/openbmc/one',
      'https://github.com/openbmc/two',
    ])
    expect(refs[1]).toMatchObject({
      discussionUrl: discussion.url,
      requestText: null,
      projectName: 'OpenBMC',
      requestHints: { organization: 'openbmc' },
    })
    expect(refs[0].requestHints).toEqual({ declaredLf: false })
    expect(deps.noteOrganizationFailures).toHaveBeenCalledWith(discussion.url, [])
  })

  it('does not cap an organization the requester declared as LF', async () => {
    vi.mocked(deps.parseRequest).mockResolvedValue({
      ok: true,
      request: { ...parsedRequest, declaredLf: true },
    })
    vi.mocked(deps.lookupOrganization).mockResolvedValue({
      repoUrls: ['https://github.com/openbmc/one'],
      exceededLimit: false,
    })

    await collectDiscussionRefs(discussion, deps, new Set())

    expect(deps.lookupOrganization).toHaveBeenCalledWith('openbmc', 500)
  })

  it('tells the discussion why an organization was not expanded and adds nothing for it', async () => {
    vi.mocked(deps.lookupOrganization).mockRejectedValue(
      new GithubPublicClientError('NOT_FOUND', 'GitHub organization not found: openbmc'),
    )

    const refs = await collectDiscussionRefs(discussion, deps, new Set())

    expect(refs.map((ref) => ref.repoUrl)).toEqual(['https://github.com/acme/tool'])
    expect(deps.noteOrganizationFailures).toHaveBeenCalledWith(discussion.url, [
      expect.stringContaining(
        "I couldn't find a GitHub organization called **github.com/openbmc**",
      ),
    ])
  })

  it('fails the walk when GitHub cannot list the organization, after telling the discussion', async () => {
    vi.mocked(deps.lookupOrganization).mockRejectedValue(new Error('network error'))

    await expect(collectDiscussionRefs(discussion, deps, new Set())).rejects.toThrow(
      'expanding organizations of discussion #77 failed: network error',
    )
    expect(deps.noteOrganizationFailures).toHaveBeenCalledWith(discussion.url, [
      expect.stringContaining("GitHub isn't answering right now"),
    ])
  })

  it('expands the organization read from the text when the parser is unavailable', async () => {
    vi.mocked(deps.parseRequest).mockResolvedValue({
      ok: false,
      reason: 'bad shape',
    })
    vi.mocked(deps.lookupOrganization).mockResolvedValue({
      repoUrls: ['https://github.com/openbmc/one'],
      exceededLimit: false,
    })

    const refs = await collectDiscussionRefs(discussion, deps, new Set())

    expect(refs.map((ref) => ref.repoUrl)).toEqual([
      'https://github.com/acme/tool',
      'https://github.com/openbmc/one',
    ])
    expect(refs[1]).toMatchObject({
      requestText: expect.stringContaining('Please onboard'),
      requestHints: { organization: 'openbmc' },
    })
  })

  it('leaves out repositories another discussion already claimed', async () => {
    vi.mocked(deps.lookupOrganization).mockResolvedValue({
      repoUrls: ['https://github.com/openbmc/one'],
      exceededLimit: false,
    })

    const refs = await collectDiscussionRefs(
      discussion,
      deps,
      new Set(['https://github.com/acme/tool']),
    )

    expect(refs.map((ref) => ref.repoUrl)).toEqual(['https://github.com/openbmc/one'])
  })
})

describe('InsightsDiscussionsSource.parseRow', () => {
  const source = new InsightsDiscussionsSource(fakeDeps())

  it('propagates discussionUrl as sourceUrl', () => {
    const result = source.parseRow({
      repoUrl: 'https://github.com/foo/bar',
      discussionUrl: 'https://github.com/linuxfoundation/insights/discussions/42',
    })

    expect(result?.sourceUrl).toBe('https://github.com/linuxfoundation/insights/discussions/42')
  })

  it('propagates requestText', () => {
    const result = source.parseRow({
      repoUrl: 'https://github.com/foo/bar',
      requestText: 'Please onboard https://github.com/foo/bar',
    })

    expect(result?.requestText).toBe('Please onboard https://github.com/foo/bar')
  })

  it('propagates the project name and the group hints of a parsed discussion', () => {
    const requestHints = { project: 'Acme', mainRepoUrl: 'https://github.com/foo/bar' }
    const result = source.parseRow({
      repoUrl: 'https://github.com/foo/bar',
      requestText: null,
      projectName: 'Acme',
      requestHints,
    })

    expect(result).toMatchObject({ requestText: null, projectName: 'Acme', requestHints })
  })

  it('leaves sourceUrl undefined when discussionUrl is missing', () => {
    const result = source.parseRow({
      repoUrl: 'https://github.com/foo/bar',
    })

    expect(result?.sourceUrl).toBeUndefined()
  })

  it('returns null when repoUrl is missing', () => {
    const result = source.parseRow({
      discussionUrl: 'https://github.com/linuxfoundation/insights/discussions/42',
    })

    expect(result).toBeNull()
  })

  it('returns null when repoUrl cannot be parsed into a project identity', () => {
    const result = source.parseRow({
      repoUrl: 'https://github.com',
      discussionUrl: 'https://github.com/linuxfoundation/insights/discussions/42',
    })

    expect(result).toBeNull()
  })
})
