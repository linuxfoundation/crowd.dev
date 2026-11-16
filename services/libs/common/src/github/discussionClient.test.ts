import { afterEach, describe, expect, it, vi } from 'vitest'

import {
  addGithubDiscussionComment,
  fetchGithubDiscussionId,
  getGithubDiscussionsToken,
  parseGithubDiscussionUrl,
  updateGithubDiscussionComment,
} from './discussionClient'
import { GithubPublicClientError } from './types'

function graphqlResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status })
}

afterEach(() => {
  vi.unstubAllGlobals()
  vi.unstubAllEnvs()
})

describe('parseGithubDiscussionUrl', () => {
  it('reads owner, repository and number from a discussion link', () => {
    expect(
      parseGithubDiscussionUrl('https://github.com/linuxfoundation/insights/discussions/2496'),
    ).toEqual({ owner: 'linuxfoundation', repo: 'insights', number: 2496 })
    expect(
      parseGithubDiscussionUrl('https://github.com/linuxfoundation/insights/discussions/2496/'),
    ).toEqual({ owner: 'linuxfoundation', repo: 'insights', number: 2496 })
    expect(parseGithubDiscussionUrl('https://acme.slack.com/archives/C1/p1')).toBeNull()
    expect(parseGithubDiscussionUrl('https://github.com/linuxfoundation/insights')).toBeNull()
  })
})

describe('getGithubDiscussionsToken', () => {
  it('reads the dedicated token and never falls back to a personal one', () => {
    vi.stubEnv('CROWD_GITHUB_DISCUSSIONS_TOKEN', 'bot-token')
    vi.stubEnv('CROWD_GITHUB_PERSONAL_ACCESS_TOKENS', 'pat-1,pat-2')
    expect(getGithubDiscussionsToken()).toBe('bot-token')

    vi.stubEnv('CROWD_GITHUB_DISCUSSIONS_TOKEN', '')
    expect(() => getGithubDiscussionsToken()).toThrow(GithubPublicClientError)
  })
})

describe('discussion comments', () => {
  const ref = { owner: 'linuxfoundation', repo: 'insights', number: 2496 }

  it('reads the discussion id and posts a comment with the token', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        graphqlResponse({ data: { repository: { discussion: { id: 'D_1' } } } }),
      )
      .mockResolvedValueOnce(
        graphqlResponse({
          data: {
            addDiscussionComment: { comment: { id: 'DC_1', url: 'https://github.com/c/1' } },
          },
        }),
      )
    vi.stubGlobal('fetch', fetchMock)

    expect(await fetchGithubDiscussionId(ref, 'tok')).toBe('D_1')
    expect(await addGithubDiscussionComment('D_1', 'hello', 'tok')).toEqual({
      id: 'DC_1',
      url: 'https://github.com/c/1',
    })

    const [url, init] = fetchMock.mock.calls[1]
    expect(url).toBe('https://api.github.com/graphql')
    expect(init.headers.Authorization).toBe('Bearer tok')
    expect(JSON.parse(init.body).variables).toEqual({ discussionId: 'D_1', body: 'hello' })
  })

  it('edits a comment in place', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(
        graphqlResponse({
          data: {
            updateDiscussionComment: { comment: { id: 'DC_1', url: 'https://github.com/c/1' } },
          },
        }),
      ),
    )

    expect(await updateGithubDiscussionComment('DC_1', 'edited', 'tok')).toMatchObject({
      id: 'DC_1',
    })
  })

  it('reports a missing discussion, a bad token and GraphQL errors', async () => {
    vi.stubGlobal(
      'fetch',
      vi
        .fn()
        .mockResolvedValueOnce(graphqlResponse({ data: { repository: { discussion: null } } })),
    )
    await expect(fetchGithubDiscussionId(ref, 'tok')).rejects.toMatchObject({ kind: 'NOT_FOUND' })

    vi.stubGlobal('fetch', vi.fn().mockResolvedValueOnce(graphqlResponse({}, 401)))
    await expect(fetchGithubDiscussionId(ref, 'tok')).rejects.toMatchObject({ kind: 'AUTH' })

    vi.stubGlobal(
      'fetch',
      vi
        .fn()
        .mockResolvedValueOnce(
          graphqlResponse({ errors: [{ type: 'FORBIDDEN', message: 'Resource not accessible' }] }),
        ),
    )
    await expect(addGithubDiscussionComment('D_1', 'x', 'tok')).rejects.toMatchObject({
      kind: 'AUTH',
      message: expect.stringContaining('Resource not accessible'),
    })
  })
})
