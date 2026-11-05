import { describe, expect, it, vi } from 'vitest'

import { GithubPublicClientError } from '@crowd/common'

import {
  expandOrganizations,
  organizationNote,
  requestedOrganizations,
} from './organizationRequest'

const repos = (owner: string, count: number) =>
  Array.from({ length: count }, (_, index) => `https://github.com/${owner}/repo-${index + 1}`)

describe('expandOrganizations', () => {
  it('lists the public repositories of each organization within the shared limit', async () => {
    const lookup = vi
      .fn()
      .mockResolvedValueOnce({ repoUrls: repos('openbmc', 3), exceededLimit: false })
      .mockResolvedValueOnce({ repoUrls: repos('other', 2), exceededLimit: false })

    const result = await expandOrganizations(['openbmc', 'other'], lookup, 10)

    expect(result).toEqual({
      ok: true,
      organizations: [
        { owner: 'openbmc', repoUrls: repos('openbmc', 3) },
        { owner: 'other', repoUrls: repos('other', 2) },
      ],
    })
    expect(lookup).toHaveBeenNthCalledWith(1, 'openbmc', 10)
    expect(lookup).toHaveBeenNthCalledWith(2, 'other', 7)
  })

  it('explains an organization that is too large, empty, unknown or unreachable', async () => {
    const tooLarge = await expandOrganizations(
      ['openbmc'],
      vi.fn().mockResolvedValue({ repoUrls: repos('openbmc', 20), exceededLimit: true }),
      20,
    )
    expect(tooLarge).toMatchObject({
      ok: false,
      reply: expect.stringContaining(
        '*github.com/openbmc* has more than 20 public repositories, and for now I take up to 20 at a time.',
      ),
      reason: 'more than 20 repositories',
    })

    const combined = await expandOrganizations(
      ['first', 'second'],
      vi
        .fn()
        .mockResolvedValueOnce({ repoUrls: repos('first', 15), exceededLimit: false })
        .mockResolvedValueOnce({ repoUrls: repos('second', 5), exceededLimit: true }),
      20,
    )
    expect(combined).toMatchObject({
      ok: false,
      reply: expect.stringContaining(
        'Together, these organizations have more than 20 public repositories, and for now I take up to 20 per request.',
      ),
    })

    const exhausted = vi
      .fn()
      .mockResolvedValue({ repoUrls: repos('first', 20), exceededLimit: false })
    expect(await expandOrganizations(['first', 'second'], exhausted, 20)).toMatchObject({
      ok: false,
      reply: expect.stringContaining('Together, these organizations have more than 20'),
    })
    expect(exhausted).toHaveBeenCalledTimes(1)

    const empty = await expandOrganizations(
      ['openbmc'],
      vi.fn().mockResolvedValue({ repoUrls: [], exceededLimit: false }),
    )
    expect(empty).toMatchObject({
      ok: false,
      reply:
        '*github.com/openbmc* has no public repositories I can add: archived repositories and forks are left out.',
    })

    const unknown = await expandOrganizations(
      ['nobody-here'],
      vi.fn().mockRejectedValue(new GithubPublicClientError('NOT_FOUND', 'not found')),
    )
    expect(unknown).toMatchObject({
      ok: false,
      reply: expect.stringContaining(
        "I couldn't find a GitHub organization called *github.com/nobody-here*",
      ),
      reason: 'not found',
    })

    const down = await expandOrganizations(
      ['openbmc'],
      vi.fn().mockRejectedValue(new GithubPublicClientError('TRANSIENT', 'network error')),
    )
    expect(down).toMatchObject({
      ok: false,
      reply: expect.stringContaining("GitHub isn't answering right now"),
      reason: 'network error',
    })
  })
})

describe('requestedOrganizations', () => {
  const text = 'do not onboard https://github.com/ignored; onboard Acme https://github.com/acme/one'

  it('follows the parser when it ran, and reads the text only without a parser', () => {
    expect(requestedOrganizations(text, { linksToFollow: [] })).toEqual([])
    expect(requestedOrganizations(text, { linksToFollow: ['https://acme.org/wiki'] })).toEqual([])
    expect(
      requestedOrganizations('onboard OpenBMC https://github.com/openbmc/', {
        linksToFollow: ['https://github.com/openbmc/'],
      }),
    ).toEqual(['openbmc'])
    expect(requestedOrganizations(text, null)).toEqual(['ignored'])
  })
})

describe('organizationNote', () => {
  it('tells the requester how many repositories the organization brought in', () => {
    expect(organizationNote({ owner: 'openbmc', repoUrls: repos('openbmc', 12) })).toBe(
      "*github.com/openbmc* is a GitHub organization, so I'm adding its 12 public repositories (archived ones and forks left out).",
    )
    expect(organizationNote({ owner: 'solo', repoUrls: repos('solo', 1) })).toContain(
      'its 1 public repository (',
    )
  })
})
