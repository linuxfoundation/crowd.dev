import { describe, expect, it, vi } from 'vitest'

import { GithubPublicClientError } from '@crowd/common'

import { checkRepositoriesOnGithub } from './repositoryCheck'

describe('checkRepositoriesOnGithub', () => {
  it('splits the repositories into present, missing and unchecked', async () => {
    const lookup = vi.fn(async (repoUrl: string) => {
      if (repoUrl.endsWith('/missing')) {
        return false
      }
      if (repoUrl.endsWith('/unparsable')) {
        throw new GithubPublicClientError('NOT_FOUND', 'Cannot parse GitHub URL')
      }
      if (repoUrl.endsWith('/limited')) {
        throw new GithubPublicClientError('RATE_LIMIT', 'rate limited')
      }
      return true
    })

    const result = await checkRepositoriesOnGithub(
      [
        'https://github.com/acme/one',
        'https://github.com/acme/missing',
        'https://github.com/acme/unparsable',
        'https://github.com/acme/limited',
      ],
      lookup,
    )

    expect(result).toEqual({
      missing: ['https://github.com/acme/missing', 'https://github.com/acme/unparsable'],
      unchecked: [{ repoUrl: 'https://github.com/acme/limited', reason: 'rate limited' }],
    })
  })

  it('does nothing for an empty list', async () => {
    const lookup = vi.fn()

    expect(await checkRepositoriesOnGithub([], lookup)).toEqual({
      missing: [],
      unchecked: [],
    })
    expect(lookup).not.toHaveBeenCalled()
  })
})
