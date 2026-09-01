import { describe, expect, it } from 'vitest'

import { canonicalizeRepoUrl } from '../canonicalizeRepoUrl'
import { matchOwnership, repoOwnerFromCanonical } from '../ownershipMatch'

describe('matchOwnership', () => {
  it('matches a namespace equal to the repo owner', () => {
    expect(matchOwnership({ namespace: '@vercel', repoOwner: 'vercel' })).toBe('matched')
  })

  it('matches a maintainer username when the namespace does not', () => {
    expect(
      matchOwnership({ namespace: 'acme', maintainers: ['seldaek'], repoOwner: 'Seldaek' }),
    ).toBe('matched')
  })

  it('matches on a vanity suffix and on prefix equality', () => {
    expect(matchOwnership({ namespace: 'tokio-rs', repoOwner: 'tokio' })).toBe('matched')
    expect(matchOwnership({ namespace: 'langchain-ai', repoOwner: 'langchain' })).toBe('matched')
  })

  it('does not let a short key claim a longer owner by prefix', () => {
    expect(matchOwnership({ namespace: 'ab', repoOwner: 'abcdef' })).toBe('unmatched')
  })

  it('matches a reverse-DNS namespace segment', () => {
    expect(matchOwnership({ namespace: 'org.projectlombok', repoOwner: 'projectlombok' })).toBe(
      'matched',
    )
    expect(matchOwnership({ namespace: 'io.github.resilience4j', repoOwner: 'resilience4j' })).toBe(
      'matched',
    )
  })

  it('reports unmatched when evidence exists but nothing lines up', () => {
    expect(
      matchOwnership({ namespace: 'squatter', maintainers: ['nobody'], repoOwner: 'torvalds' }),
    ).toBe('unmatched')
  })

  it('reports no_evidence without a repo owner', () => {
    expect(matchOwnership({ namespace: 'vercel', repoOwner: null })).toBe('no_evidence')
  })

  it('reports no_evidence when the ecosystem exposes no namespace or maintainers', () => {
    expect(matchOwnership({ maintainers: [null, undefined, ''], repoOwner: 'vercel' })).toBe(
      'no_evidence',
    )
  })
})

describe('repoOwnerFromCanonical', () => {
  it('takes the first path segment', () => {
    const repo = canonicalizeRepoUrl('https://github.com/vercel/next.js')
    expect(repo && repoOwnerFromCanonical(repo)).toBe('vercel')
  })

  it('takes the top-level group of a gitlab subgroup path', () => {
    const repo = canonicalizeRepoUrl('https://gitlab.com/group/sub/project')
    expect(repo && repoOwnerFromCanonical(repo)).toBe('group')
  })
})
