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

  it('does not match structural DNS/VCS segments against a repo owner', () => {
    // `io.github.attacker` must not claim the `github` org
    expect(matchOwnership({ namespace: 'io.github.attacker', repoOwner: 'github' })).toBe(
      'unmatched',
    )
    // `io.github.attacker` must not prefix-match `io-github` via the joined form `iogithubattacker`
    expect(matchOwnership({ namespace: 'io.github.attacker', repoOwner: 'io-github' })).toBe(
      'unmatched',
    )
    // `org.foo` must not match an owner literally named `org`
    expect(matchOwnership({ namespace: 'org.foo', repoOwner: 'org' })).toBe('unmatched')
    // `de.github-ai.pkg` — `github-ai` normalises to `github` via vanity-suffix stripping, filtered by set membership
    expect(matchOwnership({ namespace: 'de.github-ai.pkg', repoOwner: 'github' })).toBe('unmatched')
  })

  it('does not discard legitimate identity segments that share a prefix with a structural label', () => {
    // `io.github.github-tools` — `github-tools` normalises to `githubtools`, which is NOT structural
    // and must not be filtered even though it starts with `github`
    expect(matchOwnership({ namespace: 'io.github.github-tools', repoOwner: 'github-tools' })).toBe(
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

  it('excludes email-format maintainer strings to prevent domain-prefix false matches', () => {
    // acme@example.com normalises to acmeexamplecom, which would prefix-match owner acme
    expect(matchOwnership({ maintainers: ['acme@example.com'], repoOwner: 'acme' })).toBe(
      'no_evidence',
    )
    // @vercel is a handle, not an email — the leading @ is stripped by normalizeIdentity
    expect(matchOwnership({ maintainers: ['@vercel'], repoOwner: 'vercel' })).toBe('matched')
  })

  it('matches flat dotted namespaces like NuGet or Packagist vendors by all segments', () => {
    // Microsoft.Extensions — first segment is identity-bearing, not a TLD
    expect(matchOwnership({ namespace: 'Microsoft.Extensions', repoOwner: 'microsoft' })).toBe(
      'matched',
    )
    // Packagist vendor with dot — foo should be a candidate
    expect(matchOwnership({ namespace: 'foo.bar', repoOwner: 'foo' })).toBe('matched')
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

  it('returns null for host=other where first path segment is not an owner', () => {
    const repo = canonicalizeRepoUrl('https://git.sr.ht/~sircmpwn/aerc')
    expect(repo?.host).toBe('other')
    expect(repo && repoOwnerFromCanonical(repo)).toBeNull()
  })
})
