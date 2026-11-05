import { describe, expect, it } from 'vitest'

import {
  canonicalizeGithubRepoUrl,
  canonicalizeRepoUrl,
  extractGithubRepoRefs,
  extractGithubRepoUrls,
  extractGithubOrganizations,
  githubOrganizationOwner,
  githubRepoPath,
} from './githubRepoUrl'

describe('extractGithubOrganizations', () => {
  it('finds organization links in free text and leaves repositories and GitHub pages out', () => {
    expect(
      extractGithubOrganizations(
        'onboard OpenBMC, LF project, <https://github.com/openbmc/> and github.com/OpenBMC plus https://github.com/openbmc/bmcweb, (https://github.com/orgs/tikv), https://github.com/topics/sbom.',
      ),
    ).toEqual(['openbmc', 'tikv'])
    expect(
      extractGithubOrganizations(
        'onboard *<https://github.com/acme/>*, `github.com/beta` and (<https://github.com/orgs/gamma>).',
      ),
    ).toEqual(['acme', 'beta', 'gamma'])
    expect(extractGithubOrganizations('onboard https://github.com/acme/one')).toEqual([])
    expect(extractGithubOrganizations('onboard https://github.com/acme/_')).toEqual([])
    expect(extractGithubOrganizations('_onboard https://github.com/acme/_')).toEqual(['acme'])
    expect(extractGithubOrganizations('_onboard https://github.com/acme/_.')).toEqual(['acme'])
    expect(extractGithubOrganizations('_github.com/acme/_ and _github.com/beta_')).toEqual([
      'acme',
      'beta',
    ])
    expect(extractGithubOrganizations('_github.com/acme/_, and _github.com/beta_')).toEqual([
      'acme',
      'beta',
    ])
    expect(extractGithubOrganizations('onboard github.com/acme_ and _github.com/beta_')).toEqual([
      'acme',
      'beta',
    ])
    expect(extractGithubOrganizations('*onboard github.com/acme and (`github.com/beta`)*')).toEqual(
      ['acme', 'beta'],
    )
  })
})

describe('githubOrganizationOwner', () => {
  it('returns the owner of an organization or user page on GitHub', () => {
    expect(githubOrganizationOwner('https://github.com/openbmc/')).toBe('openbmc')
    expect(githubOrganizationOwner('github.com/openbmc')).toBe('openbmc')
    expect(githubOrganizationOwner('https://www.github.com/orgs/openbmc')).toBe('openbmc')
  })

  it('ignores repositories, reserved pages and other hosts', () => {
    expect(githubOrganizationOwner('https://github.com/openbmc/bmcweb')).toBeNull()
    for (const page of ['topics', 'explore', 'pricing', 'notifications', 'trending', 'issues']) {
      expect(githubOrganizationOwner(`https://github.com/${page}`)).toBeNull()
    }
    expect(githubOrganizationOwner('https://github.com/orgs')).toBeNull()
    expect(githubOrganizationOwner('https://gitlab.com/openbmc')).toBeNull()
    expect(githubOrganizationOwner('')).toBeNull()
  })
})

describe('canonicalizeRepoUrl', () => {
  it('canonicalizes a plain github.com URL', () => {
    expect(canonicalizeRepoUrl('https://github.com/torvalds/linux')).toEqual({
      url: 'https://github.com/torvalds/linux',
      host: 'github.com',
      isGithub: true,
      owner: 'torvalds',
      repo: 'linux',
    })
  })

  it('lowercases owner and repo on github.com', () => {
    const result = canonicalizeRepoUrl('https://github.com/Obmondo/kubeaid-cli')
    expect(result?.url).toBe('https://github.com/obmondo/kubeaid-cli')
    expect(result?.owner).toBe('obmondo')
    expect(result?.repo).toBe('kubeaid-cli')
  })

  it('reconciles the two production duplicates to the same canonical URL', () => {
    const a = canonicalizeRepoUrl('https://github.com/Obmondo/kubeaid-cli')
    const b = canonicalizeRepoUrl('https://github.com/obmondo/kubeaid-cli')
    expect(a?.url).toBe(b?.url)

    const c = canonicalizeRepoUrl('https://github.com/emirhan-duman/Crisis-Connect')
    const d = canonicalizeRepoUrl('https://github.com/emirhan-duman/crisis-connect')
    expect(c?.url).toBe(d?.url)
  })

  it('strips a trailing .git suffix', () => {
    expect(canonicalizeRepoUrl('https://github.com/torvalds/linux.git')?.url).toBe(
      'https://github.com/torvalds/linux',
    )
  })

  it('strips a trailing slash', () => {
    expect(canonicalizeRepoUrl('https://github.com/torvalds/linux/')?.url).toBe(
      'https://github.com/torvalds/linux',
    )
  })

  it('drops query string and hash fragment', () => {
    expect(
      canonicalizeRepoUrl('https://github.com/torvalds/linux?tab=readme-ov-file#L10')?.url,
    ).toBe('https://github.com/torvalds/linux')
  })

  it('rewrites git@github.com: SSH URLs', () => {
    expect(canonicalizeRepoUrl('git@github.com:torvalds/linux.git')?.url).toBe(
      'https://github.com/torvalds/linux',
    )
  })

  it('rewrites ssh://git@github.com/ URLs', () => {
    expect(canonicalizeRepoUrl('ssh://git@github.com/torvalds/linux.git')?.url).toBe(
      'https://github.com/torvalds/linux',
    )
  })

  it('rewrites scp-style ssh://git@github.com:<owner>/<repo> URLs', () => {
    expect(canonicalizeRepoUrl('ssh://git@github.com:torvalds/linux.git')?.url).toBe(
      'https://github.com/torvalds/linux',
    )
  })

  it('canonicalizes ssh://git@github.com:<port>/... URLs with an explicit port', () => {
    expect(canonicalizeRepoUrl('ssh://git@github.com:2222/torvalds/linux.git')?.url).toBe(
      'https://github.com/torvalds/linux',
    )
  })

  it('rewrites git@github.com: SSH URLs with a mixed-case host', () => {
    expect(canonicalizeRepoUrl('git@GitHub.com:Torvalds/Linux.git')?.url).toBe(
      'https://github.com/torvalds/linux',
    )
  })

  it('rewrites ssh://git@github.com/ URLs with a mixed-case host', () => {
    expect(canonicalizeRepoUrl('ssh://git@GITHUB.com/Torvalds/Linux')?.url).toBe(
      'https://github.com/torvalds/linux',
    )
  })

  it('adds a missing scheme', () => {
    expect(canonicalizeRepoUrl('github.com/torvalds/linux')?.url).toBe(
      'https://github.com/torvalds/linux',
    )
  })

  it('strips a leading www.', () => {
    expect(canonicalizeRepoUrl('https://www.github.com/torvalds/linux')?.url).toBe(
      'https://github.com/torvalds/linux',
    )
  })

  it('rejects a github.com URL with only an owner segment', () => {
    expect(canonicalizeRepoUrl('https://github.com/torvalds')).toBeNull()
  })

  it('canonicalizes a deep link (/tree/<branch>) to the repo root', () => {
    expect(canonicalizeRepoUrl('https://github.com/torvalds/linux/tree/main')?.url).toBe(
      'https://github.com/torvalds/linux',
    )
  })

  it('canonicalizes a deep link (/blob/<branch>/<path>) to the repo root', () => {
    expect(canonicalizeRepoUrl('https://github.com/torvalds/linux/blob/main/README')?.url).toBe(
      'https://github.com/torvalds/linux',
    )
  })

  it('strips a .git suffix on the repo segment of a deep link', () => {
    expect(canonicalizeRepoUrl('https://github.com/torvalds/linux.git/tree/main')?.url).toBe(
      'https://github.com/torvalds/linux',
    )
  })

  it('rejects reserved GitHub product owners', () => {
    expect(canonicalizeRepoUrl('https://github.com/user-attachments/assets/foo.png')).toBeNull()
    expect(canonicalizeRepoUrl('https://github.com/orgs/linuxfoundation/people')).toBeNull()
    expect(canonicalizeRepoUrl('https://github.com/marketplace/some-app')).toBeNull()
    expect(canonicalizeRepoUrl('https://github.com/features/actions')).toBeNull()
    expect(canonicalizeRepoUrl('https://github.com/search/advanced')).toBeNull()
  })

  it('rejects empty, null, or unparseable input', () => {
    expect(canonicalizeRepoUrl('')).toBeNull()
    expect(canonicalizeRepoUrl(null)).toBeNull()
    expect(canonicalizeRepoUrl(undefined)).toBeNull()
    expect(canonicalizeRepoUrl('   ')).toBeNull()
  })

  it('keeps a non-GitHub host with isGithub: false and no owner/repo', () => {
    expect(canonicalizeRepoUrl('https://gcc.gnu.org/git/gcc.git')).toEqual({
      url: 'https://gcc.gnu.org/git/gcc',
      host: 'gcc.gnu.org',
      isGithub: false,
      owner: null,
      repo: null,
    })
  })

  it('preserves path case for non-GitHub hosts (case-sensitive upstreams)', () => {
    expect(canonicalizeRepoUrl('https://gitlab.com/GNOME/Glib')?.url).toBe(
      'https://gitlab.com/GNOME/Glib',
    )
  })

  it('lowercases only the host for non-GitHub hosts', () => {
    expect(canonicalizeRepoUrl('https://GCC.GNU.ORG/git/gcc')?.host).toBe('gcc.gnu.org')
  })

  it('preserves an explicit port for non-GitHub hosts', () => {
    expect(canonicalizeRepoUrl('https://git.example.com:8443/Owner/Repo')?.url).toBe(
      'https://git.example.com:8443/Owner/Repo',
    )
  })

  it('rewrites a scp-style git@ SSH URL for a non-GitHub host', () => {
    expect(canonicalizeRepoUrl('git@gitlab.com:GNOME/Glib.git')?.url).toBe(
      'https://gitlab.com/GNOME/Glib',
    )
  })

  it('does not merge a www.-prefixed non-GitHub host with its bare counterpart', () => {
    expect(canonicalizeRepoUrl('https://www.git.example.com/Owner/Repo')?.host).toBe(
      'www.git.example.com',
    )
  })
})

describe('canonicalizeGithubRepoUrl', () => {
  it('returns the canonical URL for a github.com repo', () => {
    expect(canonicalizeGithubRepoUrl('https://github.com/Torvalds/Linux.git')).toBe(
      'https://github.com/torvalds/linux',
    )
  })

  it('returns null for a non-GitHub host', () => {
    expect(canonicalizeGithubRepoUrl('https://gitlab.com/GNOME/Glib')).toBeNull()
  })

  it('returns null for invalid input', () => {
    expect(canonicalizeGithubRepoUrl('not a url')).toBeNull()
  })
})

describe('githubRepoPath', () => {
  it('returns owner/repo for a github.com URL', () => {
    expect(githubRepoPath('https://github.com/Torvalds/Linux')).toBe('torvalds/linux')
  })

  it('returns null for a non-GitHub host', () => {
    expect(githubRepoPath('https://gcc.gnu.org/git/gcc')).toBeNull()
  })
})

describe('extractGithubRepoUrls', () => {
  it('extracts a plain https URL', () => {
    expect(extractGithubRepoUrls('See https://github.com/foo/bar for details')).toEqual([
      'https://github.com/foo/bar',
    ])
  })

  it('extracts a plain http URL', () => {
    expect(extractGithubRepoUrls('http://github.com/foo/bar')).toEqual([
      'https://github.com/foo/bar',
    ])
  })

  it('extracts a www. URL', () => {
    expect(extractGithubRepoUrls('https://www.github.com/foo/bar')).toEqual([
      'https://github.com/foo/bar',
    ])
  })

  it('extracts a URL without a scheme', () => {
    expect(extractGithubRepoUrls('please onboard github.com/agentnameservice/ans')).toEqual([
      'https://github.com/agentnameservice/ans',
    ])
  })

  it('strips a .git suffix', () => {
    expect(extractGithubRepoUrls('https://github.com/foo/bar.git')).toEqual([
      'https://github.com/foo/bar',
    ])
  })

  it('strips trailing sentence punctuation', () => {
    expect(extractGithubRepoUrls('Please onboard https://github.com/foo/bar.')).toEqual([
      'https://github.com/foo/bar',
    ])
  })

  it('extracts a URL from a markdown link', () => {
    expect(extractGithubRepoUrls('[our repo](https://github.com/foo/bar)')).toEqual([
      'https://github.com/foo/bar',
    ])
  })

  it.each([
    ['https://github.com/foo/bar/tree/main/src', 'https://github.com/foo/bar'],
    ['https://github.com/foo/bar/blob/main/README.md', 'https://github.com/foo/bar'],
    ['https://github.com/foo/bar/pull/42', 'https://github.com/foo/bar'],
    ['https://github.com/foo/bar/issues', 'https://github.com/foo/bar'],
    ['https://github.com/foo/bar/discussions', 'https://github.com/foo/bar'],
  ])('collapses deep link %s to the repo root', (input, expected) => {
    expect(extractGithubRepoUrls(input)).toEqual([expected])
  })

  it('rejects reserved GitHub owners', () => {
    expect(extractGithubRepoUrls('https://github.com/orgs/foo')).toEqual([])
    expect(extractGithubRepoUrls('https://github.com/marketplace/foo')).toEqual([])
  })

  it('rejects an owner-only URL', () => {
    expect(extractGithubRepoUrls('https://github.com/gardenlinux')).toEqual([])
  })

  it('ignores lookalike hosts', () => {
    expect(extractGithubRepoUrls('https://gist.github.com/foo/bar')).toEqual([])
    expect(extractGithubRepoUrls('https://mygithub.com/foo/bar')).toEqual([])
  })

  it('ignores github.com appearing inside the path of another URL', () => {
    expect(extractGithubRepoUrls('https://example.com/github.com/foo/bar')).toEqual([])
    expect(
      extractGithubRepoUrls('see https://example.com/github.com/foo/bar and github.com/acme/one'),
    ).toEqual(['https://github.com/acme/one'])
  })

  it('strips long runs of trailing punctuation', () => {
    expect(extractGithubRepoUrls(`github.com/foo/bar${'!'.repeat(5000)}`)).toEqual([
      'https://github.com/foo/bar',
    ])
  })

  it('folds case', () => {
    expect(extractGithubRepoUrls('https://github.com/Foo/BAR')).toEqual([
      'https://github.com/foo/bar',
    ])
  })

  it('dedupes repeated references, preserving first-seen order', () => {
    expect(
      extractGithubRepoUrls(
        'https://github.com/foo/bar and again https://github.com/foo/bar, also https://github.com/baz/qux',
      ),
    ).toEqual(['https://github.com/foo/bar', 'https://github.com/baz/qux'])
  })
})

describe('extractGithubRepoRefs', () => {
  it('keeps every occurrence and flags links to a folder or file inside the repository', () => {
    expect(
      extractGithubRepoRefs(
        'https://github.com/foo/bar/tree/main/docs, github.com/foo/bar and https://github.com/foo/bar/',
      ),
    ).toEqual([
      { url: 'https://github.com/foo/bar', deepLink: true },
      { url: 'https://github.com/foo/bar', deepLink: false },
      { url: 'https://github.com/foo/bar', deepLink: false },
    ])
  })
})
