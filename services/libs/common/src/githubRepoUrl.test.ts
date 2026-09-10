import { describe, expect, it } from 'vitest'

import { canonicalizeGithubRepoUrl, canonicalizeRepoUrl, githubRepoPath } from './githubRepoUrl'

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

  it('keeps ssh://git@github.com:<port>/... URLs with an explicit port intact', () => {
    expect(canonicalizeRepoUrl('ssh://git@github.com:2222/torvalds/linux.git')?.url).toBe(
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
