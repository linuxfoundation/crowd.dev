import { describe, expect, it } from 'vitest'

import { toRepoRow } from '../activities'

describe('toRepoRow', () => {
  it('converts a canonical GitHub URL', () => {
    expect(toRepoRow('https://github.com/torvalds/linux')).toEqual({
      url: 'https://github.com/torvalds/linux',
      host: 'github',
      owner: 'torvalds',
      name: 'linux',
    })
  })

  it('lowercases GitHub URLs', () => {
    const row = toRepoRow('https://github.com/Owner/Repo')
    expect(row?.url).toBe('https://github.com/owner/repo')
    expect(row?.owner).toBe('owner')
    expect(row?.name).toBe('repo')
  })

  it('strips .git suffix', () => {
    const row = toRepoRow('https://github.com/owner/repo.git')
    expect(row?.url).toBe('https://github.com/owner/repo')
  })

  it('strips trailing slash', () => {
    const row = toRepoRow('https://github.com/owner/repo/')
    expect(row?.url).toBe('https://github.com/owner/repo')
  })

  it('returns null for non-GitHub URL', () => {
    expect(toRepoRow('https://gitlab.com/owner/repo')).toBeNull()
  })

  it('returns null for URL with no repo segment', () => {
    expect(toRepoRow('https://github.com/owner')).toBeNull()
  })

  it('returns null for empty string', () => {
    expect(toRepoRow('')).toBeNull()
  })

  it('returns null for invalid URL', () => {
    expect(toRepoRow('not-a-url')).toBeNull()
  })
})
