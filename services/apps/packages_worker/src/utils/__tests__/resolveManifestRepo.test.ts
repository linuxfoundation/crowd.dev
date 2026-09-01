import { describe, expect, it } from 'vitest'

import { resolveManifestRepo } from '../resolveManifestRepo'

describe('resolveManifestRepo', () => {
  it('resolves the first candidate as primary', () => {
    expect(
      resolveManifestRepo([
        { field: 'repository', url: 'git+https://github.com/babel/babel.git' },
        { field: 'homepage', url: 'https://github.com/other/repo' },
      ]),
    ).toEqual({
      repo: { url: 'https://github.com/babel/babel', host: 'github' },
      signal: 'primary',
      field: 'repository',
    })
  })

  it('falls through to the next field when the primary one is missing', () => {
    expect(
      resolveManifestRepo([
        { field: 'repository', url: null },
        { field: 'homepage', url: 'https://github.com/foo/bar' },
      ]),
    ).toEqual({
      repo: { url: 'https://github.com/foo/bar', host: 'github' },
      signal: 'secondary',
      field: 'homepage',
    })
  })

  it('falls through when the primary field cannot be canonicalized', () => {
    const resolved = resolveManifestRepo([
      { field: 'repository', url: 'not a url' },
      { field: 'bugs.url', url: 'https://gitlab.com/group/sub/project/-/issues' },
    ])
    expect(resolved?.repo.url).toBe('https://gitlab.com/group/sub/project')
    expect(resolved?.signal).toBe('secondary')
  })

  it('rejects a secondary candidate that is not on a recognized VCS host', () => {
    expect(
      resolveManifestRepo([
        { field: 'repository', url: null },
        { field: 'homepage', url: 'https://example.com/docs/getting-started' },
      ]),
    ).toBeNull()
  })

  it('keeps a primary candidate on an unrecognized host', () => {
    const resolved = resolveManifestRepo([
      { field: 'repository', url: 'https://git.sr.ht/~sircmpwn/aerc' },
    ])
    expect(resolved?.repo.host).toBe('other')
    expect(resolved?.signal).toBe('primary')
  })

  it('honours an explicit signal override', () => {
    expect(
      resolveManifestRepo([
        { field: 'homepage', url: 'https://github.com/foo/bar', signal: 'secondary' },
      ])?.signal,
    ).toBe('secondary')
  })

  it('returns null when no candidate resolves', () => {
    expect(resolveManifestRepo([{ field: 'repository', url: '   ' }])).toBeNull()
  })
})
