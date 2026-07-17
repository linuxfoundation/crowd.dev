import { describe, expect, it } from 'vitest'

import { canonicalizeRepoUrl } from '../canonicalizeRepoUrl'

describe('canonicalizeRepoUrl', () => {
  it.each([
    // [input, expected url, expected host]
    ['git+https://github.com/babel/babel.git', 'https://github.com/babel/babel', 'github'],
    [
      'https://github.com/babel/babel/tree/master/packages/babel-core',
      'https://github.com/babel/babel',
      'github',
    ],
    ['git@github.com:facebook/react.git', 'https://github.com/facebook/react', 'github'],
    ['github:sindresorhus/got', 'https://github.com/sindresorhus/got', 'github'],
    ['sindresorhus/got', 'https://github.com/sindresorhus/got', 'github'],
    ['ssh://git@github.com/foo/bar.git', 'https://github.com/foo/bar', 'github'],
    ['git://github.com/foo/bar', 'https://github.com/foo/bar', 'github'],
    ['https://www.gitlab.com/foo/bar', 'https://gitlab.com/foo/bar', 'gitlab'],
    ['https://gitlab.com/foo/bar', 'https://gitlab.com/foo/bar', 'gitlab'],
    ['https://github.com/babel/babel#readme', 'https://github.com/babel/babel', 'github'],
    ['gitlab:group/project', 'https://gitlab.com/group/project', 'gitlab'],
    ['bitbucket:team/repo', 'https://bitbucket.org/team/repo', 'bitbucket'],
    ['https://example.com/owner/repo', 'https://example.com/owner/repo', 'other'],
    ['git+https://github.com/1aGh/md-claude.git', 'https://github.com/1agh/md-claude', 'github'],
    [
      'ssh://git@github.com:1inch/limit-order-protocol-utils.git',
      'https://github.com/1inch/limit-order-protocol-utils',
      'github',
    ],
    ['ssh://git@github.com:2222/foo/bar.git', 'https://github.com/foo/bar', 'github'],
    [
      'https://gitlab.com/group/subgroup/project',
      'https://gitlab.com/group/subgroup/project',
      'gitlab',
    ],
    [
      'https://gitlab.com/group/subgroup/subsubgroup/project.git',
      'https://gitlab.com/group/subgroup/subsubgroup/project',
      'gitlab',
    ],
    [
      'https://gitlab.com/group/project/-/tree/master/src',
      'https://gitlab.com/group/project',
      'gitlab',
    ],
    [
      'https://gitlab.com/group/subgroup/project/-/blob/main/README.md',
      'https://gitlab.com/group/subgroup/project',
      'gitlab',
    ],
  ])('canonicalizes %s', (input, expectedUrl, expectedHost) => {
    expect(canonicalizeRepoUrl(input)).toEqual({ url: expectedUrl, host: expectedHost })
  })

  it('lowercases GitHub/GitLab owner and name (case-insensitive hosts)', () => {
    expect(canonicalizeRepoUrl('https://github.com/Babel/Babel')).toEqual({
      url: 'https://github.com/babel/babel',
      host: 'github',
    })
    expect(canonicalizeRepoUrl('https://GitLab.com/Foo/Bar')).toEqual({
      url: 'https://gitlab.com/foo/bar',
      host: 'gitlab',
    })
  })

  it('preserves path case for other (case-sensitive) hosts', () => {
    expect(canonicalizeRepoUrl('https://example.com/Foo/Bar')).toEqual({
      url: 'https://example.com/Foo/Bar',
      host: 'other',
    })
  })

  it.each([
    ['not a url'],
    ['https://github.com/onlyowner'],
    [''],
    ['   '],
    ['123'],
    ['https://github.com/Wscats'],
  ])('returns null for unparseable input %s', (input) => {
    expect(canonicalizeRepoUrl(input)).toBeNull()
  })
})
