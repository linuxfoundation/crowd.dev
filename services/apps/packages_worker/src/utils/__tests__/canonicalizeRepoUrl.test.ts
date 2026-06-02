import { describe, expect, it } from 'vitest'

import { canonicalizeRepoUrl } from '../canonicalizeRepoUrl'

describe('canonicalizeRepoUrl', () => {
  it.each([
    // [input, expected]
    ['git+https://github.com/babel/babel.git', 'https://github.com/babel/babel'],
    [
      'https://github.com/babel/babel/tree/master/packages/babel-core',
      'https://github.com/babel/babel',
    ],
    ['git@github.com:facebook/react.git', 'https://github.com/facebook/react'],
    ['github:sindresorhus/got', 'https://github.com/sindresorhus/got'],
    ['sindresorhus/got', 'https://github.com/sindresorhus/got'],
    ['ssh://git@github.com/foo/bar.git', 'https://github.com/foo/bar'],
    ['git://github.com/foo/bar', 'https://github.com/foo/bar'],
    ['https://www.gitlab.com/foo/bar', 'https://gitlab.com/foo/bar'],
    ['https://gitlab.com/foo/bar', 'https://gitlab.com/foo/bar'],
    ['https://github.com/babel/babel#readme', 'https://github.com/babel/babel'],
    ['gitlab:group/project', 'https://gitlab.com/group/project'],
    ['bitbucket:team/repo', 'https://bitbucket.org/team/repo'],
  ])('canonicalizes %s', (input, expected) => {
    expect(canonicalizeRepoUrl(input)).toBe(expected)
  })

  it('lowercases GitHub/GitLab owner and name (case-insensitive hosts)', () => {
    expect(canonicalizeRepoUrl('https://github.com/Babel/Babel')).toBe(
      'https://github.com/babel/babel',
    )
    expect(canonicalizeRepoUrl('https://GitLab.com/Foo/Bar')).toBe('https://gitlab.com/foo/bar')
  })

  it('preserves path case for other (case-sensitive) hosts', () => {
    expect(canonicalizeRepoUrl('https://example.com/Foo/Bar')).toBe('https://example.com/Foo/Bar')
  })

  it.each([['not a url'], ['https://github.com/onlyowner'], [''], ['   ']])(
    'returns null for unparseable input %s',
    (input) => {
      expect(canonicalizeRepoUrl(input)).toBeNull()
    },
  )
})
