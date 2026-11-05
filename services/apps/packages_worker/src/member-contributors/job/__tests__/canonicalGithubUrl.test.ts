import { describe, expect, it } from 'vitest'

import { canonicalGithubUrl } from '../canonicalGithubUrl'

const aeson = {
  kind: 'github',
  url: 'https://github.com/haskell/aeson',
  owner: 'haskell',
  name: 'aeson',
}

describe('canonicalGithubUrl', () => {
  it.each([
    ['https://github.com/haskell/aeson', aeson],
    ['https://github.com/Haskell/Aeson.git', aeson],
    ['http://www.github.com/haskell/aeson/', aeson],
    ['https://github.com/haskell/aeson/tree/main/src', aeson],
    ['github.com/haskell/aeson', aeson],
    ['https://gitlab.com/a/b', { kind: 'other', host: 'gitlab.com' }],
    ['https://github.com/haskell', { kind: 'invalid' }],
    ['not a url', { kind: 'invalid' }],
    ['https://github.com/', { kind: 'invalid' }],
  ])('classifies %s', (raw, expected) => {
    expect(canonicalGithubUrl(raw)).toEqual(expected)
  })
})
