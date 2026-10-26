import { describe, expect, it } from 'vitest'

import { omitUrlValues } from './member'

describe('omitUrlValues', () => {
  it('drops values that are URLs, including leading whitespace and any casing', () => {
    expect(
      omitUrlValues({
        github: 'https://api.github.com/users/octocat',
        custom: ' HTTP://maps.app.goo.gl/x',
        default: 'https://api.github.com/users/octocat',
        enrichment: 'Glasgow, Scotland',
      }),
    ).toEqual({ enrichment: 'Glasgow, Scotland' })
  })

  it('keeps places that only look like domains or mention a URL', () => {
    const location = { github: 'beijing.cn', twitter: 'Paris https://example.com' }
    expect(omitUrlValues(location)).toEqual(location)
  })

  it('returns an empty object when there is no attribute', () => {
    expect(omitUrlValues(undefined)).toEqual({})
  })
})
