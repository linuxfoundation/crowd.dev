import { describe, expect, it } from 'vitest'

import { omitUrlValues, setAttributesDefaultValues } from './member'

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

describe('setAttributesDefaultValues', () => {
  it('picks the default country and geo from the same source', async () => {
    const attributes = await setAttributesDefaultValues(
      {
        country: { github: 'Germany', enrichment: 'France' },
        geo: { enrichment: { countryCode: 'FR', region: 'Île-de-France', city: 'Paris' } },
      },
      ['enrichment', 'github'],
    )

    expect(attributes).toEqual({
      country: { github: 'Germany', enrichment: 'France', default: 'France' },
      geo: {
        github: { countryCode: 'DE' },
        enrichment: { countryCode: 'FR', region: 'Île-de-France', city: 'Paris' },
        default: { countryCode: 'FR', region: 'Île-de-France', city: 'Paris' },
      },
    })
  })

  it('replaces a stale geo when the same source sends a new country', async () => {
    const attributes = await setAttributesDefaultValues(
      {
        country: { enrichment: 'Spain', default: 'Germany' },
        geo: {
          enrichment: { countryCode: 'DE', city: 'Berlin' },
          default: { countryCode: 'DE', city: 'Berlin' },
        },
      },
      ['enrichment'],
    )

    expect(attributes.geo).toEqual({
      enrichment: { countryCode: 'ES' },
      default: { countryCode: 'ES' },
    })
  })

  it('drops geo sources that a replaced country no longer has', async () => {
    const attributes = await setAttributesDefaultValues(
      {
        country: { default: 'France' },
        geo: { github: { countryCode: 'DE' }, default: { countryCode: 'DE' } },
      },
      ['github'],
    )

    expect(attributes).toEqual({
      country: { default: 'France' },
      geo: { default: { countryCode: 'FR' } },
    })
  })
})
