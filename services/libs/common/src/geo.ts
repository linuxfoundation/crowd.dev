import { IGeo } from '@crowd/types'

import { getCountryName, normalizeCountryCode, toCountryCode } from './country'

function toText(value: unknown): string | undefined {
  return typeof value === 'string' && value.trim() ? value.trim() : undefined
}

// A given country decides the code; region and city are kept only when geo agrees,
// so a stored geo never overrides a newer country from the same source.
export function toGeo(country: unknown, geo?: unknown): IGeo | undefined {
  const details = (geo && typeof geo === 'object' ? geo : {}) as Partial<
    Record<keyof IGeo, unknown>
  >
  const geoCountryCode = toCountryCode(details.countryCode)
  const countryCode = normalizeCountryCode(country) ?? geoCountryCode
  if (!countryCode) {
    return undefined
  }

  if (geoCountryCode && geoCountryCode !== countryCode) {
    return { countryCode }
  }

  const region = toText(details.region)
  const city = toText(details.city)
  return { countryCode, ...(region && { region }), ...(city && { city }) }
}

export function normalizeGeoAttributes(
  country: Record<string, unknown> = {},
  geo: Record<string, unknown> = {},
): { country: Record<string, string>; geo: Record<string, IGeo> } {
  const normalized = { country: {}, geo: {} }

  for (const source of new Set([...Object.keys(country), ...Object.keys(geo)])) {
    const value = toGeo(country[source], geo[source])
    if (value) {
      normalized.geo[source] = value
      normalized.country[source] = getCountryName(value.countryCode)
    }
  }

  return normalized
}
