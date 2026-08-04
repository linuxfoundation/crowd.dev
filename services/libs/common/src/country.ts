import countries from 'i18n-iso-countries'

import {
  AMBIGUOUS_LOCATION_TOKENS,
  COUNTRY_DISPLAY_NAME_BY_ALPHA2,
  COUNTRY_NAME_INPUT_ALIASES,
  JUNK_LOCATION_TOKENS,
  US_STATE_NAMES,
} from './constants'

function normalizeWhitespace(value: string): string {
  return value.replace(/\s+/g, ' ').trim()
}

function normalizeToken(value: string): string {
  return normalizeWhitespace(value)
    .replace(/[.!?]+$/, '')
    .toLowerCase()
}

function toDisplayName(alpha2: string): string | undefined {
  const override = COUNTRY_DISPLAY_NAME_BY_ALPHA2.get(alpha2)
  if (override) {
    return override
  }

  const official = countries.getName(alpha2, 'en', { select: 'official' })
  const alias = countries.getName(alpha2, 'en', { select: 'alias' })

  // Prefer plain aliases (e.g. China) over awkward official forms; skip short ones like "UK".
  if (alias && alias.length >= 4 && !alias.includes(',') && !alias.includes('.')) {
    return alias
  }

  return official || undefined
}

function parseCountryToken(token: string): string | undefined {
  if (!token || JUNK_LOCATION_TOKENS.has(token) || AMBIGUOUS_LOCATION_TOKENS.has(token)) {
    return undefined
  }

  if (US_STATE_NAMES.has(token)) {
    return 'United States'
  }

  const aliased = COUNTRY_NAME_INPUT_ALIASES.get(token) || token

  // Name/alias lookup only — getAlpha2Code does not treat CA/IN/DE as countries.
  const alpha2 = countries.getAlpha2Code(aliased, 'en')
  if (!alpha2) {
    return undefined
  }

  return toDisplayName(alpha2)
}

/**
 * Resolves a country name from a location string.
 */
export function getCountry(location: string | null | undefined): string | undefined {
  if (!location) {
    return undefined
  }

  const normalized = normalizeWhitespace(location)
  if (!normalized || JUNK_LOCATION_TOKENS.has(normalized.toLowerCase())) {
    return undefined
  }

  // Prefer later segments (…, Country) but also accept Country, City.
  const parts = normalized
    .replaceAll(' - ', ',')
    .replaceAll('/', ',')
    .replaceAll('|', ',')
    .split(',')
    .map((part) => normalizeToken(part))
    .filter(Boolean)

  for (let i = parts.length - 1; i >= 0; i--) {
    const country = parseCountryToken(parts[i])
    if (country) {
      return country
    }
  }

  return undefined
}
