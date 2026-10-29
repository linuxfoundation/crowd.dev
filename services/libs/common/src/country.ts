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

const TRAILING_PUNCTUATION = new Set(['.', '!', '?'])

function normalizeToken(value: string): string {
  const token = normalizeWhitespace(value)

  let end = token.length
  while (end > 0 && TRAILING_PUNCTUATION.has(token[end - 1])) {
    end--
  }

  return token.slice(0, end).toLowerCase()
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

const ALPHA2_BY_DISPLAY_NAME = new Map(
  [...COUNTRY_DISPLAY_NAME_BY_ALPHA2].map(([alpha2, name]) => [name.toLowerCase(), alpha2]),
)

function toAlpha2(token: string): string | undefined {
  const aliased = COUNTRY_NAME_INPUT_ALIASES.get(token) || token

  // Name/alias lookup only — getAlpha2Code does not treat CA/IN/DE as countries.
  return ALPHA2_BY_DISPLAY_NAME.get(aliased.toLowerCase()) ?? countries.getAlpha2Code(aliased, 'en')
}

function parseCountryToken(token: string): string | undefined {
  if (!token || JUNK_LOCATION_TOKENS.has(token) || AMBIGUOUS_LOCATION_TOKENS.has(token)) {
    return undefined
  }

  if (US_STATE_NAMES.has(token)) {
    return 'United States'
  }

  const alpha2 = toAlpha2(token)
  return alpha2 ? toDisplayName(alpha2) : undefined
}

export function normalizeLocation(location: string): string {
  return normalizeWhitespace(location.normalize('NFKC')).toLowerCase()
}

export function getCountryName(alpha2: string): string | undefined {
  const code = alpha2.toUpperCase()
  return countries.isValid(code) ? toDisplayName(code) : undefined
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

/**
 * Resolves any spelling of a country (e.g. "Korea, Republic of") to its standard name.
 * Returns undefined when the value is not a country.
 */
export function normalizeCountryName(value: unknown): string | undefined {
  if (typeof value !== 'string') {
    return undefined
  }

  const alpha2 = toAlpha2(normalizeToken(value))
  return alpha2 ? toDisplayName(alpha2) : getCountry(value)
}

export function normalizeCountryAttribute(
  attribute: Record<string, unknown>,
): Record<string, string> {
  return Object.fromEntries(
    Object.entries(attribute)
      .map(([source, value]) => [source, normalizeCountryName(value)])
      .filter(([, country]) => country),
  )
}
