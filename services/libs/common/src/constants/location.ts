/**
 * US state / district names (not postal codes).
 * Excludes georgia and washington — too easy to confuse with other places.
 */
export const US_STATE_NAMES = new Set([
  'alabama',
  'alaska',
  'arizona',
  'arkansas',
  'california',
  'colorado',
  'connecticut',
  'delaware',
  'florida',
  'hawaii',
  'idaho',
  'illinois',
  'indiana',
  'iowa',
  'kansas',
  'kentucky',
  'louisiana',
  'maine',
  'maryland',
  'massachusetts',
  'michigan',
  'minnesota',
  'mississippi',
  'missouri',
  'montana',
  'nebraska',
  'nevada',
  'new hampshire',
  'new jersey',
  'new mexico',
  'new york',
  'north carolina',
  'north dakota',
  'ohio',
  'oklahoma',
  'oregon',
  'pennsylvania',
  'rhode island',
  'south carolina',
  'south dakota',
  'tennessee',
  'texas',
  'utah',
  'vermont',
  'virginia',
  'west virginia',
  'wisconsin',
  'wyoming',
  'district of columbia',
])

/** Common alternate spellings → a name the country package recognizes. */
export const COUNTRY_NAME_INPUT_ALIASES = new Map<string, string>([
  ['brasil', 'Brazil'],
  ['turkiye', 'Türkiye'],
  ['viet nam', 'Vietnam'],
  ['korea', 'South Korea'],
  ['england', 'United Kingdom'],
  ['scotland', 'United Kingdom'],
  ['wales', 'United Kingdom'],
])

/** Tokens that should never resolve to a country. */
export const JUNK_LOCATION_TOKENS = new Set([
  'earth',
  'mars',
  'moon',
  'remote',
  'worldwide',
  'world',
  'global',
  'internet',
  'somewhere',
  'nowhere',
  'unknown',
  'n/a',
  'na',
  'null',
  'undefined',
  'home',
  'here',
  'africa',
  'europe',
  'asia',
  'antarctica',
])

/** Names that could mean more than one place — skip instead of guessing. */
export const AMBIGUOUS_LOCATION_TOKENS = new Set(['georgia', 'washington'])

/** Preferred display names for a few ISO codes with awkward official forms. */
export const COUNTRY_DISPLAY_NAME_BY_ALPHA2 = new Map<string, string>([
  ['US', 'United States'],
  ['GB', 'United Kingdom'],
  ['CN', 'China'],
  ['TW', 'Taiwan'],
  ['RU', 'Russia'],
  ['TR', 'Turkey'],
  ['KR', 'South Korea'],
  ['NL', 'Netherlands'],
  ['CZ', 'Czech Republic'],
  ['AE', 'United Arab Emirates'],
])
