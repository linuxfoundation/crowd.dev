import { ICountryCandidate, IMemberCountryHints } from '@crowd/data-access-layer'

export function buildLocationsPrompt(locations: string[]): string {
  return `You map free-text profile locations, mostly from GitHub, to countries.

Answer one of these for each location:
- "resolved": one country is the clear reading. Cities, regions, US states, abbreviations, addresses, misspellings and any language or script count. A major city resolves to its country even if a smaller namesake exists elsewhere ("paris" is FR, "london" is GB, "austin, tx" is US).
- "ambiguous": two or more countries are similarly likely ("georgia", "ca", "cambridge", "perth").
- "unknown": not a real place ("earth", "remote", "localhost", "europe", "worldwide"), or you cannot tell the country.

Never infer a country from a person's name.

Reply with only a JSON array with one object per location:
{"i": <index>, "s": "resolved" | "ambiguous" | "unknown", "c": "<ISO 3166-1 alpha-2 code>" or null}

Locations as [index, location] pairs:
${JSON.stringify(locations.map((location, index) => [index, location]))}`
}

export function buildHintsPrompt(members: (ICountryCandidate & IMemberCountryHints)[]): string {
  const inputs = members.map(({ location, orgCountries, emailDomains }, index) => [
    index,
    { location, orgCountries, emailDomains },
  ])

  return `Each input is a person's free-text profile location that could mean more than one country, with hints: the countries of the organizations they work at now and the domains of their verified emails.

First list the countries the location itself could mean. Then use the hints to pick one of them. A hint only chooses between those countries: if it points to a country the location can't mean, ignore it. Generic email domains like gmail.com or .io say nothing. If the hints don't settle it, answer null. Never infer a country from a person's name.

Reply with only a JSON array with one object per input:
{"i": <index>, "o": [<ISO 3166-1 alpha-2 codes the location could mean>], "c": "<one of o>" or null}

Inputs as [index, input] pairs:
${JSON.stringify(inputs)}`
}
