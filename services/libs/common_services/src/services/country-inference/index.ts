import {
  getCountry,
  getCountryName,
  normalizeLocation,
  parseLlmJson,
  partition,
} from '@crowd/common'
import {
  ICountryCandidate,
  ICountryUpdate,
  ILocationCountry,
  IMemberCountryHints,
  QueryExecutor,
  findLocationCountries,
  findMemberCountryHints,
  insertLocationCountries,
} from '@crowd/data-access-layer'
import { LlmQueryType, OrganizationAttributeSource } from '@crowd/types'

import { LlmService } from '../llm.service'
import { buildHintsPrompt, buildLocationsPrompt } from './prompts'

const LOCATIONS_PER_PROMPT = 100
const MEMBERS_PER_PROMPT = 50
const PARSER_SOURCE = OrganizationAttributeSource.SYSTEM
const LLM_SOURCE = OrganizationAttributeSource.LLM

async function askLlm<T>(llm: LlmService, prompt: string): Promise<T[]> {
  const { answer } = await llm.queryLlm(LlmQueryType.LOCATION_COUNTRY, prompt)
  const answers = parseLlmJson<T[]>(answer)
  if (!Array.isArray(answers)) {
    throw new Error('LLM answer is not a JSON array!')
  }
  return answers
}

async function askLlmForLocations(
  llm: LlmService,
  locations: string[],
): Promise<ILocationCountry[]> {
  if (locations.length === 0) {
    return []
  }

  const answers = await askLlm<{ i: number; s: string; c: string | null }>(
    llm,
    buildLocationsPrompt(locations),
  )

  return answers
    .filter(({ i }) => locations[i] !== undefined)
    .map(({ i, s, c }) => {
      const location = locations[i]
      if (s === 'resolved' && c && getCountryName(c)) {
        return { location, status: s, countryCode: c.toUpperCase() }
      }
      return { location, status: s === 'ambiguous' ? s : 'unknown', countryCode: null }
    })
}

async function askLlmWithHints(
  llm: LlmService,
  members: (ICountryCandidate & IMemberCountryHints)[],
): Promise<ICountryUpdate[]> {
  const answers = await askLlm<{ i: number; o: string[]; c: string | null }>(
    llm,
    buildHintsPrompt(members),
  )

  return answers
    .filter(({ i, o, c }) => members[i] && c && o?.includes(c) && getCountryName(c))
    .map(({ i, c }) => ({ id: members[i].id, source: LLM_SOURCE, country: getCountryName(c) }))
}

async function lookUpLocations(
  qx: QueryExecutor,
  llm: LlmService,
  locations: string[],
): Promise<Map<string, ILocationCountry>> {
  const known = await findLocationCountries(qx, locations)
  const knownLocations = new Set(known.map((row) => row.location))
  const missing = locations.filter((location) => !knownLocations.has(location))

  const batches = partition(missing, LOCATIONS_PER_PROMPT)
  const answered = (await Promise.all(batches.map((b) => askLlmForLocations(llm, b)))).flat()
  await insertLocationCountries(qx, answered)

  return new Map([...known, ...answered].map((row) => [row.location, row]))
}

async function resolveWithHints(
  qx: QueryExecutor,
  llm: LlmService,
  members: ICountryCandidate[],
): Promise<ICountryUpdate[]> {
  const hints = await findMemberCountryHints(
    qx,
    members.map((m) => m.id),
  )
  const withHints = members
    .filter((m) => hints.has(m.id))
    .map((m) => ({ ...m, ...hints.get(m.id) }))

  if (withHints.length === 0) {
    return []
  }

  const batches = partition(withHints, MEMBERS_PER_PROMPT)
  return (await Promise.all(batches.map((b) => askLlmWithHints(llm, b)))).flat()
}

export async function inferCountries(
  qx: QueryExecutor,
  llm: LlmService,
  candidates: ICountryCandidate[],
  { withHints }: { withHints: boolean },
): Promise<ICountryUpdate[]> {
  const updates: ICountryUpdate[] = []
  const unparsed: ICountryCandidate[] = []

  for (const { id, location } of candidates) {
    const country = getCountry(location)
    if (country) {
      updates.push({ id, source: PARSER_SOURCE, country })
    } else {
      unparsed.push({ id, location: normalizeLocation(location) })
    }
  }

  const locations = [...new Set(unparsed.map((c) => c.location))]
  const table = await lookUpLocations(qx, llm, locations)
  const ambiguous: ICountryCandidate[] = []

  for (const candidate of unparsed) {
    const row = table.get(candidate.location)
    if (row?.status === 'resolved') {
      updates.push({
        id: candidate.id,
        source: LLM_SOURCE,
        country: getCountryName(row.countryCode),
      })
    } else if (row?.status === 'ambiguous') {
      ambiguous.push(candidate)
    }
  }

  if (withHints && ambiguous.length > 0) {
    updates.push(...(await resolveWithHints(qx, llm, ambiguous)))
  }

  return updates
}
