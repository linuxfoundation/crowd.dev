import commandLineArgs from 'command-line-args'
import { uniq } from 'lodash'

import { DB_CONFIG } from '@/conf'
import {
  IS_LLM_ENABLED,
  getCountry,
  getCountryName,
  normalizeLocation,
  parseLlmJson,
  partition,
} from '@crowd/common'
import { LlmService } from '@crowd/common_services'
import {
  ILocationCountry,
  QueryExecutor,
  findLocationCountries,
  insertLocationCountries,
  pgpQx,
} from '@crowd/data-access-layer'
import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { getServiceLogger } from '@crowd/logging'
import { LlmQueryType, OrganizationAttributeSource } from '@crowd/types'

const log = getServiceLogger()

const BATCH_SIZE = 1000
const TEST_RUN_LIMIT = 100
const LOCATIONS_PER_PROMPT = 100
const MEMBERS_PER_PROMPT = 50
const PARSER_SOURCE = OrganizationAttributeSource.SYSTEM
const LLM_SOURCE = OrganizationAttributeSource.LLM

const options = [
  {
    name: 'type',
    type: String,
    description: 'Entity to backfill: member or org.',
  },
  {
    name: 'testRun',
    alias: 't',
    type: Boolean,
    description: `Process a single batch of ${TEST_RUN_LIMIT} candidates and exit.`,
  },
]

const parameters = commandLineArgs(options)

interface ICandidate {
  id: string
  location: string
}

interface ICountryUpdate {
  id: string
  source: string
  country: string
}

interface IMemberHints {
  orgCountries: string[]
  emailDomains: string[]
}

async function fetchMembers(
  qx: QueryExecutor,
  limit: number,
  afterId?: string,
): Promise<ICandidate[]> {
  return qx.select(
    `
      select id, attributes -> 'location' ->> 'default' as location
      from members
      where "deletedAt" is null
        ${afterId ? 'and id > $(afterId)' : ''}
        and nullif(trim(attributes -> 'location' ->> 'default'), '') is not null
        and not exists (
          select 1
          from jsonb_each_text(coalesce(attributes -> 'country', '{}'::jsonb)) c
          where nullif(trim(c.value), '') is not null
        )
      order by id
      limit $(limit)
    `,
    { limit, afterId },
  )
}

async function fetchOrganizations(
  qx: QueryExecutor,
  limit: number,
  afterId?: string,
): Promise<ICandidate[]> {
  return qx.select(
    `
      select o.id, o.location
      from organizations o
      where o."deletedAt" is null
        ${afterId ? 'and o.id > $(afterId)' : ''}
        and nullif(trim(o.location), '') is not null
        and nullif(trim(o.country), '') is null
        and not exists (
          select 1
          from "orgAttributes" a
          where a."organizationId" = o.id
            and a.name = 'country'
            and nullif(trim(a.value), '') is not null
        )
      order by o.id
      limit $(limit)
    `,
    { limit, afterId },
  )
}

async function saveMemberCountries(qx: QueryExecutor, updates: ICountryUpdate[]): Promise<void> {
  await qx.result(
    `
      update members m
      set attributes = jsonb_set(
            m.attributes,
            '{country}',
            jsonb_build_object(u.source, u.country, 'default', u.country)
          ),
          "updatedAt" = now()
      from unnest($(ids)::uuid[], $(sources)::text[], $(countries)::text[]) as u(id, source, country)
      where m.id = u.id
        and not exists (
          select 1
          from jsonb_each_text(coalesce(m.attributes -> 'country', '{}'::jsonb)) c
          where nullif(trim(c.value), '') is not null
        )
    `,
    {
      ids: updates.map((u) => u.id),
      sources: updates.map((u) => u.source),
      countries: updates.map((u) => u.country),
    },
  )
}

async function saveOrganizationCountries(
  qx: QueryExecutor,
  updates: ICountryUpdate[],
): Promise<void> {
  await qx.result(
    `
      with updated as (
        update organizations o
        set country = u.country,
            "updatedAt" = now()
        from unnest($(ids)::uuid[], $(sources)::text[], $(countries)::text[]) as u(id, source, country)
        where o.id = u.id
          and nullif(trim(o.country), '') is null
        returning o.id, u.source, u.country
      )
      insert into "orgAttributes" ("organizationId", name, source, "default", value)
      select id, 'country', source, true, country
      from updated
      on conflict ("organizationId", name, source, md5(value)) do nothing
    `,
    {
      ids: updates.map((u) => u.id),
      sources: updates.map((u) => u.source),
      countries: updates.map((u) => u.country),
    },
  )
}

async function fetchMemberHints(
  qx: QueryExecutor,
  memberIds: string[],
): Promise<Map<string, IMemberHints>> {
  const rows: { memberId: string; orgCountry: string | null; emailDomain: string | null }[] =
    await qx.select(
      `
        select mo."memberId", o.country as "orgCountry", null as "emailDomain"
        from "memberOrganizations" mo
        join organizations o on o.id = mo."organizationId"
        where mo."memberId" in ($(memberIds:csv))
          and mo."deletedAt" is null
          and mo."dateEnd" is null
          and o."deletedAt" is null
          and nullif(trim(o.country), '') is not null
        union all
        select "memberId", null, lower(split_part(value, '@', 2))
        from "memberIdentities"
        where "memberId" in ($(memberIds:csv))
          and type = 'email'
          and verified
          and "deletedAt" is null
      `,
      { memberIds },
    )

  const hints = new Map<string, IMemberHints>()
  for (const { memberId, orgCountry, emailDomain } of rows) {
    const memberHints = hints.get(memberId) ?? { orgCountries: [], emailDomains: [] }
    if (orgCountry) memberHints.orgCountries.push(orgCountry)
    if (emailDomain) memberHints.emailDomains.push(emailDomain)
    hints.set(memberId, memberHints)
  }

  return hints
}

function buildLocationsPrompt(locations: string[]): string {
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

function buildHintsPrompt(members: (ICandidate & IMemberHints)[]): string {
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

async function askLlm<T>(llm: LlmService, prompt: string): Promise<T[]> {
  try {
    const { answer } = await llm.queryLlm(LlmQueryType.LOCATION_COUNTRY, prompt)
    const answers = parseLlmJson<T[]>(answer)
    if (!Array.isArray(answers)) {
      throw new Error('LLM answer is not a JSON array!')
    }
    return answers
  } catch (err) {
    log.error({ err }, 'LLM call failed!')
    return []
  }
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
  members: (ICandidate & IMemberHints)[],
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
  members: ICandidate[],
): Promise<ICountryUpdate[]> {
  const hints = await fetchMemberHints(
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

async function inferCountries(
  qx: QueryExecutor,
  llm: LlmService,
  candidates: ICandidate[],
  isMember: boolean,
): Promise<ICountryUpdate[]> {
  const updates: ICountryUpdate[] = []
  const unparsed: ICandidate[] = []

  for (const { id, location } of candidates) {
    const country = getCountry(location)
    if (country) {
      updates.push({ id, source: PARSER_SOURCE, country })
    } else {
      unparsed.push({ id, location: normalizeLocation(location) })
    }
  }

  const table = await lookUpLocations(qx, llm, uniq(unparsed.map((c) => c.location)))
  const ambiguous: ICandidate[] = []

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

  if (isMember && ambiguous.length > 0) {
    updates.push(...(await resolveWithHints(qx, llm, ambiguous)))
  }

  return updates
}

setImmediate(async () => {
  const { type } = parameters
  if (type !== 'member' && type !== 'org') {
    log.error({ type }, 'Invalid --type! Use member or org.')
    process.exit(1)
  }

  const bedrockCredentials = {
    accessKeyId: process.env.CROWD_AWS_BEDROCK_ACCESS_KEY_ID,
    secretAccessKey: process.env.CROWD_AWS_BEDROCK_SECRET_ACCESS_KEY,
  }

  if (!IS_LLM_ENABLED || !bedrockCredentials.accessKeyId || !bedrockCredentials.secretAccessKey) {
    log.error('LLM is disabled! Set CROWD_ENABLE_LLM and the Bedrock credentials.')
    process.exit(1)
  }

  const isMember = type === 'member'
  const fetchCandidates = isMember ? fetchMembers : fetchOrganizations
  const saveCountries = isMember ? saveMemberCountries : saveOrganizationCountries

  const testRun = parameters.testRun ?? false
  const limit = testRun ? TEST_RUN_LIMIT : BATCH_SIZE

  const db = await getDbConnection({
    host: DB_CONFIG.writeHost,
    port: DB_CONFIG.port,
    database: DB_CONFIG.database,
    user: DB_CONFIG.username,
    password: DB_CONFIG.password,
  })

  const qx = pgpQx(db)
  const llm = new LlmService(qx, bedrockCredentials, log)

  log.info({ type, testRun }, 'Running script with the following parameters!')

  let processedCount = 0
  let updatedCount = 0
  let afterId: string | undefined
  let candidates: ICandidate[]

  do {
    candidates = await fetchCandidates(qx, limit, afterId)

    const updates = await inferCountries(qx, llm, candidates, isMember)
    if (updates.length > 0) {
      await saveCountries(qx, updates)
    }

    if (testRun) {
      const locations = new Map(candidates.map((c) => [c.id, c.location]))
      updates.forEach((u) => log.info({ ...u, location: locations.get(u.id) }, 'Updated country!'))
    }

    processedCount += candidates.length
    updatedCount += updates.length
    afterId = candidates[candidates.length - 1]?.id

    log.info({ processedCount, updatedCount, afterId }, 'Batch processed!')
  } while (candidates.length === limit && !testRun)

  log.info({ type, processedCount, updatedCount }, 'Backfill finished!')

  process.exit(0)
})
