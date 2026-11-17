import commandLineArgs from 'command-line-args'
import { z } from 'zod'

import { DB_CONFIG } from '@/conf'
import { IS_LLM_ENABLED, getCountryName, parseLlmJson, partition } from '@crowd/common'
import { LlmService } from '@crowd/common_services'
import {
  IDbOrgAttributeInput,
  QueryExecutor,
  insertPromptHistoryEntry,
  pgpQx,
  recalculateOrgAttributeDefaults,
  upsertOrgAttributes,
} from '@crowd/data-access-layer'
import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { getServiceLogger } from '@crowd/logging'
import { ILlmResponse, LlmQueryType, OrganizationAttributeSource } from '@crowd/types'

const log = getServiceLogger()

const BATCH_SIZE = 500
const CONCURRENCY = 10
const QUERY_TYPE = LlmQueryType.ORGANIZATION_CLASSIFICATION

const CATEGORIES = [
  'company',
  'university',
  'government',
  'nonprofit',
  'oss-project',
  'individual',
  'placeholder',
  'unknown',
] as const

type Category = (typeof CATEGORIES)[number]

const REAL_ORGANIZATION_CATEGORIES: Category[] = [
  'company',
  'university',
  'government',
  'nonprofit',
]

const answerSchema = z.object({
  category: z.enum(CATEGORIES),
  confidence: z.enum(['high', 'medium', 'low']),
  country: z.string().nullable().catch(null),
  city: z.string().trim().min(1).nullable().catch(null),
})

type Answer = z.infer<typeof answerSchema>

interface IClassificationCandidate {
  id: string
  displayName: string
  description: string | null
  headline: string | null
  identities: { platform: string; type: string; value: string; verified: boolean }[]
}

const options = [
  {
    name: 'testRun',
    alias: 't',
    type: Boolean,
    description: `Process a single batch of ${BATCH_SIZE} organizations and exit.`,
  },
  {
    name: 'dryRun',
    alias: 'd',
    type: Boolean,
    description: 'Log the answers without saving anything.',
  },
]

const parameters = commandLineArgs(options)

async function fetchOrganizations(
  qx: QueryExecutor,
  afterId?: string,
): Promise<IClassificationCandidate[]> {
  return qx.select(
    `
      select
        o.id,
        o."displayName",
        o.description,
        o.headline,
        coalesce((
          select json_agg(i)
          from (
            select oi.platform, oi.type, oi.value, oi.verified
            from "organizationIdentities" oi
            where oi."organizationId" = o.id
            order by oi.verified desc
            limit 30
          ) i
        ), '[]'::json) as identities
      from organizations o
      join "organizationsGlobalActivityCount" ac
        on ac."organizationId" = o.id
        and ac.total_count_estimate > 0
      where o."deletedAt" is null
        ${afterId ? 'and o.id > $(afterId)' : ''}
        and nullif(trim(o.country), '') is null
        and nullif(trim(o.location), '') is null
      order by o.id
      limit $(limit)
    `,
    { afterId, limit: BATCH_SIZE },
  )
}

async function findClassifiedIds(qx: QueryExecutor, ids: string[]): Promise<Set<string>> {
  if (ids.length === 0) {
    return new Set()
  }

  const rows: { entityId: string }[] = await qx.select(
    `
      select "entityId"
      from "llmPromptHistory"
      where type = $(type)
        and "entityId" in ($(ids:csv))
    `,
    { type: QUERY_TYPE, ids },
  )

  return new Set(rows.map(({ entityId }) => entityId))
}

function buildPrompt({
  displayName,
  description,
  headline,
  identities,
}: IClassificationCandidate): string {
  return `You classify organizations in a data platform for open-source communities. Organizations are created from the employer names people put on their profiles, from email domains and from GitHub organizations, so many of them are not real organizations.

Pick one category:
- "company": a business, including startups, agencies and consultancies.
- "university": a university, college, school, or a research institute that belongs to one.
- "government": a government body or public agency.
- "nonprofit": a foundation, association, standards body or other nonprofit, including foundations that host open-source projects.
- "oss-project": an open-source project or its GitHub organization, with no legal entity of its own.
- "individual": a single person, such as a personal website or a person's name used as an employer.
- "placeholder": not an organization, such as "Self-Employed", "Freelance", "None", "Student" or "N/A".
- "unknown": you can't tell.

For "company", "university", "government" and "nonprofit", also give where the headquarters is:
- "country": ISO 3166-1 alpha-2 code.
- "city": city name.

Use null for anything you don't know, and for both fields in the other categories. A country-code domain such as .de or .fr is evidence for the country; generic ones such as .com, .io, .ai or .co are not. Never infer a country from the language of the name alone.

"confidence" is "high" if you know this organization or the evidence is clear, "medium" if the answer is likely, and "low" if you are mostly guessing.

Reply with only a JSON object:
{"category": "<category>", "confidence": "high" | "medium" | "low", "country": "<code>" or null, "city": "<city>" or null}

Organization:
${JSON.stringify({ displayName, description, headline, identities })}`
}

function toAttributes(answer: Answer): IDbOrgAttributeInput[] {
  if (answer.category === 'unknown' || answer.confidence === 'low') {
    return []
  }

  const values: Record<string, string | undefined> = { category: answer.category }

  if (REAL_ORGANIZATION_CATEGORIES.includes(answer.category)) {
    const country = answer.country ? getCountryName(answer.country) : undefined
    values.country = country
    values.location = country && answer.city ? `${answer.city}, ${country}` : undefined
  }

  return Object.entries(values)
    .filter(([, value]) => value !== null && value !== undefined)
    .map(([name, value]) => ({
      name,
      value: String(value),
      source: OrganizationAttributeSource.LLM,
      default: false,
    }))
}

// Prompt history marks an organization as done, so it must be saved after its attributes.
async function saveClassification(
  qx: QueryExecutor,
  organizationId: string,
  response: ILlmResponse,
  attributes: IDbOrgAttributeInput[],
): Promise<void> {
  await upsertOrgAttributes(qx, organizationId, attributes)
  await recalculateOrgAttributeDefaults(
    qx,
    organizationId,
    attributes.map(({ name }) => name),
  )
  await insertPromptHistoryEntry(qx, QUERY_TYPE, response.model, response, organizationId)
}

async function classifyOrganization(
  qx: QueryExecutor,
  llm: LlmService,
  organization: IClassificationCandidate,
  dryRun: boolean,
): Promise<number> {
  const response = await llm.queryLlm(
    QUERY_TYPE,
    buildPrompt(organization),
    organization.id,
    undefined,
    false,
  )
  const answer = answerSchema.parse(parseLlmJson(response.answer))
  const attributes = toAttributes(answer)

  if (!dryRun) {
    await saveClassification(qx, organization.id, response, attributes)
  }

  log.info(
    {
      organizationId: organization.id,
      displayName: organization.displayName,
      answer,
      attributes: attributes.map(({ name }) => name),
    },
    'Organization classified!',
  )

  return attributes.length
}

setImmediate(async () => {
  const testRun = parameters.testRun ?? false
  const dryRun = parameters.dryRun ?? false

  const bedrockCredentials = {
    accessKeyId: process.env.CROWD_AWS_BEDROCK_ACCESS_KEY_ID,
    secretAccessKey: process.env.CROWD_AWS_BEDROCK_SECRET_ACCESS_KEY,
  }

  if (!IS_LLM_ENABLED || !bedrockCredentials.accessKeyId || !bedrockCredentials.secretAccessKey) {
    log.error('LLM is disabled! Set CROWD_ENABLE_LLM and the Bedrock credentials.')
    process.exit(1)
  }

  const db = await getDbConnection({
    host: DB_CONFIG.writeHost,
    port: DB_CONFIG.port,
    database: DB_CONFIG.database,
    user: DB_CONFIG.username,
    password: DB_CONFIG.password,
  })

  const qx = pgpQx(db)
  const llm = new LlmService(qx, bedrockCredentials, log)

  log.info({ testRun, dryRun, BATCH_SIZE }, 'Running script with the following parameters!')

  let processedCount = 0
  let filledCount = 0
  let failedCount = 0
  let afterId: string | undefined
  let hasMore = true

  while (hasMore) {
    const organizations = await fetchOrganizations(qx, afterId)
    const classifiedIds = await findClassifiedIds(
      qx,
      organizations.map(({ id }) => id),
    )
    const unclassified = organizations.filter(({ id }) => !classifiedIds.has(id))

    for (const chunk of partition(unclassified, CONCURRENCY)) {
      await Promise.all(
        chunk.map(async (organization) => {
          try {
            const attributeCount = await classifyOrganization(qx, llm, organization, dryRun)
            filledCount += attributeCount > 0 ? 1 : 0
          } catch (err) {
            failedCount++
            log.error({ err, organizationId: organization.id }, 'Failed to classify organization!')
          }
        }),
      )
    }

    processedCount += unclassified.length
    afterId = organizations[organizations.length - 1]?.id
    hasMore = organizations.length === BATCH_SIZE && !(testRun && unclassified.length > 0)

    log.info({ processedCount, filledCount, failedCount, afterId }, 'Batch processed!')
  }

  log.info({ testRun, dryRun, processedCount, filledCount, failedCount }, 'Script finished!')
  process.exit(0)
})
