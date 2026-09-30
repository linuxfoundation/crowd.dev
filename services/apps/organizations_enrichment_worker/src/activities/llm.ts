import { ApplicationFailure } from '@temporalio/client'

import { parseLlmJson } from '@crowd/common'
import { LlmService } from '@crowd/common_services'
import {
  OrganizationField,
  fetchOrgIdentities,
  findOrgAttributes,
  findOrgById,
} from '@crowd/data-access-layer'
import { dbStoreQx } from '@crowd/data-access-layer/src/queryExecutor'
import { IOrganizationIdentity, LlmQueryType, OrganizationIdentityType } from '@crowd/types'

import { svc } from '../main'

interface LlmDomainSelection {
  index: number
  reason: string
}

export async function selectMostRelevantDomainWithLLM(
  organizationId: string,
  domains: IOrganizationIdentity[],
): Promise<IOrganizationIdentity> {
  const qx = dbStoreQx(svc.postgres.writer)

  // Fetch organization data
  const [base, identities, attributes] = await Promise.all([
    findOrgById(qx, organizationId, [
      OrganizationField.ID,
      OrganizationField.DISPLAY_NAME,
      OrganizationField.DESCRIPTION,
      OrganizationField.LOGO,
      OrganizationField.TAGS,
      OrganizationField.LOCATION,
      OrganizationField.TYPE,
      OrganizationField.HEADLINE,
      OrganizationField.INDUSTRY,
      OrganizationField.FOUNDED,
    ]),
    fetchOrgIdentities(qx, organizationId),
    findOrgAttributes(qx, organizationId),
  ])

  // Build organization context
  const pickValues = (name: string, limit = 10) =>
    attributes
      .filter((a) => a.name === name)
      .map((a) => a.value)
      .slice(0, limit)

  const organization = {
    displayName: base?.displayName ?? '',
    description: base.description?.substring(0, 1000),
    phoneNumbers: pickValues('phoneNumber', 3),
    logo: base.logo,
    tags: base.tags,
    location: base.location,
    type: base.type,
    geoLocation: attributes.find((a) => a.name === 'geoLocation')?.value ?? '',
    ticker: attributes.find((a) => a.name === 'ticker')?.value ?? '',
    profiles: pickValues('profile'),
    headline: base.headline,
    industry: base.industry,
    founded: base.founded,
    alternativeNames: pickValues('alternativeName'),
    identities: identities
      .filter((i) => i.type === OrganizationIdentityType.USERNAME && i.verified)
      .map((i) => ({ platform: i.platform, value: i.value }))
      .slice(0, 15),
  }

  const domainValues = domains.map((d) => d.value)

  // Initialize LLM service
  const llmService = new LlmService(
    qx,
    {
      accessKeyId: process.env.CROWD_AWS_BEDROCK_ACCESS_KEY_ID,
      secretAccessKey: process.env.CROWD_AWS_BEDROCK_SECRET_ACCESS_KEY,
    },
    svc.log,
  )

  const numberedDomains = domainValues.map((domain, i) => `${i + 1}. ${domain}`).join('\n    ')

  // Generate prompt
  const PROMPT = `
    Analyze the following organization data and determine which domain is the most relevant primary domain.

    <organization>
    ${JSON.stringify(organization)}
    </organization>

    <domains>
    ${numberedDomains}
    </domains>

    REQUIREMENTS:
    - Select exactly ONE domain from <domains> and return its number as "index".
    - Only the numbered domains are valid choices. Never consider a domain outside the list, even if it seems more likely.
    - Use the organization data as context and pick the most relevant domain from the list.

    SELECTION RULES:
    1. Choose the domain representing the organization's main corporate identity and primary brand. 
    2. Use identities (GitHub, LinkedIn, and other social media platforms) to validate the main domain. 
    3. Avoid subsidiary or acquired domains unless they represent the main identity.
    4. If listed domains differ only by TLD, prefer the listed .com domain if there is one, unless a regional domain is clearly dominant.
    5. Ignore temporary, testing, or unrelated domains.

    OUTPUT FORMAT:
    - Return ONLY valid JSON — no markdown, code fences, explanations, or any extra text.
    - The JSON must begin with '{' and end with '}'.

    {
      "index": <number of the selected domain in <domains>>,
      "reason": "<short explanation>"
    }
  `

  const buildRetryPrompt = (invalidIndex: unknown) => `
    Your previous answer ("index": ${JSON.stringify(invalidIndex)}) is not valid.
    "index" MUST be a whole number from 1 to ${domains.length}, matching one of the numbered domains in <domains>.

    Re-read the original instructions below.
    ---------------
    ${PROMPT}
  `

  // Execute LLM query
  const executeLlmQuery = async (prompt: string) => {
    const response = await llmService.queryLlm(
      LlmQueryType.SELECT_MOST_RELEVANT_DOMAIN,
      prompt,
      organizationId,
    )
    if (!response) throw new Error('LLM returned no response')
    return parseLlmJson<LlmDomainSelection>(response.answer)
  }

  const MAX_RETRIES = 1

  try {
    let invalidIndex: unknown

    for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
      const prompt = attempt === 0 ? PROMPT : buildRetryPrompt(invalidIndex)
      const selection = await executeLlmQuery(prompt)
      const index = selection?.index

      const selected = Number.isInteger(index) ? domains[index - 1] : undefined
      if (selected) return selected

      invalidIndex = index
    }

    // temperature is 0, so Temporal retries would return the same answer
    throw ApplicationFailure.nonRetryable(
      `LLM returned invalid domain index ${JSON.stringify(invalidIndex)} for [${domainValues.join(', ')}] after ${MAX_RETRIES + 1} attempts`,
      'LLM_INVALID_DOMAIN_SELECTION',
    )
  } catch (err) {
    svc.log.error({ organizationId, err: (err as Error).message }, 'Failed to select domain')
    throw err
  }
}
