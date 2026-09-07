import { proxyActivities } from '@temporalio/workflow'

import { parseLlmJson } from '@crowd/common'
import { LlmQueryType } from '@crowd/types'

import * as activities from '../../activities'
import { FakeOrganizationAnalysisInput, FakeOrganizationVerdict } from '../../types/organization'

const {
  getOrganizationForFakeAnalysis,
  getLLMResult,
  markOrganizationAsFake,
  createFakeOrganizationSuggestion,
} = proxyActivities<typeof activities>({
  startToCloseTimeout: '15 minutes',
})

export async function fakeOrganizationAnalysisWithLLM(
  args: FakeOrganizationAnalysisInput,
): Promise<void> {
  const organizationId = args.organizationId

  const context = await getOrganizationForFakeAnalysis(organizationId)

  if (!context) {
    return
  }

  const PROMPT = `This JSON is an organization created from a member's email domain, plus that member.
    <json> ${JSON.stringify(context)} </json>

    TASK
    Decide whether the domain represents a real organization (company, employer, university, government, foundation) or a personal/vanity domain that should not be treated as a company.

    HOW THIS ORG WAS CREATED
    - Ingest saw a verified member email, took the domain, created this organization, and linked that member.
    - There is almost always exactly one member, and role.source is "email-domain". Every org you see here looks like that, so member count and role source are not signals.
    - Public inboxes (gmail, outlook, …) are already excluded. A custom domain is not by itself a company.
    - description, headline, industry, location, and size are usually still empty because the org was just minted from a domain. That is also not a signal.

    Use the org domain identities, the member's displayName, emails, and usernames. You may use knowledge of well-known real organizations when you are sure.

    Decide in this order:

    1. GENUINE — you recognize this as a real organization from the document or from knowledge you are sure of. Stop here.
    2. FAKE — the domain is the linked member's name or personal brand, and you do not recognize this as a company.
      Compare the domain's registrable name (label before the TLD, ignoring hyphens, dots, digits) to the member's displayName: given name, family name, given+family, initials. Any language or script.
      Also use member emails and usernames. A domain that is just that person's name or personal brand (.me, name.dev, firstlast.io) is the fake pattern. Use it.
    3. UNSURE — the name relationship is weak or partial, or the domain looks like it could be a real one-person shop (consultancy, studio, or product name that is not just the person).

    Return ONLY valid JSON. No code fences or extra text.

    JSON SCHEMA:
    { "verdict": "fake" | "genuine" | "unsure", "reason": "<short concise explanation>" }
  `

  const llm = await getLLMResult(LlmQueryType.FAKE_ORGANIZATION_ANALYSIS, PROMPT, organizationId)
  const { verdict } = parseLlmJson<{ verdict?: FakeOrganizationVerdict }>(llm.answer)

  switch (verdict) {
    case 'fake':
      await markOrganizationAsFake(organizationId)
      break
    case 'genuine':
      break
    // unsure, and any verdict the model made up, goes to human review
    default:
      await createFakeOrganizationSuggestion(organizationId)
  }
}
