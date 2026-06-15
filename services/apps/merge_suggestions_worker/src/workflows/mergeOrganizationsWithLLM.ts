import { continueAsNew, proxyActivities } from '@temporalio/workflow'

import { LLMSuggestionVerdictType, OrganizationMergeSuggestionTable } from '@crowd/types'

import type * as activities from '../activities'
import { ILLMResult, IProcessMergeOrganizationSuggestionsWithLLM } from '../types'

const {
  getRawOrganizationMergeSuggestions,
  getOrganizationsForLLMConsumption,
  removeOrganizationMergeSuggestions,
  addOrganizationSuggestionToNoMerge,
} = proxyActivities<typeof activities>({
  startToCloseTimeout: '2 minutes',
  retry: { maximumAttempts: 3 },
})

const { getLLMResult, saveLLMVerdict, mergeOrganizations } = proxyActivities<typeof activities>({
  startToCloseTimeout: '5 minutes',
  retry: {
    initialInterval: '1 minute',
    backoffCoefficient: 2,
    maximumInterval: '4 minutes',
    maximumAttempts: 4,
  },
})

export async function mergeOrganizationsWithLLM(
  args: IProcessMergeOrganizationSuggestionsWithLLM,
): Promise<void> {
  const SUGGESTIONS_PER_RUN = 5
  const REGION = 'us-east-1'
  const MODEL_ID = 'us.anthropic.claude-sonnet-4-20250514-v1:0'
  const MODEL_ARGS = {
    max_tokens: 2000,
    anthropic_version: 'bedrock-2023-05-31',
    temperature: 0,
  }
  const PROMPT = `Please compare and come up with a boolean answer if these two organizations are the same organization or not. Print 'true' if they are the same organization, 'false' otherwise. No explanation required. Don't print anything else.`

  const suggestions = await getRawOrganizationMergeSuggestions(
    args.tenantId,
    args.similarity,
    SUGGESTIONS_PER_RUN,
    args.onlyLFXMembers,
    args.organizationIds,
  )

  if (suggestions.length === 0) {
    return
  }

  for (const suggestion of suggestions) {
    const organizations = await getOrganizationsForLLMConsumption(suggestion)

    if (organizations.length !== 2) {
      console.log(
        `Failed getting organization data in suggestion. Skipping suggestion: ${suggestion}`,
      )
      await removeOrganizationMergeSuggestions(
        suggestion,
        OrganizationMergeSuggestionTable.ORGANIZATION_TO_MERGE_FILTERED,
      )
      await removeOrganizationMergeSuggestions(
        suggestion,
        OrganizationMergeSuggestionTable.ORGANIZATION_TO_MERGE_RAW,
      )
      continue
    }

    const llmResult: ILLMResult = await getLLMResult(
      organizations,
      MODEL_ID,
      PROMPT,
      REGION,
      MODEL_ARGS,
    )

    await saveLLMVerdict({
      type: LLMSuggestionVerdictType.ORGANIZATION,
      model: MODEL_ID,
      primaryId: suggestion[0],
      secondaryId: suggestion[1],
      prompt: llmResult.prompt,
      responseTimeSeconds: llmResult.responseTimeSeconds,
      inputTokenCount: llmResult.body.usage.input_tokens,
      outputTokenCount: llmResult.body.usage.output_tokens,
      verdict: llmResult.body.content[0].text,
    })

    if (llmResult.body.content[0].text === 'true') {
      console.log(
        `LLM verdict says these two orgs are the same. Merging organizations: ${suggestion[0]} and ${suggestion[1]}!`,
      )
      await mergeOrganizations(suggestion[0], suggestion[1])
    } else {
      console.log(
        `LLM doesn't think these orgs are the same. Removing from suggestions and adding to no merge: ${suggestion[0]} and ${suggestion[1]}!`,
      )
      await removeOrganizationMergeSuggestions(
        suggestion,
        OrganizationMergeSuggestionTable.ORGANIZATION_TO_MERGE_FILTERED,
      )
      await removeOrganizationMergeSuggestions(
        suggestion,
        OrganizationMergeSuggestionTable.ORGANIZATION_TO_MERGE_RAW,
      )
      await addOrganizationSuggestionToNoMerge(suggestion)
    }
  }

  await continueAsNew<typeof mergeOrganizationsWithLLM>(args)
}
