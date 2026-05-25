import { continueAsNew, proxyActivities } from '@temporalio/workflow'

import { LLMSuggestionVerdictType, MemberMergeSuggestionTable } from '@crowd/types'

import * as commonActivities from '../activities/common'
import * as memberActivities from '../activities/memberMergeSuggestions'
import { ILLMResult, IProcessMergeMemberSuggestionsWithLLM } from '../types'
import { removeEmailLikeIdentitiesFromMember } from '../utils'

const memberActivitiesProxy = proxyActivities<typeof memberActivities>({
  startToCloseTimeout: '1 minute',
})

const commonActivitiesProxy = proxyActivities<typeof commonActivities>({
  startToCloseTimeout: '5 minute',
  retry: {
    initialInterval: '10 seconds',
    maximumAttempts: 3,
  },
})

export async function mergeMembersWithLLM(
  args: IProcessMergeMemberSuggestionsWithLLM,
): Promise<void> {
  const SUGGESTIONS_PER_RUN = 10
  const REGION = 'us-east-1'
  const MODEL_ID = 'us.anthropic.claude-sonnet-4-20250514-v1:0'
  const MODEL_ARGS = {
    max_tokens: 2000,
    anthropic_version: 'bedrock-2023-05-31',
    temperature: 0,
  }
  const PROMPT = `Please compare and come up with a boolean answer if these two members are the same person or not. 
                  Only compare data from first member and second member. Never compare data from only one member with itself. 
                  Never tokenize 'platform' field using character tokenization. Use word tokenization for platform field in identities.
                  You should check all the sent fields between members to find similarities both literally and semantically. 
                  Here are the fields written with respect to their importance and how to check. Identities >> Organizations > Attributes and other fields >> Display name - 
                  1. Identities: Tokenize value field (identity.value) using character tokenization. Exact match or identities with edit distance <= 2 suggests that members are similar. 
                  Don't compare identities in a single member. Only compare identities between members. 
                  2. Organizations: Members are more likely to be the same when they have/had roles in similar organizations. 
                  If there are no intersecting organizations it doesn't necessarily mean that they're different members.
                  3. Attributes and other fields: If one member have a specific field and other member doesn't, skip that field when deciding similarity. 
                  Checking semantically instead of literally is important for such fields. Important fields here are: location, timezone, languages, programming languages. 
                  For example one member might have Berlin in location, while other can have Germany - consider such members have same location.  
                  4. Display Name: Tokenize using both character and word tokenization. When the display name is more than one word, and the difference is a few edit distances consider it a strong indication of similarity.
                  When one display name is contained by the other, check other fields for the final decision. The same members on different platforms might have different display names.
                  Display names can be multiple words and might be sorted in different order in different platforms for the same member. Display name is a supporting signal only — it is never sufficient on its own. 
                  If display name is the only thing that matches and there are no corroborating signals from identities, organizations, or attributes, return 'false'.
                  CRITICAL RULE - NEVER MERGE IF SAME PLATFORM WITH DIFFERENT VALUES:
                  Before making any decision, you MUST check if both members have identities on the same platform.
                  If member1.identities[x].platform === member2.identities[y].platform (they share a platform), then:
                  - Check if member1.identities[x].value === member2.identities[y].value
                  - If the values are DIFFERENT, immediately return 'false' - these are definitely different people
                  - This rule applies REGARDLESS of how similar other fields appear.
                  This check must be performed FIRST before evaluating any other similarities. Only do such labeling if both members have identities in the same platform. If they don't have identities in the same platform, ignore the rule.
                  BOT CHECKS - NEVER MERGE IF ONE PROFILE IS A BOT AND THE OTHER IS NOT
                  - Check the bot status in attributes.isBot.default for each member
                  - If one member has attributes.isBot.default === true and the other has attributes.isBot.default === false (or undefined), return 'false'
                  - Bots and humans are never the same entity
                  - This check must be performed before evaluating any other similarities
                  Print 'true' if they are the same member, 'false' otherwise. No explanation required. Don't print anything else.`

  const suggestions = await memberActivitiesProxy.getRawMemberMergeSuggestions(
    args.similarity,
    SUGGESTIONS_PER_RUN,
  )

  if (suggestions.length === 0) {
    return
  }

  for (const suggestion of suggestions) {
    const members = await memberActivitiesProxy.getMembersForLLMConsumption(suggestion)

    if (members.length !== 2) {
      console.log(`Failed getting members data in suggestion. Skipping suggestion: ${suggestion}`)
      await memberActivitiesProxy.removeMemberMergeSuggestion(
        suggestion,
        MemberMergeSuggestionTable.MEMBER_TO_MERGE_RAW,
      )
      await memberActivitiesProxy.removeMemberMergeSuggestion(
        suggestion,
        MemberMergeSuggestionTable.MEMBER_TO_MERGE_FILTERED,
      )
      continue
    }

    const llmResult: ILLMResult = await commonActivitiesProxy.getLLMResult(
      members.map((member) => removeEmailLikeIdentitiesFromMember(member)),
      MODEL_ID,
      PROMPT,
      REGION,
      MODEL_ARGS,
    )

    await commonActivitiesProxy.saveLLMVerdict({
      type: LLMSuggestionVerdictType.MEMBER,
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
        `LLM verdict says these two members are the same. Merging members: ${suggestion[0]} and ${suggestion[1]}!`,
      )
      await commonActivitiesProxy.mergeMembers(suggestion[0], suggestion[1])
    } else {
      console.log(
        `LLM doesn't think these members are the same. Removing from suggestions and adding to no merge: ${suggestion[0]} and ${suggestion[1]}!`,
      )
      await memberActivitiesProxy.removeMemberMergeSuggestion(
        suggestion,
        MemberMergeSuggestionTable.MEMBER_TO_MERGE_FILTERED,
      )
      await memberActivitiesProxy.removeMemberMergeSuggestion(
        suggestion,
        MemberMergeSuggestionTable.MEMBER_TO_MERGE_RAW,
      )
      await memberActivitiesProxy.addMemberSuggestionToNoMerge(suggestion)
    }
  }

  await continueAsNew<typeof mergeMembersWithLLM>(args)
}
