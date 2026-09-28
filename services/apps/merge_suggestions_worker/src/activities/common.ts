import { performance } from 'perf_hooks'

/* eslint-disable @typescript-eslint/no-explicit-any */
import { BedrockRuntimeClient, InvokeModelCommand } from '@aws-sdk/client-bedrock-runtime'
import { ApplicationFailure } from '@temporalio/client'
import axios from 'axios'
import { z } from 'zod'

import { Error404, IS_LLM_ENABLED } from '@crowd/common'
import { CommonMemberService } from '@crowd/common_services'
import { insertLlmSuggestionVerdict, pgpQx } from '@crowd/data-access-layer'
import { ITenant } from '@crowd/data-access-layer/src/old/apps/merge_suggestions_worker//types'
import TenantRepository from '@crowd/data-access-layer/src/old/apps/merge_suggestions_worker/tenant.repo'
import {
  ILLMConsumableMember,
  ILLMConsumableOrganization,
  ILLMSuggestionVerdict,
} from '@crowd/types'

import { svc } from '../main'
import { ILLMMergeDecisionResult, ILLMResult, ILLMToolUseContent } from '../types'

const MERGE_DECISION_TOOL = {
  name: 'submit_merge_decision',
  description: 'Submit whether the two compared entities are the same.',
  input_schema: {
    type: 'object',
    properties: {
      reason: {
        type: 'string',
        description: 'One short sentence explaining the decision.',
      },
      decision: {
        type: 'boolean',
        description: 'true if both entities are the same, false otherwise.',
      },
    },
    required: ['reason', 'decision'],
  },
}

const mergeDecisionSchema = z.object({
  reason: z.string().trim().min(1),
  decision: z.boolean(),
})

export async function getAllTenants(): Promise<ITenant[]> {
  const tenantRepository = new TenantRepository(svc.postgres.writer.connection(), svc.log)
  const tenants = await tenantRepository.getAllTenants()

  return tenants
}

export async function getLLMResult(
  suggestion: ILLMConsumableMember[] | ILLMConsumableOrganization[],
  modelId: string,
  prompt: string,
  region: string,
  modelSpecificArgs: any,
): Promise<ILLMResult> {
  if (!IS_LLM_ENABLED) {
    svc.log.error('LLM usage is disabled. Check CROWD_LLM_ENABLED env variable!')
    return
  }

  if (suggestion.length !== 2) {
    console.log(suggestion)
    throw new Error('Exactly 2 entities are required for LLM comparison')
  }

  const client = new BedrockRuntimeClient({
    credentials: {
      accessKeyId: process.env['CROWD_AWS_BEDROCK_ACCESS_KEY_ID'],
      secretAccessKey: process.env['CROWD_AWS_BEDROCK_SECRET_ACCESS_KEY'],
    },
    region,
  })

  const start = performance.now()

  const end = () => {
    const end = performance.now()
    const duration = end - start
    return Math.ceil(duration / 1000)
  }

  const fullPrompt = `Your task is to analyze the following two json documents. <json> ${JSON.stringify(
    suggestion,
  )} </json>. ${prompt}`

  const command = new InvokeModelCommand({
    body: JSON.stringify({
      messages: [
        {
          role: 'user',
          content: [
            {
              type: 'text',
              text: fullPrompt,
            },
          ],
        },
      ],
      ...modelSpecificArgs,
    }),
    modelId,
    accept: 'application/json',
    contentType: 'application/json',
  })

  const res = await client.send(command)

  return {
    body: JSON.parse(res.body.transformToString()),
    prompt: fullPrompt,
    modelSpecificArgs,
    responseTimeSeconds: end(),
  }
}

export async function getLLMMergeDecision(
  entities: ILLMConsumableMember[] | ILLMConsumableOrganization[],
  modelId: string,
  prompt: string,
  region: string,
  modelSpecificArgs: Record<string, unknown>,
): Promise<ILLMMergeDecisionResult> {
  if (!IS_LLM_ENABLED) {
    throw ApplicationFailure.nonRetryable(
      'LLM usage is disabled. Check CROWD_ENABLE_LLM env variable!',
    )
  }

  const result = await getLLMResult(entities, modelId, prompt, region, {
    ...modelSpecificArgs,
    tools: [MERGE_DECISION_TOOL],
    tool_choice: { type: 'tool', name: MERGE_DECISION_TOOL.name },
  })

  const toolUse = result.body.content.find(
    (content): content is ILLMToolUseContent =>
      content.type === 'tool_use' && content.name === MERGE_DECISION_TOOL.name,
  )
  const parsed = mergeDecisionSchema.safeParse(toolUse?.input)

  if (!parsed.success) {
    svc.log.warn(
      { content: result.body.content, stopReason: result.body.stop_reason },
      'LLM returned an invalid merge decision',
    )
    throw ApplicationFailure.retryable(
      `Invalid LLM merge decision: ${z.prettifyError(parsed.error)}`,
      'InvalidLLMMergeDecision',
    )
  }

  return {
    response: parsed.data,
    prompt: result.prompt,
    inputTokenCount: result.body.usage.input_tokens,
    outputTokenCount: result.body.usage.output_tokens,
    responseTimeSeconds: result.responseTimeSeconds,
  }
}

export async function saveLLMVerdict(verdict: ILLMSuggestionVerdict): Promise<void> {
  const qx = pgpQx(svc.postgres.writer.connection())
  await insertLlmSuggestionVerdict(qx, verdict)
}

export async function mergeMembers(
  primaryMemberId: string,
  secondaryMemberId: string,
): Promise<void> {
  const qx = pgpQx(svc.postgres.writer.connection())
  const memberService = new CommonMemberService(qx, svc.temporal, svc.log)

  try {
    await memberService.merge(primaryMemberId, secondaryMemberId)
  } catch (error) {
    if (error instanceof Error404) {
      svc.log.info(
        { primaryMemberId, secondaryMemberId },
        'Skipping merge, member no longer exists',
      )
      return
    }

    svc.log.error({ err: error }, 'Failed to merge members')
    throw error
  }
}

export async function mergeOrganizations(
  primaryOrganizationId: string,
  secondaryOrganizationId: string,
): Promise<void> {
  const url = `${process.env['CROWD_API_SERVICE_URL']}/organization/${primaryOrganizationId}/merge`
  const requestOptions = {
    method: 'PUT',
    headers: {
      Authorization: `Bearer ${process.env['CROWD_LF_AGENT_USER_TOKEN']}`,
      'Content-Type': 'application/json',
    },
    data: {
      organizationToMerge: secondaryOrganizationId,
      segments: [],
    },
  }

  try {
    await axios(url, requestOptions)
  } catch (error) {
    svc.log.error({ err: error, status: error.response?.status }, 'Failed to merge organization')
    throw error
  }
}
