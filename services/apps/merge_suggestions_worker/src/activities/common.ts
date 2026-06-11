/* eslint-disable @typescript-eslint/no-explicit-any */
import { BedrockRuntimeClient, InvokeModelCommand } from '@aws-sdk/client-bedrock-runtime'
import axios from 'axios'
import { performance } from 'perf_hooks'

import { IS_LLM_ENABLED } from '@crowd/common'
import { CommonMemberService } from '@crowd/common_services'
import { pgpQx } from '@crowd/data-access-layer'
import { ITenant } from '@crowd/data-access-layer/src/old/apps/merge_suggestions_worker//types'
import LLMSuggestionVerdictsRepository from '@crowd/data-access-layer/src/old/apps/merge_suggestions_worker/llmSuggestionVerdicts.repo'
import TenantRepository from '@crowd/data-access-layer/src/old/apps/merge_suggestions_worker/tenant.repo'
import {
  ILLMConsumableMember,
  ILLMConsumableOrganization,
  ILLMSuggestionVerdict,
} from '@crowd/types'

import { svc } from '../main'
import { ILLMResult } from '../types'

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

export async function saveLLMVerdict(verdict: ILLMSuggestionVerdict): Promise<string> {
  const llmVerdictRepository = new LLMSuggestionVerdictsRepository(
    svc.postgres.writer.connection(),
    svc.log,
  )
  return llmVerdictRepository.saveLLMVerdict(verdict)
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
