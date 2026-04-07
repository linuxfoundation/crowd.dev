import {
  BedrockRuntimeClient,
  InvokeModelCommand,
  InvokeModelCommandOutput,
} from '@aws-sdk/client-bedrock-runtime'
import { performance } from 'perf_hooks'

import { IS_LLM_ENABLED, parseLlmJson } from '@crowd/common'
import { insertPromptHistoryEntry } from '@crowd/data-access-layer'
import { QueryExecutor } from '@crowd/data-access-layer'
import { Logger, LoggerBase } from '@crowd/logging'
import {
  ILlmResponse,
  ILlmResult,
  ILlmSettings,
  LLM_MODEL_PRICING_MAP,
  LLM_MODEL_REGION_MAP,
  LLM_SETTINGS,
  LlmMemberEnrichmentResult,
  LlmQueryType,
} from '@crowd/types'

export interface IBedrockClientCredentials {
  accessKeyId: string
  secretAccessKey: string
}

export class LlmService extends LoggerBase {
  private readonly clientRegionMap: Map<string, BedrockRuntimeClient>
  private readonly qx: QueryExecutor

  public constructor(
    qx: QueryExecutor,
    private readonly bedrockCredentials: IBedrockClientCredentials,
    parentLog: Logger,
  ) {
    super(parentLog)

    if (!bedrockCredentials.accessKeyId || !bedrockCredentials.secretAccessKey) {
      this.log.warn('LLM usage is not configured properly. Missing Bedrock credentials!')
    }

    this.qx = qx
    this.clientRegionMap = new Map()
  }

  private client(settings: ILlmSettings): BedrockRuntimeClient {
    const region = LLM_MODEL_REGION_MAP[settings.modelId]

    if (!this.bedrockCredentials.accessKeyId || !this.bedrockCredentials.secretAccessKey) {
      this.log.warn('LLM usage is not configured properly. Missing Bedrock credentials!')
      return null
    }

    let client: BedrockRuntimeClient
    if (this.clientRegionMap.has(region)) {
      client = this.clientRegionMap.get(region)
    } else {
      client = new BedrockRuntimeClient({
        credentials: {
          accessKeyId: this.bedrockCredentials.accessKeyId,
          secretAccessKey: this.bedrockCredentials.secretAccessKey,
        },
        region,
      })
      this.clientRegionMap.set(region, client)
    }

    return client
  }

  public async queryLlm(
    type: LlmQueryType,
    prompt: string,
    entityId?: string,
    metadata?: Record<string, unknown>,
    saveHistory = true,
  ): Promise<ILlmResponse> {
    if (
      !IS_LLM_ENABLED ||
      !this.bedrockCredentials.accessKeyId ||
      !this.bedrockCredentials.secretAccessKey
    ) {
      this.log.error('LLM usage is disabled. Check CROWD_LLM_ENABLED env variable!')
      return
    }

    const settings = LLM_SETTINGS[type]
    if (!settings) {
      throw new Error(`No settings found for LLM query type: ${type}`)
    }

    const client = this.client(settings)

    const start = performance.now()
    const end = () => {
      const end = performance.now()
      const duration = end - start
      return Math.ceil(duration / 1000)
    }

    const command = new InvokeModelCommand({
      body: JSON.stringify({
        messages: [
          {
            role: 'user',
            content: [
              {
                type: 'text',
                text: prompt,
              },
            ],
          },
        ],
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        ...(settings.arguments as any),
      }),
      modelId: settings.modelId,
      accept: 'application/json',
      contentType: 'application/json',
    })

    let res: InvokeModelCommandOutput
    try {
      res = await client.send(command)
    } catch (err) {
      this.log.error(err, { settings, prompt }, 'Failed to query LLM!')
      throw err
    }

    const body = JSON.parse(res.body.transformToString())
    const responseTimeSeconds = end()

    const inputTokenCount = body.usage.input_tokens
    const outputTokenCount = body.usage.output_tokens
    const answer = body.content[0].text

    const pricing = LLM_MODEL_PRICING_MAP[settings.modelId]

    const inputCost = (inputTokenCount / 1000) * pricing.costPer1000InputTokens
    const outputCost = (outputTokenCount / 1000) * pricing.costPer1000OutputTokens
    const totalCost = inputCost + outputCost

    this.log.debug({ type, entityId, inputCost, outputCost, totalCost }, 'Estimated LLM cost!')

    const result = {
      prompt,
      answer,
      inputTokenCount,
      outputTokenCount,
      responseTimeSeconds,
      model: settings.modelId,
    }

    if (saveHistory) {
      try {
        await insertPromptHistoryEntry(this.qx, type, settings.modelId, result, entityId, metadata)
      } catch (err) {
        this.log.error(err, 'Failed to save LLM prompt history entry!')
        throw err
      }
    }

    return result
  }

  public async consolidateMemberEnrichmentData(
    memberId: string,
    prompt: string,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  ): Promise<ILlmResult<LlmMemberEnrichmentResult>> {
    const response = await this.queryLlm(LlmQueryType.MEMBER_ENRICHMENT, prompt, memberId)

    if (!response) {
      return {
        result: null,
      } as ILlmResult<LlmMemberEnrichmentResult>
    }

    const result = parseLlmJson<LlmMemberEnrichmentResult>(response.answer)

    return {
      result,
      ...response,
    }
  }
  public async findRelatedLinkedinProfiles(
    memberId: string,
    prompt: string,
  ): Promise<ILlmResult<{ profileIndex: number }>> {
    const response = await this.queryLlm(
      LlmQueryType.MEMBER_ENRICHMENT_FIND_RELATED_LINKEDIN_PROFILES,
      prompt,
      memberId,
    )

    if (!response) {
      return {
        result: null,
      } as ILlmResult<{ profileIndex: number }>
    }

    const result = parseLlmJson<{ profileIndex: number }>(response.answer)

    return {
      result,
      ...response,
    }
  }

  public async squashMultipleValueAttributes<T>(
    memberId: string,
    prompt: string,
  ): Promise<ILlmResult<T>> {
    const response = await this.queryLlm(
      LlmQueryType.MEMBER_ENRICHMENT_SQUASH_MULTIPLE_VALUE_ATTRIBUTES,
      prompt,
      memberId,
    )

    if (!response) {
      return {
        result: null,
      } as ILlmResult<T>
    }

    const result = parseLlmJson<T>(response.answer)

    return {
      result,
      ...response,
    }
  }

  public async squashWorkExperiencesFromMultipleSources<T>(
    memberId: string,
    prompt: string,
  ): Promise<ILlmResult<T>> {
    const response = await this.queryLlm(
      LlmQueryType.MEMBER_ENRICHMENT_SQUASH_WORK_EXPERIENCES_FROM_MULTIPLE_SOURCES,
      prompt,
      memberId,
    )

    if (!response) {
      return {
        result: null,
      } as ILlmResult<T>
    }

    const result = parseLlmJson<T>(response.answer)

    return {
      result,
      ...response,
    }
  }

  public async findMainGithubOrganization<T>(prompt: string): Promise<ILlmResult<T>> {
    const response = await this.queryLlm(
      LlmQueryType.MATCH_MAIN_GITHUB_ORGANIZATION_AND_DESCRIPTION,
      prompt,
    )

    if (!response) {
      return {
        result: null,
      } as ILlmResult<T>
    }

    const result = parseLlmJson<T>(response.answer)

    return {
      result,
      ...response,
    }
  }

  public async findRepoCategories<T>(prompt: string): Promise<ILlmResult<T>> {
    const response = await this.queryLlm(LlmQueryType.REPO_CATEGORIES, prompt)

    if (!response) {
      return {
        result: null,
      } as ILlmResult<T>
    }

    const result = parseLlmJson<T>(response.answer)

    return {
      result,
      ...response,
    }
  }

  public async findRepoCollections<T>(prompt: string): Promise<ILlmResult<T>> {
    const response = await this.queryLlm(LlmQueryType.REPO_COLLECTIONS, prompt)

    if (!response) {
      return {
        result: null,
      } as ILlmResult<T>
    }

    const result = parseLlmJson<T>(response.answer)

    return {
      result,
      ...response,
    }
  }
}
