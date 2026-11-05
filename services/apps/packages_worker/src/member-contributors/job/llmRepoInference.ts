import { z } from 'zod'

import { parseLlmJson } from '@crowd/common'
import type { LlmUsage } from '@crowd/data-access-layer/src/osspckgs/memberContributorJobs'
import { getServiceChildLogger } from '@crowd/logging'
import { LlmModelType, estimateLlmCostUsd } from '@crowd/types'

import type { BedrockInvoke } from '../../llm/bedrockClient'

const log = getServiceChildLogger('member-contributors:llm-repo-inference')

export const REPO_INFERENCE_MODEL_ID = LlmModelType.CLAUDE_SONNET_5_5
export const MAX_REPO_CANDIDATES = 3
const MAX_TOKENS = 2048
export const TRUNCATED_ANSWER = 'LLM answer truncated at max_tokens'

const candidateSchema = z.object({
  repositoryUrl: z.string().min(1),
  confidence: z.number().min(0).max(1),
})
const answerSchema = z.object({
  candidates: z.array(candidateSchema),
  rationale: z.string().optional(),
})

const ANSWER_JSON_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  required: ['candidates', 'rationale'],
  properties: {
    candidates: {
      type: 'array',
      maxItems: MAX_REPO_CANDIDATES,
      items: {
        type: 'object',
        additionalProperties: false,
        required: ['repositoryUrl', 'confidence'],
        properties: {
          repositoryUrl: { type: 'string' },
          confidence: { type: 'number', minimum: 0, maximum: 1 },
        },
      },
    },
    rationale: { type: 'string' },
  },
}

const SYSTEM_PROMPT = `You resolve a software package, given as a package URL (purl, e.g. pkg:hackage/aeson or pkg:conan/boost), to its canonical source repository.
Rules:
- Answer only from what you know. Never invent a URL you have not seen.
- Return up to ${MAX_REPO_CANDIDATES} candidate GitHub repository URLs ranked by likelihood, each with a confidence between 0 and 1 that it is the package's canonical upstream source repository.
- Prefer the upstream project over forks, mirrors, packaging repositories and distribution recipes.
- If you do not know the repository, return an empty candidates list.
- rationale: one short sentence.
Respond ONLY with a single JSON object matching this JSON schema, no prose:
${JSON.stringify(ANSWER_JSON_SCHEMA)}`

export interface RepoCandidate {
  repositoryUrl: string
  confidence: number
}

export type RepoInference =
  | { status: 'answered'; candidates: RepoCandidate[]; usage: LlmUsage }
  | { status: 'unparseable'; reason: string; usage: LlmUsage }
  | { status: 'truncated'; usage: LlmUsage }

interface BedrockMessageResponse {
  content?: Array<{ type?: string; text?: string }>
  usage?: { input_tokens?: unknown; output_tokens?: unknown }
  stop_reason?: string
}

function requestBody(purl: string): string {
  return JSON.stringify({
    anthropic_version: 'bedrock-2023-05-31',
    max_tokens: MAX_TOKENS,
    output_config: { effort: 'low' },
    system: SYSTEM_PROMPT,
    messages: [{ role: 'user', content: [{ type: 'text', text: purl }] }],
  })
}

function usageOf(response: BedrockMessageResponse): LlmUsage {
  const inputTokens = response.usage?.input_tokens
  const outputTokens = response.usage?.output_tokens
  if (typeof inputTokens !== 'number' || typeof outputTokens !== 'number') {
    throw new Error('Bedrock response had no token usage')
  }
  return {
    inputTokens,
    outputTokens,
    costUsd: estimateLlmCostUsd(REPO_INFERENCE_MODEL_ID, inputTokens, outputTokens),
  }
}

function firstText(response: BedrockMessageResponse): string | null {
  const block = response.content?.find(
    (entry) => entry.type === 'text' && typeof entry.text === 'string',
  )
  return block?.text ?? null
}

function candidatesOf(response: BedrockMessageResponse): RepoCandidate[] | string {
  if (response.stop_reason === 'max_tokens') {
    return TRUNCATED_ANSWER
  }
  if (response.stop_reason === 'refusal') {
    return 'LLM refused to answer'
  }
  const text = firstText(response)
  return text === null ? 'LLM answer had no text content' : parseAnswer(text)
}

function parseAnswer(text: string): RepoCandidate[] | string {
  let raw: unknown
  try {
    raw = parseLlmJson<unknown>(text)
  } catch {
    return 'LLM answer was not valid JSON'
  }
  const parsed = answerSchema.safeParse(raw)
  if (!parsed.success) {
    return 'LLM answer did not match the expected schema'
  }
  return parsed.data.candidates.slice(0, MAX_REPO_CANDIDATES)
}

export async function inferRepositoryCandidates(
  purl: string,
  invoke: BedrockInvoke,
): Promise<RepoInference> {
  const startedAt = Date.now()
  const response = JSON.parse(
    await invoke(REPO_INFERENCE_MODEL_ID, requestBody(purl)),
  ) as BedrockMessageResponse
  const usage = usageOf(response)
  const candidates = candidatesOf(response)
  log.info(
    {
      modelId: REPO_INFERENCE_MODEL_ID,
      purl,
      stopReason: response.stop_reason,
      inputTokens: usage.inputTokens,
      outputTokens: usage.outputTokens,
      costUsd: usage.costUsd,
      candidates: typeof candidates === 'string' ? 0 : candidates.length,
      durationMs: Date.now() - startedAt,
    },
    'LLM repo inference call',
  )
  if (candidates === TRUNCATED_ANSWER) {
    return { status: 'truncated', usage }
  }
  if (typeof candidates === 'string') {
    return { status: 'unparseable', reason: candidates, usage }
  }
  return { status: 'answered', candidates, usage }
}
