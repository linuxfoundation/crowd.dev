import { BedrockRuntimeClient, InvokeModelCommand } from '@aws-sdk/client-bedrock-runtime'

import { parseLlmJson } from '@crowd/common'
import { getServiceChildLogger } from '@crowd/logging'
import { LLM_MODEL_REGION_MAP, LlmModelType } from '@crowd/types'

import { ParsedProtocol } from './types'

const log = getServiceChildLogger('reporting-protocol:llm')

const MAX_TOKENS = 4096
const DEFAULT_REGION = 'us-east-1'
const MAX_INPUT_CHARS = 100_000

// Claude Haiku 4.5 rates ($/token). Adjust if the model or Bedrock pricing changes.
const USD_PER_INPUT_TOKEN = 1 / 1_000_000
const USD_PER_OUTPUT_TOKEN = 5 / 1_000_000

const PROTOCOL_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  required: ['methods', 'guidelines'],
  properties: {
    methods: {
      type: 'array',
      items: {
        type: 'object',
        additionalProperties: false,
        required: ['type', 'status', 'endpoint', 'condition'],
        properties: {
          type: {
            type: 'string',
            enum: [
              'github-pvr',
              'email',
              'web-form',
              'bounty-platform',
              'security-txt',
              'mailing-list',
            ],
          },
          status: { type: 'string', enum: ['preferred', 'accepted', 'fallback', 'prohibited'] },
          endpoint: { type: 'string' },
          condition: { type: ['string', 'null'] },
        },
      },
    },
    guidelines: {
      type: ['object', 'null'],
      additionalProperties: false,
      required: ['generalPrinciples', 'avoid', 'recommend'],
      properties: {
        generalPrinciples: { type: 'array', items: { type: 'string' } },
        avoid: { type: 'array', items: { type: 'string' } },
        recommend: {
          type: 'array',
          items: {
            type: 'object',
            additionalProperties: false,
            required: ['scenario', 'action'],
            properties: { scenario: { type: 'string' }, action: { type: 'string' } },
          },
        },
      },
    },
  },
}

const SYSTEM_PROMPT = `You extract a project's declared vulnerability-reporting protocol from its security policy text.
Rules:
- Only emit methods the text actually declares. Never invent endpoints; copy them exactly as written (deobfuscate "name at host dot tld" into a plain email address).
- For github-pvr and security-txt methods, use the literal endpoint sentinels "github-pvr" and "security.txt".
- status: "preferred" for the method the project asks for first (at most one), "accepted" for other welcomed methods, "fallback" for methods offered only when others fail, "prohibited" for channels the text tells reporters NOT to use.
- condition: the verbatim-ish clause gating a method (e.g. "only if opening a GHSA is not possible"), else null.
- guidelines: distill reporting expectations into generalPrinciples / avoid / recommend. Use null when the text has none.
- Output nothing but the JSON.`

export interface LlmExtractConfig {
  modelId: string
  timeoutMs: number
  accessKeyId: string | undefined
  secretAccessKey: string | undefined
}

export interface LlmExtractResult {
  parsed: ParsedProtocol | null
  costUsd: number | null
}

const clients = new Map<string, BedrockRuntimeClient>()

function clientFor(cfg: LlmExtractConfig, region: string): BedrockRuntimeClient {
  const key = `${region}:${cfg.accessKeyId}`
  let client = clients.get(key)
  if (!client) {
    client = new BedrockRuntimeClient({
      region,
      credentials: {
        accessKeyId: cfg.accessKeyId as string,
        secretAccessKey: cfg.secretAccessKey as string,
      },
    })
    clients.set(key, client)
  }
  return client
}

function usageCostUsd(body: unknown): number | null {
  const usage = (body as { usage?: { input_tokens?: number; output_tokens?: number } })?.usage
  if (!usage) return null
  return (
    (usage.input_tokens ?? 0) * USD_PER_INPUT_TOKEN +
    (usage.output_tokens ?? 0) * USD_PER_OUTPUT_TOKEN
  )
}

export async function llmExtractProtocol(
  text: string,
  cfg: LlmExtractConfig,
): Promise<LlmExtractResult> {
  if (!cfg.accessKeyId || !cfg.secretAccessKey) {
    log.warn({ modelId: cfg.modelId }, 'Missing Bedrock credentials — skipping LLM extraction')
    return { parsed: null, costUsd: null }
  }

  const region = LLM_MODEL_REGION_MAP[cfg.modelId as LlmModelType] ?? DEFAULT_REGION
  const controller = new AbortController()
  const timeoutHandle = setTimeout(() => controller.abort(), cfg.timeoutMs)
  try {
    const command = new InvokeModelCommand({
      modelId: cfg.modelId,
      accept: 'application/json',
      contentType: 'application/json',
      body: JSON.stringify({
        anthropic_version: 'bedrock-2023-05-31',
        max_tokens: MAX_TOKENS,
        system: `${SYSTEM_PROMPT}\n\nRespond ONLY with a single JSON object matching this JSON schema, no prose:\n${JSON.stringify(PROTOCOL_SCHEMA)}`,
        messages: [
          { role: 'user', content: [{ type: 'text', text: text.slice(0, MAX_INPUT_CHARS) }] },
        ],
      }),
    })
    const res = await clientFor(cfg, region).send(command, { abortSignal: controller.signal })
    const body = JSON.parse(res.body.transformToString())
    const costUsd = usageCostUsd(body)
    const answer: string | undefined = body?.content?.[0]?.text
    if (!answer) return { parsed: null, costUsd }
    try {
      return { parsed: parseLlmJson<ParsedProtocol>(answer), costUsd }
    } catch (err) {
      log.warn(
        { errMsg: (err as Error).message, modelId: cfg.modelId },
        'LLM protocol extraction failed',
      )
      return { parsed: null, costUsd }
    }
  } catch (err) {
    log.warn(
      { errMsg: (err as Error).message, modelId: cfg.modelId },
      'LLM protocol extraction failed',
    )
    return { parsed: null, costUsd: null }
  } finally {
    clearTimeout(timeoutHandle)
  }
}
