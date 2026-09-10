import { getServiceChildLogger } from '@crowd/logging'

import { getAnthropicAwsAgentSdkEnv, getAnthropicPersonalAuthEnv } from './credentials'

const log = getServiceChildLogger('anthropic-aws-agent')

export interface AgentRunResult {
  structuredOutput: Record<string, unknown> | null
  isError: boolean
  errorMessage: string
  numTurns: number
  costUsd: number
}

export interface RunClaudeAgentQueryInput {
  prompt: string
  systemPrompt: string
  cwd: string
  model: string
  schema: Record<string, unknown>
  maxTurns?: number
  timeoutMs?: number
  // Read-only tools auto-allowed instead of bypassing permissions entirely.
  // CLI refuses --dangerously-skip-permissions as root (container standard).
  allowedTools?: string[]
  disallowedTools?: string[]
  // Called on every message to allow Temporal heartbeat without waiting for run completion.
  // Enables activity to signal liveness during up-to-timeoutMs agent call.
  onProgress?: () => void
}

const DEFAULT_ALLOWED_TOOLS = ['Read', 'Grep', 'Glob']
const DEFAULT_DISALLOWED_TOOLS = [
  'Bash',
  'Write',
  'Edit',
  'NotebookEdit',
  'WebFetch',
  'WebSearch',
  'Task',
]

function getErrorMessage(
  isError: boolean,
  result: string | undefined,
  structuredOutput: Record<string, unknown> | null,
): string {
  if (isError) return result || 'Unknown error'
  if (!structuredOutput)
    return `Agent completed without structured output: ${result || 'no result text'}`
  return ''
}

// @anthropic-ai/claude-agent-sdk ships ESM-only; callers may compile to
// CommonJS, so dynamic import is required. Wraps SDK with read-only tool restrictions,
// AWS auth fallback, structured output, and timeout support.
export async function runClaudeAgentQuery(
  input: RunClaudeAgentQueryInput,
): Promise<AgentRunResult> {
  const {
    prompt,
    systemPrompt,
    cwd,
    model,
    schema,
    maxTurns = 15,
    timeoutMs = 600_000,
    allowedTools = DEFAULT_ALLOWED_TOOLS,
    disallowedTools = DEFAULT_DISALLOWED_TOOLS,
    onProgress,
  } = input

  // Build environment: prefer Claude Platform on AWS, then a personal `claude
  // setup-token` OAuth token (local dev), then omit to fall back to local CLI auth.
  let env: NodeJS.ProcessEnv | undefined
  let authMode = 'fallback on local claude code token'
  try {
    env = { ...process.env, ...getAnthropicAwsAgentSdkEnv() }
    authMode = 'claude-platform-on-aws'
  } catch (err) {
    const personalAuthEnv = getAnthropicPersonalAuthEnv()
    if (personalAuthEnv) {
      env = { ...process.env, ...personalAuthEnv }
      authMode = 'personal-oauth-token'
    } else {
      env = undefined
      log.warn({ err }, 'anthropic-aws agent: Claude Platform on AWS not configured, falling back')
    }
  }

  log.info({ authMode }, 'anthropic-aws agent: auth mode for this run')

  const controller = new AbortController()
  const timeoutHandle = setTimeout(() => controller.abort(), timeoutMs)

  try {
    const { query } = await import('@anthropic-ai/claude-agent-sdk')
    const q = query({
      prompt,
      options: {
        systemPrompt,
        cwd,
        model,
        maxTurns,
        tools: allowedTools,
        disallowedTools,
        allowedTools,
        outputFormat: {
          type: 'json_schema',
          schema,
        },
        abortController: controller,
        env,
      },
    })

    let result: AgentRunResult | null = null
    let turns = 0

    for await (const message of q) {
      try {
        onProgress?.()
      } catch (err) {
        log.warn({ err }, 'onProgress callback failed, continuing')
      }

      if (message.type === 'result') {
        turns = message.num_turns ?? 0

        if (message.subtype !== 'success') {
          result = {
            structuredOutput: null,
            isError: true,
            errorMessage: message.errors?.[0] ?? message.subtype ?? 'Unknown error',
            numTurns: turns,
            costUsd: message.total_cost_usd ?? 0,
          }
        } else {
          const structuredOutput = (message.structured_output ?? null) as Record<
            string,
            unknown
          > | null
          result = {
            structuredOutput,
            isError: message.is_error,
            errorMessage: getErrorMessage(message.is_error, message.result, structuredOutput),
            numTurns: turns,
            costUsd: message.total_cost_usd ?? 0,
          }
        }
        break
      }
    }

    if (!result) {
      return {
        structuredOutput: null,
        isError: true,
        errorMessage: 'No result message received from agent',
        numTurns: turns,
        costUsd: 0,
      }
    }

    return result
  } catch (err) {
    const errorMsg = err instanceof Error ? err.message : String(err)
    return {
      structuredOutput: null,
      isError: true,
      errorMessage: errorMsg,
      numTurns: 0,
      costUsd: 0,
    }
  } finally {
    clearTimeout(timeoutHandle)
  }
}
