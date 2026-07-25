// @anthropic-ai/claude-agent-sdk ships ESM-only; packages_worker compiles to
// CommonJS, so it must be loaded via dynamic import rather than a static one.

// Agent runner wrapping Claude Agent SDK with read-only tool restrictions,
// API key fallback, structured output, and timeout support.

export interface AgentRunResult {
  structuredOutput: Record<string, unknown> | null
  isError: boolean
  errorMessage: string
  numTurns: number
  costUsd: number
}

export interface RunAnalysisAgentInput {
  prompt: string
  systemPrompt: string
  cwd: string
  model: string
  schema: Record<string, unknown>
  maxTurns?: number
  timeoutMs?: number
  // Called on every streamed message (i.e. at least once per agent turn), so a
  // caller can heartbeat a Temporal activity during a single up-to-timeoutMs
  // agent call rather than only once the whole call returns.
  onProgress?: () => void
}

export async function runAnalysisAgent(input: RunAnalysisAgentInput): Promise<AgentRunResult> {
  const {
    prompt,
    systemPrompt,
    cwd,
    model,
    schema,
    maxTurns = 15,
    timeoutMs = 600_000,
    onProgress,
  } = input

  const apiKey = process.env.BLAST_RADIUS_ANTHROPIC_API_KEY
  const baseUrl = process.env.BLAST_RADIUS_ANTHROPIC_BASE_URL

  // Build environment: if API key is set, pass it (and an optional base URL override,
  // e.g. for routing through a LiteLLM proxy); otherwise omit to fall back to CLI auth.
  const env = apiKey
    ? {
        ...process.env,
        ANTHROPIC_API_KEY: apiKey,
        ...(baseUrl ? { ANTHROPIC_BASE_URL: baseUrl } : {}),
      }
    : undefined

  // Setup timeout via AbortController
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
        tools: ['Read', 'Grep', 'Glob'],
        disallowedTools: ['Bash', 'Write', 'Edit', 'NotebookEdit', 'WebFetch', 'WebSearch', 'Task'],
        // Read/Grep/Glob are read-only — auto-allow them instead of bypassing permissions
        // entirely. bypassPermissions requires --dangerously-skip-permissions, which the
        // Claude Code CLI refuses to run as root — the exact setup packages_worker's
        // container runs under.
        allowedTools: ['Read', 'Grep', 'Glob'],
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
      onProgress?.()

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
            errorMessage: message.is_error
              ? message.result || 'Unknown error'
              : !structuredOutput
                ? `Agent completed without structured output: ${message.result || 'no result text'}`
                : '',
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
