import { beforeEach, describe, expect, it, vi } from 'vitest'

import { inferRepositoryCandidates } from '../llmRepoInference'

const { log } = vi.hoisted(() => ({ log: { info: vi.fn(), warn: vi.fn(), error: vi.fn() } }))

vi.mock('@crowd/logging', () => ({ getServiceChildLogger: () => log }))

type ResponseBlock = { type: 'thinking'; thinking: string } | { type: 'text'; text: string }

const usage = { inputTokens: 300, outputTokens: 80, costUsd: 300 * 0.000002 + 80 * 0.00001 }
const twoCandidates = [
  { repositoryUrl: 'https://github.com/haskell/aeson', confidence: 0.95 },
  { repositoryUrl: 'https://github.com/bos/aeson', confidence: 0.4 },
]
const fourCandidates = [1, 2, 3, 4].map((n) => ({
  repositoryUrl: `https://github.com/o/r${n}`,
  confidence: 0.9,
}))

function bedrockResponse(text: string | null, stopReason = 'end_turn'): string {
  const content: ResponseBlock[] = [{ type: 'thinking', thinking: '' }]
  if (text !== null) content.push({ type: 'text', text })
  return JSON.stringify({
    content,
    usage: { input_tokens: 300, output_tokens: 80 },
    stop_reason: stopReason,
  })
}

function answer(candidates: unknown[], rationale = ''): string {
  return JSON.stringify({ candidates, rationale })
}

function invokeReturning(response: string) {
  return vi.fn().mockResolvedValue(response)
}

beforeEach(() => {
  vi.clearAllMocks()
})

describe('inferRepositoryCandidates', () => {
  it('sends the purl to sonnet 5.5, parses the first text block and logs the call', async () => {
    const invoke = invokeReturning(
      bedrockResponse(answer(twoCandidates, 'aeson moved to the haskell org')),
    )

    const result = await inferRepositoryCandidates('pkg:hackage/aeson', invoke)

    expect(invoke).toHaveBeenCalledTimes(1)
    const [modelId, body] = invoke.mock.calls[0]
    expect(modelId).toBe('us.anthropic.claude-sonnet-5-5')
    expect(JSON.parse(body)).toEqual({
      anthropic_version: 'bedrock-2023-05-31',
      max_tokens: 2048,
      output_config: { effort: 'low' },
      system: expect.stringContaining('canonical source repository'),
      messages: [{ role: 'user', content: [{ type: 'text', text: 'pkg:hackage/aeson' }] }],
    })
    expect(result).toEqual({ status: 'answered', candidates: twoCandidates, usage })
    expect(log.info.mock.calls).toEqual([
      [
        {
          modelId: 'us.anthropic.claude-sonnet-5-5',
          purl: 'pkg:hackage/aeson',
          stopReason: 'end_turn',
          inputTokens: 300,
          outputTokens: 80,
          costUsd: usage.costUsd,
          candidates: 2,
          durationMs: expect.any(Number),
        },
        'LLM repo inference call',
      ],
    ])
  })

  it.each([
    [
      'a fenced answer, capped at three candidates',
      bedrockResponse('```json\n' + answer(fourCandidates) + '\n```'),
      { status: 'answered', candidates: fourCandidates.slice(0, 3), usage },
    ],
    [
      'an empty candidate list',
      bedrockResponse(answer([], 'unknown')),
      { status: 'answered', candidates: [], usage },
    ],
    [
      'a response without a text block',
      bedrockResponse(null),
      { status: 'unparseable', reason: 'LLM answer had no text content', usage },
    ],
    [
      'an answer that is not JSON',
      bedrockResponse('I think it is github.com/haskell/aeson'),
      { status: 'unparseable', reason: 'LLM answer was not valid JSON', usage },
    ],
    [
      'an answer outside the schema',
      bedrockResponse(answer([{ repositoryUrl: 'https://github.com/a/b', confidence: 1.7 }])),
      { status: 'unparseable', reason: 'LLM answer did not match the expected schema', usage },
    ],
    [
      'a refusal',
      bedrockResponse(null, 'refusal'),
      { status: 'unparseable', reason: 'LLM refused to answer', usage },
    ],
    [
      'an answer truncated at max_tokens',
      bedrockResponse('{"candidates": [', 'max_tokens'),
      { status: 'truncated', usage },
    ],
  ])('returns and logs the result with usage for %s', async (_label, response, expected) => {
    await expect(
      inferRepositoryCandidates('pkg:conan/x', invokeReturning(response)),
    ).resolves.toEqual(expected)
    expect(log.info.mock.calls).toEqual([
      [
        expect.objectContaining({
          stopReason: JSON.parse(response).stop_reason,
          inputTokens: 300,
          outputTokens: 80,
        }),
        'LLM repo inference call',
      ],
    ])
  })

  it.each([
    ['usage is missing', undefined],
    ['usage is non-numeric', { input_tokens: '300', output_tokens: 80 }],
    ['usage is partial', { input_tokens: 300 }],
  ])('rejects a response whose %s', async (_label, responseUsage) => {
    const invoke = invokeReturning(
      JSON.stringify({
        content: [{ type: 'text', text: '{}' }],
        usage: responseUsage,
        stop_reason: 'end_turn',
      }),
    )
    await expect(inferRepositoryCandidates('pkg:conan/x', invoke)).rejects.toThrow(
      'Bedrock response had no token usage',
    )
  })

  it('propagates transport errors', async () => {
    const invoke = vi.fn().mockRejectedValue(new Error('ThrottlingException'))
    await expect(inferRepositoryCandidates('pkg:conan/x', invoke)).rejects.toThrow(
      'ThrottlingException',
    )
  })
})
