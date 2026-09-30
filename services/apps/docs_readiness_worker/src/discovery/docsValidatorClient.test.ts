// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { validateDocsUrl } from './docsValidator'
import { DOCS_VALIDATOR_MODEL, createAnthropicAwsDocsValidatorClient } from './docsValidatorClient'

const ENV_NAMES = [
  'CROWD_AKRITES_ANTHROPIC_AWS_REGION',
  'CROWD_AKRITES_ANTHROPIC_AWS_WORKSPACE_ID',
  'CROWD_AKRITES_ANTHROPIC_AWS_API_KEY',
]
const API_KEY = 'test-key-not-a-real-secret'

const project = { name: 'Agones' }
const page = { finalUrl: 'https://agones.dev/', status: 200 }

function setEnv() {
  process.env.CROWD_AKRITES_ANTHROPIC_AWS_REGION = 'us-west-2'
  process.env.CROWD_AKRITES_ANTHROPIC_AWS_WORKSPACE_ID = 'wrkspc_test'
  process.env.CROWD_AKRITES_ANTHROPIC_AWS_API_KEY = API_KEY
}

describe('createAnthropicAwsDocsValidatorClient', () => {
  const saved = ENV_NAMES.map((name) => [name, process.env[name]] as const)

  beforeEach(() => {
    setEnv()
  })

  afterEach(() => {
    vi.unstubAllGlobals()
    for (const [name, value] of saved) {
      if (value === undefined) delete process.env[name]
      else process.env[name] = value
    }
  })

  it('posts one message to the Claude Platform on AWS Messages endpoint', async () => {
    const fetchMock = vi.fn(
      async () =>
        new Response(
          JSON.stringify({ content: [{ type: 'text', text: '{"verdict":"other","reason":"x"}' }] }),
        ),
    )
    vi.stubGlobal('fetch', fetchMock)

    const reply = await createAnthropicAwsDocsValidatorClient().complete('the prompt')

    expect(reply).toBe('{"verdict":"other","reason":"x"}')
    const [url, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit]
    expect(url).toBe('https://aws-external-anthropic.us-west-2.api.aws/v1/messages')
    expect(init.method).toBe('POST')
    expect(init.headers).toMatchObject({
      'x-api-key': API_KEY,
      'anthropic-workspace-id': 'wrkspc_test',
      'anthropic-version': '2023-06-01',
    })
    expect(JSON.parse(init.body as string)).toMatchObject({
      model: DOCS_VALIDATOR_MODEL,
      max_tokens: 300,
      temperature: 0,
      messages: [{ role: 'user', content: 'the prompt' }],
    })
  })

  it('passes the abort signal to fetch', async () => {
    const fetchMock = vi.fn(async () => new Response(JSON.stringify({ content: [] })))
    vi.stubGlobal('fetch', fetchMock)
    const controller = new AbortController()

    await createAnthropicAwsDocsValidatorClient().complete('the prompt', controller.signal)

    const [, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit]
    expect(init.signal).toBe(controller.signal)
  })

  it('returns an empty reply for an empty content array', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => new Response(JSON.stringify({ content: [] }))),
    )
    expect(await createAnthropicAwsDocsValidatorClient().complete('p')).toBe('')
  })

  it('ignores non-text content blocks', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(
        async () =>
          new Response(
            JSON.stringify({
              content: [
                { type: 'thinking', thinking: 'hmm' },
                { type: 'text', text: 'answer' },
              ],
            }),
          ),
      ),
    )
    expect(await createAnthropicAwsDocsValidatorClient().complete('p')).toBe('answer')
  })

  it('returns unclear when a 200 response has a malformed JSON body', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => new Response('not json {')),
    )

    const result = await validateDocsUrl(createAnthropicAwsDocsValidatorClient(), project, page)

    expect(result).toEqual({
      verdict: 'unclear',
      reason: 'validator failed: Anthropic response parse failed (SyntaxError)',
    })
  })

  it('returns unclear without calling the API when credentials are missing', async () => {
    delete process.env.CROWD_AKRITES_ANTHROPIC_AWS_API_KEY
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)

    const result = await validateDocsUrl(createAnthropicAwsDocsValidatorClient(), project, page)

    expect(result.verdict).toBe('unclear')
    expect(result.reason).toContain('CROWD_AKRITES_ANTHROPIC_AWS_API_KEY')
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('returns unclear on an HTTP error without leaking the key', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => new Response(`bad key ${API_KEY}`, { status: 401 })),
    )

    const result = await validateDocsUrl(createAnthropicAwsDocsValidatorClient(), project, page)

    expect(result.verdict).toBe('unclear')
    expect(result.reason).toContain('HTTP 401')
    expect(result.reason).not.toContain(API_KEY)
    expect(result.reason).not.toContain('wrkspc_test')
  })

  it('does not leak a malformed key that makes fetch throw', async () => {
    process.env.CROWD_AKRITES_ANTHROPIC_AWS_API_KEY = 'SECRETKEY\nTAIL'

    const result = await validateDocsUrl(createAnthropicAwsDocsValidatorClient(), project, page)

    expect(result.verdict).toBe('unclear')
    expect(result.reason).toMatch(/^validator failed: Anthropic request failed \(\w+\)$/)
    for (const secret of ['SECRETKEY', 'TAIL', 'wrkspc_test']) {
      expect(result.reason).not.toContain(secret)
    }
  })

  it('returns unclear on a network failure', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => {
        throw new TypeError('fetch failed')
      }),
    )

    const result = await validateDocsUrl(createAnthropicAwsDocsValidatorClient(), project, page)

    expect(result).toEqual({
      verdict: 'unclear',
      reason: 'validator failed: Anthropic request failed (TypeError)',
    })
  })
})
