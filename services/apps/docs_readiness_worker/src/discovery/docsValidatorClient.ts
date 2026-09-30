// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
import { getAnthropicAwsCredentials } from '@crowd/anthropic-aws'

import type { DocsValidatorClient } from './docsValidator'

// Small and fast: the task is a short classification over a few hundred tokens.
export const DOCS_VALIDATOR_MODEL = 'claude-haiku-4-5-20251001'

const MAX_OUTPUT_TOKENS = 300
const ANTHROPIC_VERSION = '2023-06-01'

interface IMessagesResponse {
  content?: { type: string; text?: string }[]
}

// Original errors can echo request headers (the API key), so only the error name is kept.
function safeError(what: string, err: unknown): Error {
  const name = err instanceof Error ? err.name : 'unknown error'
  return new Error(name === 'AbortError' ? `${what} was aborted` : `${what} failed (${name})`)
}

// Plain fetch: the agent SDK in @crowd/anthropic-aws needs a subprocess and a filesystem.
export function createAnthropicAwsDocsValidatorClient(): DocsValidatorClient {
  return {
    async complete(prompt, signal) {
      const { region, workspaceId, apiKey } = getAnthropicAwsCredentials()
      let response: Response
      try {
        response = await fetch(`https://aws-external-anthropic.${region}.api.aws/v1/messages`, {
          method: 'POST',
          headers: {
            'content-type': 'application/json',
            'anthropic-version': ANTHROPIC_VERSION,
            'anthropic-workspace-id': workspaceId,
            'x-api-key': apiKey,
          },
          body: JSON.stringify({
            model: DOCS_VALIDATOR_MODEL,
            max_tokens: MAX_OUTPUT_TOKENS,
            temperature: 0,
            messages: [{ role: 'user', content: prompt }],
          }),
          signal,
        })
      } catch (err) {
        throw safeError('Anthropic request', err)
      }
      if (!response.ok) {
        throw new Error(`Anthropic API responded with HTTP ${response.status}`)
      }
      let body: IMessagesResponse
      try {
        body = (await response.json()) as IMessagesResponse
      } catch (err) {
        throw safeError('Anthropic response parse', err)
      }
      return (body.content ?? [])
        .filter((block) => block.type === 'text')
        .map((block) => block.text ?? '')
        .join('')
    },
  }
}
