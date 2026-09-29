import { createHmac } from 'crypto'

import type { Request } from 'express'
import { describe, expect, it, vi } from 'vitest'

vi.mock('@crowd/slack', () => ({
  getSlackBotConfig: vi.fn(() => ({ signingSecret: 'test-signing-secret' })),
}))

import { verifySlackSignature } from './verifySignature'

function signedRequest(rawBody: string, timestamp: number): Request {
  const signature = `v0=${createHmac('sha256', 'test-signing-secret')
    .update(`v0:${timestamp}:${rawBody}`)
    .digest('hex')}`

  return {
    headers: {
      'x-slack-request-timestamp': String(timestamp),
      'x-slack-signature': signature,
    },
    rawBody: Buffer.from(rawBody),
  } as unknown as Request
}

describe('verifySlackSignature', () => {
  it('accepts a request signed with the correct secret and a fresh timestamp', () => {
    const req = signedRequest('payload=%7B%7D', Math.floor(Date.now() / 1000))
    expect(verifySlackSignature(req)).toBe(true)
  })

  it('rejects a request with a tampered signature', () => {
    const req = signedRequest('payload=%7B%7D', Math.floor(Date.now() / 1000))
    req.headers['x-slack-signature'] = 'v0=deadbeef'
    expect(verifySlackSignature(req)).toBe(false)
  })

  it('rejects a stale request (replay protection)', () => {
    const staleTimestamp = Math.floor(Date.now() / 1000) - 600
    const req = signedRequest('payload=%7B%7D', staleTimestamp)
    expect(verifySlackSignature(req)).toBe(false)
  })

  it('rejects a request missing required headers', () => {
    expect(
      verifySlackSignature({ headers: {}, rawBody: Buffer.from('') } as unknown as Request),
    ).toBe(false)
  })
})
