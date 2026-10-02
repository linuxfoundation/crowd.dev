import { beforeEach, describe, expect, it, vi } from 'vitest'

vi.mock('./verifySignature', () => ({ verifySlackSignature: vi.fn(() => true) }))
vi.mock('@/services/slack/requestClassificationBot', () => ({
  runRequestClassificationBot: vi.fn(async () => undefined),
}))

import { runRequestClassificationBot } from '@/services/slack/requestClassificationBot'

import events from './events'
import { verifySlackSignature } from './verifySignature'

function call(body: object, headers: Record<string, string> = {}) {
  const req = {
    body,
    headers,
    log: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
  } as any
  const res = { sendStatus: vi.fn(), json: vi.fn() } as any
  return events(req, res).then(() => ({ req, res }))
}

const mention = {
  type: 'event_callback',
  event: { type: 'app_mention', text: '<@U0BOT> hi', channel: 'C1', ts: '100.1' },
}

describe('slack events', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.mocked(verifySlackSignature).mockReturnValue(true)
  })

  it('answers the url verification challenge', async () => {
    const { res } = await call({ type: 'url_verification', challenge: 'abc' })

    expect(res.json).toHaveBeenCalledWith({ challenge: 'abc' })
  })

  it('acks and classifies a mention, replying in the message thread', async () => {
    const { res } = await call(mention)

    expect(res.sendStatus).toHaveBeenCalledWith(200)
    expect(runRequestClassificationBot).toHaveBeenCalledWith(
      expect.objectContaining({ text: '<@U0BOT> hi', channelId: 'C1', threadTs: '100.1' }),
    )
  })

  it('replies in the existing thread when the mention is inside one', async () => {
    await call({ ...mention, event: { ...mention.event, thread_ts: '90.0' } })

    expect(runRequestClassificationBot).toHaveBeenCalledWith(
      expect.objectContaining({ threadTs: '90.0' }),
    )
  })

  it('ignores Slack retries, bot messages and other event types', async () => {
    await call(mention, { 'x-slack-retry-num': '1' })
    await call({ ...mention, event: { ...mention.event, bot_id: 'B1' } })
    await call({ ...mention, event: { ...mention.event, type: 'message' } })

    expect(runRequestClassificationBot).not.toHaveBeenCalled()
  })

  it('does nothing for unverified requests', async () => {
    vi.mocked(verifySlackSignature).mockReturnValue(false)

    const { res } = await call(mention)

    expect(res.sendStatus).toHaveBeenCalledWith(200)
    expect(runRequestClassificationBot).not.toHaveBeenCalled()
  })
})
