import { beforeEach, describe, expect, it, vi } from 'vitest'

vi.mock('@crowd/slack', () => ({
  getSlackPermalink: vi.fn(async () => 'https://slack.test/permalink'),
  postSlackMessage: vi.fn(async () => ({ ok: true })),
}))
vi.mock('@crowd/project-onboarding/src/requestClassifierDeps', () => ({
  withRequestClassifierDeps: vi.fn(async () => ({
    resolution: { kind: 'lf_not_in_pcc', projectName: 'Acme' },
    node: 'lf_not_in_pcc_flag_human',
    trace: { parsed: null, pccLookup: null, cdpLookup: null, failure: null },
  })),
}))
vi.mock('./slackBackground', () => ({ getBgQx: vi.fn(async () => ({})) }))

import { getSlackPermalink, postSlackMessage } from '@crowd/slack'

import { runRequestClassificationBot } from './requestClassificationBot'

const log = { info: vi.fn(), warn: vi.fn(), error: vi.fn() } as any

function run(overrides: Record<string, unknown> = {}) {
  return runRequestClassificationBot({
    text: '<@U0BOT> onboard Acme',
    channelId: 'C1',
    messageTs: '100.1',
    threadTs: '90.0',
    options: { log },
    ...overrides,
  })
}

describe('runRequestClassificationBot', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.mocked(postSlackMessage).mockResolvedValue({ ok: true })
  })

  it('links the mentioned message and replies in the thread', async () => {
    await run()

    expect(getSlackPermalink).toHaveBeenCalledWith('C1', '100.1')
    expect(postSlackMessage).toHaveBeenCalledWith(
      expect.objectContaining({ channel: 'C1', thread_ts: '90.0' }),
    )
    expect(log.warn).not.toHaveBeenCalled()
  })

  it('warns when Slack does not deliver the reply', async () => {
    vi.mocked(postSlackMessage).mockResolvedValue({ ok: false, error: 'channel_not_found' })

    await run()

    expect(log.warn).toHaveBeenCalledWith(
      expect.objectContaining({ channelId: 'C1', error: 'channel_not_found' }),
      'Slack bot reply was not delivered.',
    )
  })

  it('asks for the request details when only the mention is left', async () => {
    await run({ text: '<@U0BOT>' })

    expect(getSlackPermalink).not.toHaveBeenCalled()
    expect(postSlackMessage).toHaveBeenCalledWith(
      expect.objectContaining({ text: expect.stringContaining('Tell me about the project') }),
    )
  })
})
