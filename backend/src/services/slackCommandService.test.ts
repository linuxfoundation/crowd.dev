import { describe, expect, it, vi } from 'vitest'

vi.mock('./slack/onboardProjectCommand', () => ({
  runOnboardProjectCommand: vi.fn(),
  postToResponseUrl: vi.fn(),
  textMessage: vi.fn((text: string) => ({ blocks: [{ text: { text } }] })),
}))

import { postToResponseUrl, runOnboardProjectCommand } from './slack/onboardProjectCommand'
import SlackCommandService from './slackCommandService'

function flushMicrotasks() {
  return new Promise((resolve) => setImmediate(resolve))
}

describe('SlackCommandService.onboardProject', () => {
  it('posts a Slack failure message when runOnboardProjectCommand throws unexpectedly', async () => {
    const log = { error: vi.fn(), warn: vi.fn() }
    vi.mocked(runOnboardProjectCommand).mockRejectedValue(new Error('boom'))

    const service = new SlackCommandService({ log } as any)

    await service.onboardProject({ repoUrl: 'https://github.com/foo/bar' }, {
      responseUrl: 'https://hooks.slack.com/response',
      userId: 'U123',
    } as any)
    await flushMicrotasks()

    expect(log.error).toHaveBeenCalled()
    expect(postToResponseUrl).toHaveBeenCalledWith(
      'https://hooks.slack.com/response',
      expect.anything(),
      log,
    )
  })
})
