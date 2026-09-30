import { beforeEach, describe, expect, it, vi } from 'vitest'

vi.mock('./verifySignature', () => ({ verifySlackSignature: vi.fn(() => true) }))
vi.mock('@/services/slack/forceOnboardingCommand', () => ({
  runForceOnboardingCommand: vi.fn(async () => undefined),
}))

import { runForceOnboardingCommand } from '@/services/slack/forceOnboardingCommand'

import interactivity from './interactivity'
import { verifySlackSignature } from './verifySignature'

function call(payload: object) {
  const req = {
    body: { payload: JSON.stringify(payload) },
    log: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
  } as any
  const res = { sendStatus: vi.fn() } as any
  return interactivity(req, res).then(() => ({ req, res }))
}

const forcePayload = {
  type: 'block_actions',
  user: { id: 'U123' },
  response_url: 'https://hooks.slack.com/response',
  actions: [{ action_id: 'force_onboarding', value: 'catalog-1' }],
}

describe('interactivity', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.mocked(verifySlackSignature).mockReturnValue(true)
  })

  it('acks and runs the force onboarding flow with the clicker as actor', async () => {
    const { res } = await call(forcePayload)

    expect(res.sendStatus).toHaveBeenCalledWith(200)
    expect(runForceOnboardingCommand).toHaveBeenCalledWith(
      expect.objectContaining({
        catalogId: 'catalog-1',
        responseUrl: 'https://hooks.slack.com/response',
        actorId: 'U123',
      }),
    )
  })

  it('ignores unknown actions', async () => {
    const { res } = await call({ ...forcePayload, actions: [{ action_id: 'other' }] })

    expect(res.sendStatus).toHaveBeenCalledWith(200)
    expect(runForceOnboardingCommand).not.toHaveBeenCalled()
  })

  it('does nothing for an unverified request', async () => {
    vi.mocked(verifySlackSignature).mockReturnValue(false)

    const { res } = await call(forcePayload)

    expect(res.sendStatus).toHaveBeenCalledWith(200)
    expect(runForceOnboardingCommand).not.toHaveBeenCalled()
  })
})
