import { beforeEach, describe, expect, it, vi } from 'vitest'

vi.mock('@crowd/slack', () => ({
  SlackChannel: { CDP_PROJECT_CATALOG_SKIP_ALERTS: 'cdp-project-catalog-skip-alerts' },
  SlackPersona: { WARNING_PROPAGATOR: 'WARNING_PROPAGATOR', ERROR_REPORTER: 'ERROR_REPORTER' },
  sendSlackNotificationAsync: vi.fn(),
}))

import { sendSlackNotificationAsync } from '@crowd/slack'

import { buildSlackBotRequestAlert, notifySlackBotRequest } from './slackBotRequestAlert'

const project = {
  repoName: 'bar',
  repoUrl: 'https://github.com/foo/bar',
  sourceUrl: 'https://acme.slack.com/archives/C1/p1',
}

describe('buildSlackBotRequestAlert', () => {
  it('links the originating Slack message and names the requester', () => {
    const [header, reason] = buildSlackBotRequestAlert('skipped', project, {
      reason: 'already in CDP',
      actorId: 'U123',
    })

    expect(header.text).toContain('*bar*')
    expect(header.text).toContain('https://github.com/foo/bar')
    expect(header.text).toContain('<https://acme.slack.com/archives/C1/p1|Slack request>')
    expect(header.text).toContain('<@U123>')
    expect(reason.title).toBe('Skip reason')
    expect(reason.text).toContain('already in CDP')
  })

  it('falls back when the source message was not recorded', () => {
    const [header] = buildSlackBotRequestAlert(
      'errored',
      { ...project, sourceUrl: null },
      { reason: 'boom', actorId: 'U123' },
    )

    expect(header.text).toContain('_source Slack message not recorded_')
  })

  it('truncates long reasons', () => {
    const [, reason] = buildSlackBotRequestAlert('errored', project, {
      reason: 'x'.repeat(600),
      actorId: 'U123',
    })

    expect(reason.title).toBe('Error')
    expect(reason.text).toContain(`${'x'.repeat(500)}…`)
    expect(reason.text).not.toContain('x'.repeat(501))
  })
})

describe('notifySlackBotRequest', () => {
  const log = { warn: vi.fn() } as any

  beforeEach(() => {
    vi.mocked(sendSlackNotificationAsync).mockReset()
    log.warn.mockReset()
  })

  it('sends skipped alerts as a warning', async () => {
    await notifySlackBotRequest('skipped', project, { reason: 'r', actorId: 'U1' }, log)

    expect(sendSlackNotificationAsync).toHaveBeenCalledWith(
      'cdp-project-catalog-skip-alerts',
      'WARNING_PROPAGATOR',
      'Skipped — bar',
      expect.any(Array),
    )
  })

  it('sends errored alerts as an error', async () => {
    await notifySlackBotRequest('errored', project, { reason: 'r', actorId: 'U1' }, log)

    expect(sendSlackNotificationAsync).toHaveBeenCalledWith(
      'cdp-project-catalog-skip-alerts',
      'ERROR_REPORTER',
      'Onboarding failed — bar',
      expect.any(Array),
    )
  })

  it('never throws when sending fails', async () => {
    vi.mocked(sendSlackNotificationAsync).mockRejectedValue(new Error('down'))

    await expect(
      notifySlackBotRequest('skipped', project, { reason: 'r', actorId: 'U1' }, log),
    ).resolves.toBeUndefined()
    expect(log.warn).toHaveBeenCalled()
  })
})
