import axios from 'axios'
import { beforeEach, describe, expect, it, vi } from 'vitest'

vi.mock('axios', () => ({
  default: { post: vi.fn(async () => undefined) },
}))

vi.mock('@crowd/data-access-layer', () => ({
  claimProjectCatalogForForcedOnboarding: vi.fn(),
  updateProjectCatalog: vi.fn(async () => ({})),
  isSlackPermalink: vi.fn((url: string) => url.includes('slack.com/archives')),
}))

vi.mock('@crowd/project-onboarding', () => ({
  onboardProject: vi.fn(),
}))

vi.mock('./slackBackground', () => ({
  getBgQx: vi.fn(async () => ({})),
  reserveDailyProjectOnboardingRequest: vi.fn(),
}))

vi.mock('./slackBotRequestAlert', () => ({
  notifySlackBotRequest: vi.fn(async () => undefined),
}))

import {
  claimProjectCatalogForForcedOnboarding,
  updateProjectCatalog,
} from '@crowd/data-access-layer'
import { onboardProject } from '@crowd/project-onboarding'

import { runForceOnboardingCommand } from './forceOnboardingCommand'
import { reserveDailyProjectOnboardingRequest } from './slackBackground'
import { notifySlackBotRequest } from './slackBotRequestAlert'

const catalogId = '5f0c2f4e-6a51-4c5d-9d63-3f1d1f5a3a11'
const claimed = {
  id: catalogId,
  repoUrl: 'https://github.com/foo/bar',
  repoName: 'bar',
  projectSlug: 'foo/bar',
  sourceUrl: 'https://lfx.slack.com/archives/C1/p1',
}

const log = { warn: vi.fn(), error: vi.fn() } as any
const run = (id = catalogId) =>
  runForceOnboardingCommand({
    catalogId: id,
    responseUrl: 'https://hooks.slack.com/response',
    actorId: 'U123',
    log,
  })

function lastSlackMessage(): any {
  const calls = vi.mocked(axios.post).mock.calls
  return calls[calls.length - 1][1]
}

describe('runForceOnboardingCommand', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.mocked(claimProjectCatalogForForcedOnboarding).mockResolvedValue(claimed as any)
    vi.mocked(onboardProject).mockResolvedValue({ outcome: 'onboarded' } as any)
  })

  it('onboards the repo and replaces the original message', async () => {
    await run()

    expect(onboardProject).toHaveBeenCalledWith(
      expect.objectContaining({ repoUrl: claimed.repoUrl, projectSlug: claimed.projectSlug }),
    )
    expect(updateProjectCatalog).toHaveBeenCalledWith(
      expect.anything(),
      catalogId,
      expect.objectContaining({ action: 'onboarded', onboardingError: null }),
    )
    expect(lastSlackMessage().replace_original).toBe(true)
    expect(lastSlackMessage().blocks[0].text.text).toContain('force-onboarded successfully')
  })

  it('rejects an invalid catalog id without touching the database', async () => {
    await run('not-a-uuid')

    expect(claimProjectCatalogForForcedOnboarding).not.toHaveBeenCalled()
    expect(onboardProject).not.toHaveBeenCalled()
  })

  it('stops when the clicker hit the daily onboarding cap', async () => {
    vi.mocked(reserveDailyProjectOnboardingRequest).mockImplementationOnce(() => {
      throw new Error('Daily project onboarding request limit reached')
    })

    await run()

    expect(claimProjectCatalogForForcedOnboarding).not.toHaveBeenCalled()
    expect(lastSlackMessage().blocks[0].text.text).toContain('limit reached')
  })

  it('does not onboard when the claim is refused', async () => {
    vi.mocked(claimProjectCatalogForForcedOnboarding).mockResolvedValue(null)

    await run()

    expect(onboardProject).not.toHaveBeenCalled()
    expect(lastSlackMessage().blocks[0].text.text).toContain('can no longer be force-onboarded')
  })

  it('records the error and alerts when onboarding fails', async () => {
    vi.mocked(onboardProject).mockResolvedValue({ outcome: 'error', error: 'boom' } as any)

    await run()

    expect(updateProjectCatalog).toHaveBeenCalledWith(
      expect.anything(),
      catalogId,
      expect.objectContaining({ action: 'error', onboardingError: 'boom', onboardedAt: null }),
    )
    expect(notifySlackBotRequest).toHaveBeenCalledWith(
      'errored',
      expect.objectContaining({ sourceUrl: claimed.sourceUrl }),
      { reason: 'boom', actorId: 'U123' },
      log,
    )
  })

  it('records the error and alerts when onboarding throws', async () => {
    vi.mocked(onboardProject).mockRejectedValue(new Error('network'))

    await run()

    expect(updateProjectCatalog).toHaveBeenCalledWith(
      expect.anything(),
      catalogId,
      expect.objectContaining({ action: 'error', onboardingError: 'network' }),
    )
    expect(notifySlackBotRequest).toHaveBeenCalledWith(
      'errored',
      expect.anything(),
      { reason: 'network', actorId: 'U123' },
      log,
    )
  })
})
