import axios from 'axios'
import { beforeEach, describe, expect, it, vi } from 'vitest'

vi.mock('axios', () => ({
  default: { post: vi.fn(async () => undefined) },
}))

vi.mock('@crowd/data-access-layer/src/database', () => ({
  getDbConnection: vi.fn(async () => ({})),
  WRITE_DB_CONFIG: vi.fn(() => ({})),
  pgpQx: vi.fn(() => ({})),
}))

vi.mock('@crowd/data-access-layer', () => ({
  deriveProjectIdentityFromRepoUrl: vi.fn(() => ({ projectSlug: 'foo/bar', repoName: 'bar' })),
  claimProjectCatalogForSlackEvaluation: vi.fn(),
  claimProjectCatalogForOnboarding: vi.fn(),
  finalizeProjectCatalogEvaluation: vi.fn(),
  updateProjectCatalog: vi.fn(async () => ({})),
  findRepoUrlsInCdp: vi.fn(async () => new Set()),
  findGithubOwnersWithLfProjects: vi.fn(async () => new Set()),
  findGithubOwnersWithNonLfRepos: vi.fn(async () => new Set()),
  computeExclusivelyLfOwners: vi.fn(() => new Set()),
  resolvePrecheckSkipReason: vi.fn(() => null),
  markProjectCatalogPreCheckSkipped: vi.fn(),
  setProjectCatalogSourceUrl: vi.fn(async () => undefined),
}))

vi.mock('@crowd/slack', () => ({
  postSlackMessage: vi.fn(),
  getSlackPermalink: vi.fn(),
}))

vi.mock('./slackBotRequestAlert', () => ({
  notifySlackBotRequest: vi.fn(async () => undefined),
}))

vi.mock('../../api/public/v1/projectEvaluation/evaluateProject', () => ({
  evaluateProject: vi.fn(),
}))

vi.mock('@crowd/project-onboarding', () => ({
  onboardProject: vi.fn(),
}))

import {
  claimProjectCatalogForOnboarding,
  claimProjectCatalogForSlackEvaluation,
  deriveProjectIdentityFromRepoUrl,
  finalizeProjectCatalogEvaluation,
  markProjectCatalogPreCheckSkipped,
  resolvePrecheckSkipReason,
  setProjectCatalogSourceUrl,
  updateProjectCatalog,
} from '@crowd/data-access-layer'
import { onboardProject } from '@crowd/project-onboarding'
import { getSlackPermalink, postSlackMessage } from '@crowd/slack'

import { evaluateProject } from '../../api/public/v1/projectEvaluation/evaluateProject'
import { runOnboardProjectCommand } from './onboardProjectCommand'
import { notifySlackBotRequest } from './slackBotRequestAlert'

const catalogEntry = {
  id: 'catalog-1',
  repoUrl: 'https://github.com/foo/bar',
  repoName: 'bar',
  projectSlug: 'foo/bar',
  lfCriticalityScore: null,
  source: 'manual',
}

function mockOptions() {
  return {
    log: { warn: vi.fn(), error: vi.fn() },
    database: { sequelize: {} },
  } as any
}

function lastSlackText(): string {
  const calls = vi.mocked(axios.post).mock.calls
  const message = calls[calls.length - 1][1] as { blocks: { text: { text: string } }[] }
  return message.blocks[0].text.text
}

describe('runOnboardProjectCommand', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.mocked(deriveProjectIdentityFromRepoUrl).mockReturnValue({
      projectSlug: 'foo/bar',
      repoName: 'bar',
    })
    vi.mocked(claimProjectCatalogForOnboarding).mockResolvedValue({
      ...catalogEntry,
      onboardedAt: new Date().toISOString(),
    } as any)
  })

  it('evaluates and onboards a positively evaluated repo', async () => {
    vi.mocked(claimProjectCatalogForSlackEvaluation).mockResolvedValue(catalogEntry as any)
    vi.mocked(evaluateProject).mockResolvedValue({
      outcome: 'onboard',
      evaluationResult: 'true',
      evaluationReason: null,
      metrics: null,
    })
    vi.mocked(finalizeProjectCatalogEvaluation).mockResolvedValue({
      ...catalogEntry,
      action: 'onboard',
    } as any)
    vi.mocked(onboardProject).mockResolvedValue({
      outcome: 'onboarded',
      segmentId: 'segment-1',
      error: null,
    })

    await runOnboardProjectCommand({
      repoUrl: catalogEntry.repoUrl,
      options: mockOptions(),
      responseUrl: 'https://hooks.slack.com/response',
      actorId: 'U123',
    })

    expect(onboardProject).toHaveBeenCalledTimes(1)
    expect(updateProjectCatalog).toHaveBeenCalledWith(
      expect.anything(),
      catalogEntry.id,
      expect.objectContaining({ action: 'onboarded', onboardingError: null }),
    )
    expect(lastSlackText()).toContain('onboarded successfully')
  })

  it('reports a negative evaluation and does not onboard', async () => {
    vi.mocked(claimProjectCatalogForSlackEvaluation).mockResolvedValue(catalogEntry as any)
    vi.mocked(evaluateProject).mockResolvedValue({
      outcome: 'skip',
      evaluationResult: 'false',
      evaluationReason: 'project is a documentation repo, an SDK, a website, a recipe, etc',
      metrics: null,
    })
    vi.mocked(finalizeProjectCatalogEvaluation).mockResolvedValue({
      ...catalogEntry,
      action: 'skip',
    } as any)

    await runOnboardProjectCommand({
      repoUrl: catalogEntry.repoUrl,
      options: mockOptions(),
      responseUrl: 'https://hooks.slack.com/response',
      actorId: 'U123',
    })

    expect(onboardProject).not.toHaveBeenCalled()
    expect(lastSlackText()).toContain('skip')
    const calls = vi.mocked(axios.post).mock.calls
    const lastMessage = calls[calls.length - 1][1] as any
    const button = lastMessage.blocks.find((b: any) => b.type === 'actions').elements[0]
    expect(button).toMatchObject({ action_id: 'force_onboarding', value: catalogEntry.id })
  })

  it('reports back without erroring when the repo is already onboarded or a duplicate evaluate request is in flight', async () => {
    vi.mocked(claimProjectCatalogForSlackEvaluation).mockResolvedValue(null)

    await runOnboardProjectCommand({
      repoUrl: catalogEntry.repoUrl,
      options: mockOptions(),
      responseUrl: 'https://hooks.slack.com/response',
      actorId: 'U123',
    })

    expect(evaluateProject).not.toHaveBeenCalled()
    expect(lastSlackText()).toContain('already onboarded')
  })

  it('reports a clear message when the daily catalog cap is exhausted', async () => {
    process.env.CROWD_PROJECT_CATALOG_DAILY_CAP = '1'
    try {
      await runOnboardProjectCommand({
        repoUrl: catalogEntry.repoUrl,
        options: mockOptions(),
        responseUrl: 'https://hooks.slack.com/response',
        actorId: 'daily-cap-actor',
      })
      expect(claimProjectCatalogForSlackEvaluation).toHaveBeenCalledTimes(1)

      vi.mocked(axios.post).mockClear()
      await runOnboardProjectCommand({
        repoUrl: catalogEntry.repoUrl,
        options: mockOptions(),
        responseUrl: 'https://hooks.slack.com/response',
        actorId: 'daily-cap-actor',
      })

      expect(claimProjectCatalogForSlackEvaluation).toHaveBeenCalledTimes(1)
      expect(lastSlackText()).toContain('no_entry')
    } finally {
      delete process.env.CROWD_PROJECT_CATALOG_DAILY_CAP
    }
  })

  it('does not double-onboard when the nightly worker already claimed the row', async () => {
    vi.mocked(claimProjectCatalogForSlackEvaluation).mockResolvedValue(catalogEntry as any)
    vi.mocked(evaluateProject).mockResolvedValue({
      outcome: 'onboard',
      evaluationResult: 'true',
      evaluationReason: null,
      metrics: null,
    })
    vi.mocked(finalizeProjectCatalogEvaluation).mockResolvedValue({
      ...catalogEntry,
      action: 'onboard',
    } as any)
    vi.mocked(claimProjectCatalogForOnboarding).mockResolvedValue(null)

    await runOnboardProjectCommand({
      repoUrl: catalogEntry.repoUrl,
      options: mockOptions(),
      responseUrl: 'https://hooks.slack.com/response',
      actorId: 'U123',
    })

    expect(onboardProject).not.toHaveBeenCalled()
    expect(lastSlackText()).toContain('automatic onboarding job')
  })

  it('persists a terminal state when the LLM cap rejects the request', async () => {
    vi.mocked(claimProjectCatalogForSlackEvaluation).mockResolvedValue(catalogEntry as any)
    vi.mocked(evaluateProject).mockRejectedValue(new Error('daily LLM cap exceeded'))

    await runOnboardProjectCommand({
      repoUrl: catalogEntry.repoUrl,
      options: mockOptions(),
      responseUrl: 'https://hooks.slack.com/response',
      actorId: 'U123',
    })

    expect(markProjectCatalogPreCheckSkipped).toHaveBeenCalledWith(
      expect.anything(),
      catalogEntry.id,
      expect.stringContaining('daily LLM cap exceeded'),
    )
    expect(lastSlackText()).toContain('no_entry')
  })

  it('persists onboardingError and reverts the claim when onboarding fails', async () => {
    vi.mocked(claimProjectCatalogForSlackEvaluation).mockResolvedValue(catalogEntry as any)
    vi.mocked(evaluateProject).mockResolvedValue({
      outcome: 'onboard',
      evaluationResult: 'true',
      evaluationReason: null,
      metrics: null,
    })
    vi.mocked(finalizeProjectCatalogEvaluation).mockResolvedValue({
      ...catalogEntry,
      action: 'onboard',
    } as any)
    vi.mocked(onboardProject).mockResolvedValue({
      outcome: 'error',
      segmentId: null,
      error: 'onboarding pipeline exploded',
    })

    await runOnboardProjectCommand({
      repoUrl: catalogEntry.repoUrl,
      options: mockOptions(),
      responseUrl: 'https://hooks.slack.com/response',
      actorId: 'U123',
    })

    expect(updateProjectCatalog).toHaveBeenCalledWith(
      expect.anything(),
      catalogEntry.id,
      expect.objectContaining({
        action: 'error',
        onboardingError: 'onboarding pipeline exploded',
        onboardedAt: null,
      }),
    )
    expect(lastSlackText()).toContain('onboarding failed')
  })

  describe('request message', () => {
    beforeEach(() => {
      vi.mocked(claimProjectCatalogForSlackEvaluation).mockResolvedValue(catalogEntry as any)
      vi.mocked(evaluateProject).mockRejectedValue(new Error('stop after claim'))
      vi.mocked(postSlackMessage).mockResolvedValue({ ok: true, ts: '1700000000.000100' })
      vi.mocked(getSlackPermalink).mockResolvedValue(
        'https://slack.test/archives/C1/p1700000000000100',
      )
    })

    const run = (channelId?: string) =>
      runOnboardProjectCommand({
        repoUrl: catalogEntry.repoUrl,
        options: mockOptions(),
        responseUrl: 'https://hooks.slack.com/response',
        actorId: 'U123',
        channelId,
      })

    it('posts the request to the channel and stores its permalink as sourceUrl', async () => {
      await run('C1')

      expect(postSlackMessage).toHaveBeenCalledWith(
        expect.objectContaining({ channel: 'C1', text: expect.stringContaining('<@U123>') }),
      )
      expect(getSlackPermalink).toHaveBeenCalledWith('C1', '1700000000.000100')
      expect(setProjectCatalogSourceUrl).toHaveBeenCalledWith(
        expect.anything(),
        catalogEntry.id,
        'https://slack.test/archives/C1/p1700000000000100',
      )
    })

    it('leaves sourceUrl alone and keeps going when the message cannot be posted', async () => {
      vi.mocked(postSlackMessage).mockResolvedValue({ ok: false, error: 'not_in_channel' })

      await run('C1')

      expect(getSlackPermalink).not.toHaveBeenCalled()
      expect(setProjectCatalogSourceUrl).not.toHaveBeenCalled()
      expect(evaluateProject).toHaveBeenCalledTimes(1)
    })

    it('warns and stores nothing when Slack sent no channel id', async () => {
      const options = mockOptions()
      await runOnboardProjectCommand({
        repoUrl: catalogEntry.repoUrl,
        options,
        responseUrl: 'https://hooks.slack.com/response',
        actorId: 'U123',
      })

      expect(options.log.warn).toHaveBeenCalled()

      expect(postSlackMessage).not.toHaveBeenCalled()
      expect(setProjectCatalogSourceUrl).not.toHaveBeenCalled()
    })

    it('does not store anything when the permalink cannot be fetched', async () => {
      vi.mocked(getSlackPermalink).mockResolvedValue(null)

      await run('C1')

      expect(setProjectCatalogSourceUrl).not.toHaveBeenCalled()
    })

    it('does not fail the command when storing the sourceUrl throws', async () => {
      vi.mocked(setProjectCatalogSourceUrl).mockRejectedValueOnce(new Error('db down'))

      await run('C1')

      expect(evaluateProject).toHaveBeenCalledTimes(1)
    })

    it('posts nothing when the claim is refused', async () => {
      vi.mocked(claimProjectCatalogForSlackEvaluation).mockResolvedValue(null)

      await run('C1')

      expect(postSlackMessage).not.toHaveBeenCalled()
      expect(setProjectCatalogSourceUrl).not.toHaveBeenCalled()
    })
  })

  describe('review channel alerts', () => {
    const run = () =>
      runOnboardProjectCommand({
        repoUrl: catalogEntry.repoUrl,
        options: mockOptions(),
        responseUrl: 'https://hooks.slack.com/response',
        actorId: 'U123',
      })

    const evaluated = (
      overrides: Partial<Awaited<ReturnType<typeof evaluateProject>>>,
    ): Awaited<ReturnType<typeof evaluateProject>> => ({
      outcome: 'onboard',
      evaluationResult: 'true',
      evaluationReason: null,
      metrics: null,
      ...overrides,
    })

    beforeEach(() => {
      vi.mocked(claimProjectCatalogForSlackEvaluation).mockResolvedValue(catalogEntry as any)
      vi.mocked(finalizeProjectCatalogEvaluation).mockResolvedValue({ ...catalogEntry } as any)
    })

    it('alerts a skip when the pre-check skips the repo', async () => {
      vi.mocked(resolvePrecheckSkipReason).mockReturnValueOnce('already in CDP')
      vi.mocked(markProjectCatalogPreCheckSkipped).mockResolvedValueOnce(1)

      await run()

      expect(notifySlackBotRequest).toHaveBeenCalledWith(
        'skipped',
        expect.objectContaining({ repoName: 'bar', repoUrl: catalogEntry.repoUrl }),
        { reason: 'already in CDP', actorId: 'U123' },
        expect.anything(),
      )
    })

    it('links the current request message, not a source url retained from discovery', async () => {
      vi.mocked(claimProjectCatalogForSlackEvaluation).mockResolvedValue({
        ...catalogEntry,
        sourceUrl: 'https://github.com/foo/bar/discussions/1',
      } as any)
      vi.mocked(postSlackMessage).mockResolvedValue({ ok: true, ts: '1.1' })
      vi.mocked(getSlackPermalink).mockResolvedValue('https://acme.slack.com/archives/C1/p11')
      vi.mocked(resolvePrecheckSkipReason).mockReturnValueOnce('already in CDP')
      vi.mocked(markProjectCatalogPreCheckSkipped).mockResolvedValueOnce(1)

      await runOnboardProjectCommand({
        repoUrl: catalogEntry.repoUrl,
        options: mockOptions(),
        responseUrl: 'https://hooks.slack.com/response',
        actorId: 'U123',
        channelId: 'C1',
      })

      expect(notifySlackBotRequest).toHaveBeenCalledWith(
        'skipped',
        expect.objectContaining({ sourceUrl: 'https://acme.slack.com/archives/C1/p11' }),
        expect.anything(),
        expect.anything(),
      )
    })

    it('falls back to no link when the request message could not be posted', async () => {
      vi.mocked(claimProjectCatalogForSlackEvaluation).mockResolvedValue({
        ...catalogEntry,
        sourceUrl: 'https://github.com/foo/bar/discussions/1',
      } as any)
      vi.mocked(postSlackMessage).mockResolvedValue({ ok: false, error: 'not_in_channel' })
      vi.mocked(resolvePrecheckSkipReason).mockReturnValueOnce('already in CDP')
      vi.mocked(markProjectCatalogPreCheckSkipped).mockResolvedValueOnce(1)

      await runOnboardProjectCommand({
        repoUrl: catalogEntry.repoUrl,
        options: mockOptions(),
        responseUrl: 'https://hooks.slack.com/response',
        actorId: 'U123',
        channelId: 'C1',
      })

      expect(notifySlackBotRequest).toHaveBeenCalledWith(
        'skipped',
        expect.objectContaining({ sourceUrl: null }),
        expect.anything(),
        expect.anything(),
      )
    })

    it('does not alert when a concurrent request already moved the pre-check skip on', async () => {
      vi.mocked(resolvePrecheckSkipReason).mockReturnValueOnce('already in CDP')
      vi.mocked(markProjectCatalogPreCheckSkipped).mockResolvedValueOnce(0)

      await run()

      expect(notifySlackBotRequest).not.toHaveBeenCalled()
    })

    it('alerts a skip when the request is rejected before evaluation', async () => {
      vi.mocked(evaluateProject).mockRejectedValue(new Error('daily LLM cap exceeded'))
      vi.mocked(markProjectCatalogPreCheckSkipped).mockResolvedValueOnce(1)

      await run()

      expect(notifySlackBotRequest).toHaveBeenCalledWith(
        'skipped',
        expect.anything(),
        { reason: expect.stringContaining('daily LLM cap exceeded'), actorId: 'U123' },
        expect.anything(),
      )
    })

    it('alerts an error when the evaluation errors', async () => {
      vi.mocked(evaluateProject).mockResolvedValue(
        evaluated({ outcome: 'skip', evaluationResult: 'error', evaluationReason: 'llm down' }),
      )

      await run()

      expect(notifySlackBotRequest).toHaveBeenCalledWith(
        'errored',
        expect.anything(),
        { reason: 'llm down', actorId: 'U123' },
        expect.anything(),
      )
    })

    it('alerts a skip on a negative evaluation', async () => {
      vi.mocked(evaluateProject).mockResolvedValue(
        evaluated({ outcome: 'skip', evaluationResult: 'false', evaluationReason: 'docs repo' }),
      )

      await run()

      expect(notifySlackBotRequest).toHaveBeenCalledWith(
        'skipped',
        expect.anything(),
        { reason: 'docs repo', actorId: 'U123' },
        expect.anything(),
      )
    })

    it('does not alert when finalize lost the race', async () => {
      vi.mocked(evaluateProject).mockResolvedValue(
        evaluated({ outcome: 'skip', evaluationResult: 'false', evaluationReason: 'docs repo' }),
      )
      vi.mocked(finalizeProjectCatalogEvaluation).mockResolvedValue(null as any)

      await run()

      expect(notifySlackBotRequest).not.toHaveBeenCalled()
    })

    it('alerts an error when onboarding returns an error', async () => {
      vi.mocked(evaluateProject).mockResolvedValue(evaluated({}))
      vi.mocked(onboardProject).mockResolvedValue({
        outcome: 'error',
        segmentId: null,
        error: 'pipeline exploded',
      })

      await run()

      expect(notifySlackBotRequest).toHaveBeenCalledWith(
        'errored',
        expect.anything(),
        { reason: 'pipeline exploded', actorId: 'U123' },
        expect.anything(),
      )
    })

    it('alerts an error when onboarding throws', async () => {
      vi.mocked(evaluateProject).mockResolvedValue(evaluated({}))
      vi.mocked(onboardProject).mockRejectedValue(new Error('network down'))

      await run()

      expect(notifySlackBotRequest).toHaveBeenCalledWith(
        'errored',
        expect.anything(),
        { reason: 'network down', actorId: 'U123' },
        expect.anything(),
      )
    })

    it('does not alert on a successful onboarding', async () => {
      vi.mocked(evaluateProject).mockResolvedValue(evaluated({}))
      vi.mocked(onboardProject).mockResolvedValue({
        outcome: 'onboarded',
        segmentId: 'segment-1',
        error: null,
      })

      await run()

      expect(notifySlackBotRequest).not.toHaveBeenCalled()
    })
  })
})
