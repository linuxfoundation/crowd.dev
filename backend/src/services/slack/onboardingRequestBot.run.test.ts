import { beforeEach, describe, expect, it, vi } from 'vitest'

import { RateLimitError } from '@crowd/common'
import { WorkflowIdReusePolicy } from '@crowd/temporal'

const { reserveDailyLlmCall, fetchPublicOrgRepos, isPublicGithubRepo } = vi.hoisted(() => ({
  reserveDailyLlmCall: vi.fn(),
  fetchPublicOrgRepos: vi.fn(),
  isPublicGithubRepo: vi.fn(),
}))

vi.mock('@crowd/common', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@crowd/common')>()),
  fetchPublicOrgRepos,
  isPublicGithubRepo,
  getGithubToken: vi.fn(() => 'github-token'),
}))

vi.mock('@crowd/slack', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@crowd/slack')>()),
  getSlackPermalink: vi.fn(async () => 'https://acme.slack.com/archives/C1/p1'),
  postSlackMessage: vi.fn(async () => ({ ok: true, ts: '1760000000.200000' })),
  updateSlackMessage: vi.fn(async () => ({ ok: true })),
  addSlackReaction: vi.fn(async () => ({ ok: true })),
}))
vi.mock('@crowd/data-access-layer', () => ({
  bulkInsertProjectCatalog: vi.fn(async (_qx: unknown, rows: { repoUrl: string }[]) =>
    rows.map((row, index) => ({ id: `id-${index + 1}`, repoUrl: row.repoUrl })),
  ),
  deriveProjectIdentityFromRepoUrl: vi.fn(() => ({ projectSlug: 'acme-one', repoName: 'one' })),
  findExistingProjectCatalogRepoUrls: vi.fn(async () => new Set<string>()),
  findProjectCatalogByRepoUrls: vi.fn(async () => []),
  findTrackedRepoProjects: vi.fn(async () => []),
  promoteProjectCatalogProvenance: vi.fn(async () => 0),
  reopenRejectedProjectCatalog: vi.fn(async () => []),
  upsertSlackOnboardingRequest: vi.fn(async () => undefined),
}))
vi.mock('./slackBackground', () => ({ getBgQx: vi.fn(async () => ({})) }))
vi.mock('../../api/public/v1/projectEvaluation/dailyLlmCap', () => ({
  createDailyLlmCap: vi.fn(() => reserveDailyLlmCall),
}))
vi.mock('@crowd/project-onboarding/src/requestLlm', () => ({
  createOnboardingRequestLlm: vi.fn(() => async () => null),
}))
vi.mock('@crowd/project-onboarding', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@crowd/project-onboarding')>()),
  parseOnboardingRequest: vi.fn(),
}))

import {
  bulkInsertProjectCatalog,
  findExistingProjectCatalogRepoUrls,
  findProjectCatalogByRepoUrls,
  findTrackedRepoProjects,
  promoteProjectCatalogProvenance,
  reopenRejectedProjectCatalog,
  upsertSlackOnboardingRequest,
} from '@crowd/data-access-layer'
import { parseOnboardingRequest } from '@crowd/project-onboarding'
import {
  addSlackReaction,
  getSlackPermalink,
  postSlackMessage,
  updateSlackMessage,
} from '@crowd/slack'

import { runOnboardingRequestBot } from './onboardingRequestBot'

const log = { info: vi.fn(), warn: vi.fn(), error: vi.fn() } as any
const startWorkflow = vi.fn(async () => undefined)
const temporal = { workflow: { start: startWorkflow } } as never

const parsedRequest = {
  githubRepoUrls: ['https://github.com/acme/one'],
  nonGithubRepoUrls: [],
  linksToFollow: [],
  projectName: 'Acme',
  declaredLf: null,
  asksAboutHierarchy: false,
}

function parsedAs(overrides: Partial<typeof parsedRequest>) {
  vi.mocked(parseOnboardingRequest).mockResolvedValueOnce({
    ok: true,
    request: { ...parsedRequest, ...overrides },
  })
}

function run(overrides: Record<string, unknown> = {}) {
  return runOnboardingRequestBot({
    text: '<@U0BOT> onboard Acme <https://github.com/acme/one>',
    channelId: 'C1',
    messageTs: '100.1',
    threadTs: '90.0',
    actorId: 'U1',
    options: { log, temporal },
    ...overrides,
  })
}

describe('runOnboardingRequestBot', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.mocked(postSlackMessage).mockResolvedValue({ ok: true, ts: '1760000000.200000' })
    vi.mocked(findExistingProjectCatalogRepoUrls).mockResolvedValue(new Set())
    vi.mocked(findTrackedRepoProjects).mockResolvedValue([])
    vi.mocked(findProjectCatalogByRepoUrls).mockResolvedValue([])
    vi.mocked(reopenRejectedProjectCatalog).mockResolvedValue([])
    isPublicGithubRepo.mockResolvedValue(true)
    vi.mocked(parseOnboardingRequest).mockResolvedValue({ ok: true, request: parsedRequest })
  })

  it('refuses a repository that does not exist on GitHub without touching the catalog', async () => {
    isPublicGithubRepo.mockResolvedValueOnce(false)

    await run({ text: '<@U0BOT> onboard Acme github.com/acme/one' })

    expect(isPublicGithubRepo).toHaveBeenCalledWith(
      'https://github.com/acme/one',
      'github-token',
      expect.any(AbortSignal),
    )
    expect(getSlackPermalink).not.toHaveBeenCalled()
    expect(bulkInsertProjectCatalog).not.toHaveBeenCalled()
    expect(postSlackMessage).toHaveBeenCalledWith({
      channel: 'C1',
      thread_ts: '90.0',
      text: "🚫 *<https://github.com/acme/one|acme/one>* · not added: I couldn't find it on GitHub. Check the link (it may be private or a typo) and send it again.",
    })
    expect(addSlackReaction).toHaveBeenCalledWith({
      channel: 'C1',
      timestamp: '100.1',
      name: 'no_entry_sign',
    })
  })

  it('queues the repositories that exist and reports the missing one in the same status', async () => {
    parsedAs({ githubRepoUrls: ['https://github.com/acme/one', 'https://github.com/acme/typo'] })
    isPublicGithubRepo.mockImplementation(async (repoUrl: string) => !repoUrl.endsWith('/typo'))

    await run()

    expect(bulkInsertProjectCatalog).toHaveBeenCalledWith(expect.anything(), [
      expect.objectContaining({ repoUrl: 'https://github.com/acme/one' }),
    ])
    expect(postSlackMessage).toHaveBeenCalledWith(
      expect.objectContaining({
        text: [
          '⏳ Checking *<https://github.com/acme/one|acme/one>*…',
          "🚫 *<https://github.com/acme/typo|acme/typo>* · not added: I couldn't find it on GitHub. Check the link (it may be private or a typo) and send it again.",
          "I'll update this message as each repo is checked.",
        ].join('\n'),
      }),
    )
    expect(addSlackReaction).toHaveBeenCalledWith(
      expect.objectContaining({ name: 'no_entry_sign' }),
    )
    expect(addSlackReaction).toHaveBeenCalledWith(
      expect.objectContaining({ name: 'hourglass_flowing_sand' }),
    )
  })

  it('does not check on GitHub a repository that is already in LFX Insights', async () => {
    vi.mocked(findTrackedRepoProjects).mockResolvedValueOnce([
      { repoUrl: 'https://github.com/acme/one', projectName: 'Acme', projectSlug: 'acme' },
    ] as never)

    await run()

    expect(isPublicGithubRepo).not.toHaveBeenCalled()
    expect(bulkInsertProjectCatalog).not.toHaveBeenCalled()
    expect(addSlackReaction).toHaveBeenCalledWith(expect.objectContaining({ name: 'blue_book' }))
  })

  it('keeps a repository it could not check on GitHub and logs why', async () => {
    isPublicGithubRepo.mockRejectedValueOnce(new RateLimitError('GitHub rate limit'))

    await run()

    expect(bulkInsertProjectCatalog).toHaveBeenCalledWith(expect.anything(), [
      expect.objectContaining({ repoUrl: 'https://github.com/acme/one' }),
    ])
    expect(log.warn).toHaveBeenCalledWith(
      expect.objectContaining({ repoUrl: 'https://github.com/acme/one' }),
      'Repository could not be checked on GitHub.',
    )
  })

  it('checks again a repository whose earlier request was rejected for a requester error and says where that was', async () => {
    vi.mocked(findExistingProjectCatalogRepoUrls).mockResolvedValueOnce(
      new Set(['https://github.com/acme/one']),
    )
    vi.mocked(reopenRejectedProjectCatalog).mockResolvedValueOnce([
      {
        id: 'id-old',
        repoUrl: 'https://github.com/acme/one',
        previousSourceUrl: 'https://acme.slack.com/archives/C1/p0',
        previousDecisionReason: 'wrong_project',
      },
    ])

    await run()

    const note =
      "*<https://github.com/acme/one|acme/one>* was already requested <https://acme.slack.com/archives/C1/p0|here> and wasn't added (Wrong project), so I'm checking it again."
    expect(bulkInsertProjectCatalog).toHaveBeenCalledWith(expect.anything(), [])
    expect(promoteProjectCatalogProvenance).toHaveBeenCalledWith(expect.anything(), 'slack-bot', [])
    expect(postSlackMessage).toHaveBeenCalledWith({
      channel: 'C1',
      thread_ts: '90.0',
      text: [
        '⏳ Checking *<https://github.com/acme/one|acme/one>*…',
        note,
        "I'll update this message as each repo is checked.",
      ].join('\n'),
    })
    expect(upsertSlackOnboardingRequest).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ header: { lines: [note], buttons: [], alreadyTracked: 0 } }),
    )
    expect(startWorkflow).toHaveBeenCalledWith(
      'evaluateProjects',
      expect.objectContaining({ args: [{ catalogIds: ['id-old'] }] }),
    )
    expect(addSlackReaction).toHaveBeenCalledWith({
      channel: 'C1',
      timestamp: '100.1',
      name: 'hourglass_flowing_sand',
    })
  })

  it('queues the repositories of the mentioned message, starts their evaluation and replies in the thread', async () => {
    await run()

    expect(getSlackPermalink).toHaveBeenCalledWith('C1', '100.1')
    expect(bulkInsertProjectCatalog).toHaveBeenCalledWith(expect.anything(), [
      expect.objectContaining({
        repoUrl: 'https://github.com/acme/one',
        sourceUrl: 'https://acme.slack.com/archives/C1/p1',
        requestText: 'onboard Acme https://github.com/acme/one',
        action: 'auto',
        projectName: 'Acme',
      }),
    ])
    expect(startWorkflow).toHaveBeenCalledWith('evaluateProjects', {
      taskQueue: 'projects-evaluation',
      workflowId: 'evaluate-slack-C1-100.1',
      workflowIdReusePolicy: WorkflowIdReusePolicy.REJECT_DUPLICATE,
      args: [{ catalogIds: ['id-1'] }],
    })
    expect(postSlackMessage).toHaveBeenCalledWith({
      channel: 'C1',
      thread_ts: '90.0',
      text: expect.stringContaining('⏳ Checking *<https://github.com/acme/one|acme/one>*…'),
    })
    expect(log.warn).not.toHaveBeenCalled()
    expect(upsertSlackOnboardingRequest).toHaveBeenCalledWith(expect.anything(), {
      sourceUrl: 'https://acme.slack.com/archives/C1/p1',
      channel: 'C1',
      messageTs: '1760000000.200000',
      header: { lines: [], buttons: [], alreadyTracked: 0 },
    })
    expect(addSlackReaction).toHaveBeenCalledWith({
      channel: 'C1',
      timestamp: '100.1',
      name: 'hourglass_flowing_sand',
    })
  })

  it('keeps the tracked and earlier repositories in the status header', async () => {
    vi.mocked(parseOnboardingRequest).mockResolvedValue({
      ok: true,
      request: parsedAs({
        githubRepoUrls: ['https://github.com/acme/one', 'https://github.com/acme/two'],
      }),
    } as never)
    vi.mocked(findTrackedRepoProjects).mockResolvedValueOnce([
      { repoUrl: 'https://github.com/acme/one', projectName: 'Acme', projectSlug: 'acme' },
    ])

    await run()

    expect(upsertSlackOnboardingRequest).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({
        header: {
          lines: [
            '📘 *<https://github.com/acme/one|acme/one>* is already in LFX Insights under *Acme*. Nothing else to do.',
          ],
          buttons: [
            {
              text: 'View Acme in LFX Insights',
              url: 'https://insights.linuxfoundation.org/project/acme',
            },
          ],
          alreadyTracked: 1,
        },
      }),
    )
  })

  it('leaves rows adopted by the request out of the status header', async () => {
    vi.mocked(parseOnboardingRequest).mockResolvedValue({
      ok: true,
      request: parsedAs({
        githubRepoUrls: ['https://github.com/acme/one', 'https://github.com/acme/two'],
      }),
    } as never)
    vi.mocked(findExistingProjectCatalogRepoUrls).mockResolvedValueOnce(
      new Set(['https://github.com/acme/two']),
    )
    const earlier = { repoUrl: 'https://github.com/acme/two', action: 'evaluate' }
    vi.mocked(findProjectCatalogByRepoUrls)
      .mockResolvedValueOnce([{ ...earlier, sourceUrl: null } as never])
      .mockResolvedValueOnce([
        { ...earlier, sourceUrl: 'https://acme.slack.com/archives/C1/p1' } as never,
      ])

    await run()

    expect(upsertSlackOnboardingRequest).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ header: { lines: [], buttons: [], alreadyTracked: 0 } }),
    )
  })

  it('keeps an earlier row without origin in the header when the ingest did not adopt it', async () => {
    vi.mocked(parseOnboardingRequest).mockResolvedValue({
      ok: true,
      request: parsedAs({
        githubRepoUrls: ['https://github.com/acme/one', 'https://github.com/acme/two'],
      }),
    } as never)
    vi.mocked(findExistingProjectCatalogRepoUrls).mockResolvedValueOnce(
      new Set(['https://github.com/acme/two']),
    )
    vi.mocked(findProjectCatalogByRepoUrls).mockResolvedValue([
      { repoUrl: 'https://github.com/acme/two', action: 'evaluate', sourceUrl: null } as never,
    ])

    await run()

    expect(upsertSlackOnboardingRequest).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({
        header: expect.objectContaining({
          lines: [expect.stringContaining('acme/two')],
        }),
      }),
    )
  })

  it('registers the status message when the request only adopted existing rows', async () => {
    vi.mocked(parseOnboardingRequest).mockResolvedValue({
      ok: true,
      request: parsedAs({ githubRepoUrls: ['https://github.com/acme/two'] }),
    } as never)
    vi.mocked(findExistingProjectCatalogRepoUrls).mockResolvedValueOnce(
      new Set(['https://github.com/acme/two']),
    )
    vi.mocked(bulkInsertProjectCatalog).mockResolvedValueOnce([])
    const earlier = { repoUrl: 'https://github.com/acme/two', action: 'evaluate' }
    vi.mocked(findProjectCatalogByRepoUrls)
      .mockResolvedValueOnce([{ ...earlier, sourceUrl: null } as never])
      .mockResolvedValueOnce([
        { ...earlier, sourceUrl: 'https://acme.slack.com/archives/C1/p1' } as never,
      ])

    await run()

    expect(startWorkflow).not.toHaveBeenCalled()
    expect(upsertSlackOnboardingRequest).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ header: { lines: [], buttons: [], alreadyTracked: 0 } }),
    )
  })

  it('does not start an evaluation when every repository was already catalogued', async () => {
    vi.mocked(findExistingProjectCatalogRepoUrls).mockResolvedValue(
      new Set(['https://github.com/acme/one']),
    )

    vi.mocked(findProjectCatalogByRepoUrls).mockResolvedValue([
      {
        repoUrl: 'https://github.com/acme/one',
        action: 'review',
        skipReason: null,
        sourceUrl: 'https://acme.slack.com/archives/C1/p1760000000123456',
      } as never,
    ])

    await run()

    expect(bulkInsertProjectCatalog).toHaveBeenCalledWith(expect.anything(), [])
    expect(startWorkflow).not.toHaveBeenCalled()
    expect(findProjectCatalogByRepoUrls).toHaveBeenCalledTimes(2)
    expect(vi.mocked(findProjectCatalogByRepoUrls).mock.invocationCallOrder[0]).toBeLessThan(
      vi.mocked(bulkInsertProjectCatalog).mock.invocationCallOrder[0],
    )
    expect(vi.mocked(findProjectCatalogByRepoUrls).mock.invocationCallOrder[1]).toBeGreaterThan(
      vi.mocked(bulkInsertProjectCatalog).mock.invocationCallOrder[0],
    )
    expect(postSlackMessage).toHaveBeenCalledWith(
      expect.objectContaining({
        text: expect.stringContaining(
          'was already requested <https://acme.slack.com/archives/C1/p1760000000123456|here> and is with the CDP team',
        ),
      }),
    )
    expect(addSlackReaction).toHaveBeenCalledWith(expect.objectContaining({ name: 'eyes' }))
    expect(addSlackReaction).not.toHaveBeenCalledWith(
      expect.objectContaining({ name: 'hourglass_flowing_sand' }),
    )
  })

  it('tells the requester the rows were recorded when the evaluation cannot be started', async () => {
    startWorkflow.mockRejectedValueOnce(new Error('temporal down'))

    await run()

    expect(log.error).toHaveBeenCalledWith(
      expect.objectContaining({ error: 'temporal down', channelId: 'C1', catalogIds: ['id-1'] }),
      'Immediate evaluation could not be started for the Slack request.',
    )
    expect(updateSlackMessage).toHaveBeenCalledWith(
      expect.objectContaining({
        channel: 'C1',
        ts: '1760000000.200000',
        text: expect.stringMatching(/Checking[\s\S]*next scheduled evaluation/),
      }),
    )
    expect(postSlackMessage).not.toHaveBeenCalledWith(
      expect.objectContaining({ text: expect.stringContaining("couldn't record this request") }),
    )
  })

  it('stores the status message before starting the evaluation', async () => {
    await run()

    const stored = vi.mocked(upsertSlackOnboardingRequest).mock.invocationCallOrder[0]
    expect(stored).toBeLessThan(startWorkflow.mock.invocationCallOrder[0])
  })

  it('leaves the rows to the scheduled evaluation when Temporal is not configured', async () => {
    await run({ options: { log, temporal: null } })

    expect(startWorkflow).not.toHaveBeenCalled()
    expect(log.warn).toHaveBeenCalledWith(
      expect.objectContaining({ channelId: 'C1', catalogIds: ['id-1'] }),
      'Temporal is not configured, the Slack request waits for the scheduled evaluation.',
    )
    expect(updateSlackMessage).toHaveBeenCalledWith(
      expect.objectContaining({
        text: expect.stringMatching(/Checking[\s\S]*next scheduled evaluation/),
      }),
    )
  })

  it('names the unsupported host when the only repository is not on GitHub, without touching the catalog', async () => {
    parsedAs({ githubRepoUrls: [], nonGithubRepoUrls: ['https://gitlab.com/acme/one'] })

    await run({ text: '<@U0BOT> please onboard Acme, it lives on https://gitlab.com/acme/one' })

    expect(getSlackPermalink).not.toHaveBeenCalled()
    expect(bulkInsertProjectCatalog).not.toHaveBeenCalled()
    expect(postSlackMessage).toHaveBeenCalledWith(
      expect.objectContaining({
        text: expect.stringContaining('*gitlab.com/acme/one* is on GitLab'),
      }),
    )
  })

  it('asks for the GitHub links when only a web page was linked', async () => {
    parsedAs({ githubRepoUrls: [], linksToFollow: ['https://wiki.example.org/projects'] })

    await run({ text: '<@U0BOT> onboard the project at https://wiki.example.org/projects' })

    expect(bulkInsertProjectCatalog).not.toHaveBeenCalled()
    expect(postSlackMessage).toHaveBeenCalledWith(
      expect.objectContaining({ text: expect.stringContaining("I don't open web pages") }),
    )
  })

  it('queues the public repositories of a GitHub organization and says so in the status', async () => {
    parsedAs({
      githubRepoUrls: [],
      projectName: 'OpenBMC',
      declaredLf: true,
      linksToFollow: ['https://github.com/openbmc/'],
    })
    fetchPublicOrgRepos.mockResolvedValue({
      repoUrls: [
        'https://github.com/openbmc/bmcweb',
        'https://github.com/openbmc/phosphor-logging',
      ],
      exceededLimit: false,
    })

    await run({ text: '<@U0BOT> onboard OpenBMC, LF project, <https://github.com/openbmc/>' })

    expect(fetchPublicOrgRepos).toHaveBeenCalledWith('openbmc', 'github-token', 50)
    expect(bulkInsertProjectCatalog).toHaveBeenCalledWith(expect.anything(), [
      expect.objectContaining({
        repoUrl: 'https://github.com/openbmc/bmcweb',
        projectName: 'OpenBMC',
        requestedBy: 'U1',
        requestHints: { organization: 'openbmc' },
      }),
      expect.objectContaining({
        repoUrl: 'https://github.com/openbmc/phosphor-logging',
        requestHints: { organization: 'openbmc' },
      }),
    ])
    const { text: reply } = vi.mocked(postSlackMessage).mock.calls[0][0] as { text: string }
    expect(reply).toContain('⏳ Checking *<https://github.com/openbmc/bmcweb|openbmc/bmcweb>*…')
    expect(reply).toContain(
      "*github.com/openbmc* is a GitHub organization, so I'm adding its 2 public repositories",
    )
    expect(upsertSlackOnboardingRequest).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({
        header: expect.objectContaining({
          lines: [expect.stringContaining('is a GitHub organization')],
        }),
      }),
    )
    expect(startWorkflow).toHaveBeenCalled()
  })

  it('gives adopted earlier rows of an organization the owner hint too', async () => {
    parsedAs({
      githubRepoUrls: [],
      projectName: 'OpenBMC',
      linksToFollow: ['https://github.com/openbmc/'],
    })
    fetchPublicOrgRepos.mockResolvedValue({
      repoUrls: ['https://github.com/openbmc/bmcweb', 'https://github.com/openbmc/docs'],
      exceededLimit: false,
    })
    vi.mocked(findExistingProjectCatalogRepoUrls).mockResolvedValueOnce(
      new Set(['https://github.com/openbmc/docs']),
    )

    await run({ text: '<@U0BOT> onboard OpenBMC <https://github.com/openbmc/>' })

    expect(promoteProjectCatalogProvenance).toHaveBeenCalledWith(expect.anything(), 'slack-bot', [
      {
        repoUrl: 'https://github.com/openbmc/docs',
        sourceUrl: 'https://acme.slack.com/archives/C1/p1',
        organization: 'openbmc',
        requestText: 'onboard OpenBMC https://github.com/openbmc/',
        projectName: 'OpenBMC',
        requestedBy: 'U1',
      },
    ])
  })

  it('leaves out an organization the parser did not keep', async () => {
    parsedAs({ githubRepoUrls: ['https://github.com/acme/one'], linksToFollow: [] })

    await run({
      text: '<@U0BOT> do not onboard https://github.com/ignored; onboard Acme https://github.com/acme/one',
    })

    expect(fetchPublicOrgRepos).not.toHaveBeenCalled()
    expect(bulkInsertProjectCatalog).toHaveBeenCalledWith(expect.anything(), [
      expect.objectContaining({ repoUrl: 'https://github.com/acme/one' }),
    ])
  })

  it('explains an organization it cannot list instead of queueing anything', async () => {
    parsedAs({
      githubRepoUrls: [],
      projectName: 'OpenBMC',
      linksToFollow: ['https://github.com/openbmc'],
    })
    fetchPublicOrgRepos.mockResolvedValue({
      repoUrls: Array.from({ length: 50 }, (_, i) => `https://github.com/openbmc/r${i}`),
      exceededLimit: true,
    })

    await run({ text: '<@U0BOT> onboard OpenBMC https://github.com/openbmc' })

    expect(bulkInsertProjectCatalog).not.toHaveBeenCalled()
    expect(startWorkflow).not.toHaveBeenCalled()
    expect(postSlackMessage).toHaveBeenCalledWith(
      expect.objectContaining({
        text: expect.stringContaining('has more than 50 public repositories'),
      }),
    )
  })

  it('asks for the missing link and keeps the project name the requester gave', async () => {
    parsedAs({ githubRepoUrls: [], projectName: 'Purgatoria' })

    await run({ text: '<@U0BOT> please onboard Purgatoria, it is a new AI lab project' })

    expect(bulkInsertProjectCatalog).not.toHaveBeenCalled()
    expect(postSlackMessage).toHaveBeenCalledWith(
      expect.objectContaining({
        text: expect.stringContaining('I need at least one GitHub link to add *Purgatoria*.'),
      }),
    )
  })

  it('answers a hierarchy question without creating a row, even when a repository is linked', async () => {
    parsedAs({ asksAboutHierarchy: true, projectName: 'pointblank' })

    await run({
      text: '<@U0BOT> should pointblank https://github.com/acme/one be under CNCF or LFN?',
    })

    expect(getSlackPermalink).not.toHaveBeenCalled()
    expect(bulkInsertProjectCatalog).not.toHaveBeenCalled()
    expect(postSlackMessage).toHaveBeenCalledWith(
      expect.objectContaining({
        text: expect.stringContaining(
          "which foundation *pointblank* belongs to, and I can't decide that",
        ),
      }),
    )
  })

  it('queues what the parser understood: bare owner/repo included, excluded links left out', async () => {
    parsedAs({ githubRepoUrls: ['https://github.com/acme/tool', 'https://github.com/acme/one'] })

    await run({
      text: '<@U0BOT> do not onboard <https://github.com/acme/old>; onboard Acme acme/tool and <https://github.com/acme/one>',
    })

    expect(bulkInsertProjectCatalog).toHaveBeenCalledWith(expect.anything(), [
      expect.objectContaining({ repoUrl: 'https://github.com/acme/tool' }),
      expect.objectContaining({ repoUrl: 'https://github.com/acme/one' }),
    ])
  })

  it('counts one parser call per Slack user and falls back to the text links past the daily limit', async () => {
    reserveDailyLlmCall.mockImplementationOnce(() => {
      throw new RateLimitError('Daily evaluation limit reached')
    })

    await run()

    expect(reserveDailyLlmCall).toHaveBeenCalledWith('U1')
    expect(parseOnboardingRequest).not.toHaveBeenCalled()
    expect(bulkInsertProjectCatalog).toHaveBeenCalledWith(expect.anything(), [
      expect.objectContaining({ repoUrl: 'https://github.com/acme/one', projectName: null }),
    ])
    expect(log.warn).toHaveBeenCalledWith(
      expect.objectContaining({ actorId: 'U1' }),
      'Daily parser limit reached for this Slack user, falling back to the GitHub links in the text.',
    )
  })

  it('answers a tracked repository as already in LFX Insights with a link button and no row', async () => {
    vi.mocked(findTrackedRepoProjects).mockResolvedValue([
      { repoUrl: 'https://github.com/acme/one', projectName: 'Acme', projectSlug: 'acme' },
    ])

    await run()

    expect(getSlackPermalink).not.toHaveBeenCalled()
    expect(bulkInsertProjectCatalog).not.toHaveBeenCalled()
    expect(startWorkflow).not.toHaveBeenCalled()
    expect(postSlackMessage).toHaveBeenCalledWith(
      expect.objectContaining({
        text: '📘 *<https://github.com/acme/one|acme/one>* is already in LFX Insights under *Acme*. Nothing else to do.',
        blocks: expect.arrayContaining([expect.objectContaining({ type: 'actions' })]),
      }),
    )
    expect(addSlackReaction).toHaveBeenCalledWith(
      expect.objectContaining({ timestamp: '100.1', name: 'blue_book' }),
    )
  })

  it('queues only the repositories that are not tracked yet and reports both', async () => {
    parsedAs({ githubRepoUrls: ['https://github.com/acme/one', 'https://github.com/acme/two'] })
    vi.mocked(findTrackedRepoProjects).mockResolvedValue([
      { repoUrl: 'https://github.com/acme/one', projectName: 'Acme', projectSlug: 'acme' },
    ])

    await run({
      text: '<@U0BOT> onboard Acme <https://github.com/acme/one> <https://github.com/acme/two>',
    })

    expect(bulkInsertProjectCatalog).toHaveBeenCalledWith(expect.anything(), [
      expect.objectContaining({ repoUrl: 'https://github.com/acme/two' }),
    ])
    expect(addSlackReaction).toHaveBeenCalledWith(expect.objectContaining({ name: 'blue_book' }))
    expect(addSlackReaction).toHaveBeenCalledWith(
      expect.objectContaining({ name: 'hourglass_flowing_sand' }),
    )
    expect(postSlackMessage).toHaveBeenCalledWith(
      expect.objectContaining({
        text: expect.stringMatching(
          /already in LFX Insights under \*Acme\*[\s\S]*⏳ Checking \*<https:\/\/github.com\/acme\/two\|acme\/two>\*/,
        ),
      }),
    )
  })

  it('tells the requester what it normalised: duplicates, folder links, skipped hosts, truncation', async () => {
    const filler = ' lorem'.repeat(2000)
    parsedAs({ nonGithubRepoUrls: ['https://gitlab.com/acme/legacy'] })

    await run({
      text: `<@U0BOT> onboard Acme <https://github.com/acme/one/tree/main/docs> <https://github.com/acme/one> and gitlab.com/acme/legacy${filler}`,
    })

    expect(bulkInsertProjectCatalog).toHaveBeenCalledWith(expect.anything(), [
      expect.objectContaining({ repoUrl: 'https://github.com/acme/one' }),
    ])
    const { text: reply } = vi.mocked(postSlackMessage).mock.calls[0][0] as { text: string }
    expect(reply).toContain(
      "You sent *<https://github.com/acme/one|acme/one>* twice, I'm handling it once.",
    )
    expect(reply).toContain('You linked a page inside *<https://github.com/acme/one|acme/one>*')
    expect(reply).toContain(
      'Skipped gitlab.com/acme/legacy: only GitHub repositories can be added.',
    )
    expect(reply).toContain('I only read the first 10,000 characters.')
    expect(parseOnboardingRequest).toHaveBeenCalledWith(
      expect.stringMatching(/^.{9990,10000}$/s),
      expect.anything(),
    )
  })

  it('still acknowledges the request when the earlier-request refresh fails', async () => {
    vi.mocked(findExistingProjectCatalogRepoUrls).mockResolvedValue(
      new Set(['https://github.com/acme/one']),
    )
    vi.mocked(findProjectCatalogByRepoUrls)
      .mockResolvedValueOnce([
        {
          repoUrl: 'https://github.com/acme/one',
          action: 'review',
          skipReason: null,
          sourceUrl: 'https://acme.slack.com/archives/C1/p1760000000123456',
        } as never,
      ])
      .mockRejectedValueOnce(new Error('db hiccup'))

    await run()

    expect(postSlackMessage).toHaveBeenCalledWith(
      expect.objectContaining({ text: expect.stringContaining('is with the CDP team') }),
    )
    expect(postSlackMessage).not.toHaveBeenCalledWith(
      expect.objectContaining({ text: expect.stringContaining("couldn't record this request") }),
    )
  })

  it('falls back to the GitHub links in the text when the parser fails', async () => {
    vi.mocked(parseOnboardingRequest).mockRejectedValueOnce(new Error('bedrock down'))

    await run()

    expect(log.warn).toHaveBeenCalledWith(
      expect.objectContaining({ channelId: 'C1', error: 'bedrock down' }),
      'Onboarding request parser failed, falling back to the GitHub links in the text.',
    )
    expect(bulkInsertProjectCatalog).toHaveBeenCalledWith(expect.anything(), [
      expect.objectContaining({ repoUrl: 'https://github.com/acme/one', projectName: null }),
    ])
    expect(postSlackMessage).toHaveBeenCalledWith(
      expect.objectContaining({ text: expect.stringContaining('⏳ Checking') }),
    )
  })

  it('still names an unsupported host when the parser is unavailable', async () => {
    vi.mocked(parseOnboardingRequest).mockResolvedValueOnce({ ok: false, reason: 'no answer' })

    await run({ text: '<@U0BOT> please onboard Acme, it lives on gitlab.com/acme/one' })

    expect(bulkInsertProjectCatalog).not.toHaveBeenCalled()
    expect(postSlackMessage).toHaveBeenCalledWith(
      expect.objectContaining({
        text: expect.stringContaining('*gitlab.com/acme/one* is on GitLab'),
      }),
    )
  })

  it('uses a generic missing-link reply when the parser is unavailable and no link was found', async () => {
    vi.mocked(parseOnboardingRequest).mockResolvedValueOnce({ ok: false, reason: 'no answer' })

    await run({ text: '<@U0BOT> please onboard Acme' })

    expect(bulkInsertProjectCatalog).not.toHaveBeenCalled()
    expect(postSlackMessage).toHaveBeenCalledWith(
      expect.objectContaining({
        text: expect.stringContaining('I need at least one GitHub link to add this project.'),
      }),
    )
  })

  it('explains itself when only the mention is left or the user asks for help', async () => {
    await run({ text: '<@U0BOT>' })
    await run({ text: '<@U0BOT> Help' })

    expect(parseOnboardingRequest).not.toHaveBeenCalled()
    expect(bulkInsertProjectCatalog).not.toHaveBeenCalled()
    expect(addSlackReaction).not.toHaveBeenCalled()
    expect(postSlackMessage).toHaveBeenCalledTimes(2)
    expect(postSlackMessage).toHaveBeenCalledWith(
      expect.objectContaining({ text: expect.stringContaining('Hi! I add GitHub repositories') }),
    )
  })

  it('warns when Slack does not deliver the reply', async () => {
    vi.mocked(postSlackMessage).mockResolvedValue({ ok: false, error: 'channel_not_found' })

    await run()

    expect(log.warn).toHaveBeenCalledWith(
      expect.objectContaining({ channelId: 'C1', error: 'channel_not_found' }),
      'Slack bot reply was not delivered.',
    )
  })

  it('fails the request instead of queueing rows without a Slack permalink', async () => {
    vi.mocked(getSlackPermalink).mockResolvedValueOnce(null)

    await run()

    expect(bulkInsertProjectCatalog).not.toHaveBeenCalled()
    expect(postSlackMessage).toHaveBeenCalledWith(
      expect.objectContaining({ text: expect.stringContaining("couldn't record this request") }),
    )
  })

  it('reports a failure in the thread when the request cannot be recorded', async () => {
    vi.mocked(bulkInsertProjectCatalog).mockRejectedValueOnce(new Error('db down'))

    await run()

    expect(log.error).toHaveBeenCalledWith(
      expect.objectContaining({ error: 'db down', channelId: 'C1' }),
      'Slack request failed.',
    )
    expect(postSlackMessage).toHaveBeenCalledWith(
      expect.objectContaining({ text: expect.stringContaining("couldn't record this request") }),
    )
  })
})
