import { beforeEach, describe, expect, it, vi } from 'vitest'

vi.mock('@crowd/data-access-layer', () => ({
  findProjectCatalogById: vi.fn(),
  findProjectCatalogBySourceUrl: vi.fn(async () => []),
  answerProjectCatalogQuestion: vi.fn(),
}))
vi.mock('./onboardProjectCommand', () => ({
  postToResponseUrl: vi.fn(async () => undefined),
  textMessage: vi.fn((text: string) => ({ text })),
}))
vi.mock('./slackBackground', () => ({
  getBgQx: vi.fn(async () => ({ tx: (run: (tx: unknown) => Promise<unknown>) => run({}) })),
}))
vi.mock('@crowd/project-onboarding', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@crowd/project-onboarding')>()),
  applyRequestStatus: vi.fn(async () => undefined),
  refreshRequestStatusMessage: vi.fn(async () => true),
}))
vi.mock('@crowd/slack', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@crowd/slack')>()),
  getSlackBotConfig: vi.fn(() => ({ reviewerUserIds: ['U-REVIEWER'] })),
}))

import {
  answerProjectCatalogQuestion,
  findProjectCatalogById,
  findProjectCatalogBySourceUrl,
} from '@crowd/data-access-layer'
import { applyRequestStatus, refreshRequestStatusMessage } from '@crowd/project-onboarding'

import { runAnswerRequestCommand } from './answerRequestCommand'
import { postToResponseUrl } from './onboardProjectCommand'

const CATALOG_ID = '8f1c2a44-5b7e-4d2f-9c1a-3e6f7a8b9c0d'
const RESPONSE_URL = 'https://hooks.slack.com/response'
const log = { info: vi.fn(), warn: vi.fn(), error: vi.fn() } as any
const startWorkflow = vi.fn(async () => undefined)
const temporal = { workflow: { start: startWorkflow } } as never

const question = {
  kind: 'did_you_mean' as const,
  text: 'Did you mean Acme?',
  options: [
    {
      id: 'yes',
      label: 'Yes, Acme',
      hints: { pccProject: { projectId: 'p', name: 'Acme', slug: 'a', isLeaf: true } },
    },
    { id: 'no', label: 'No, ask the CDP team', handOff: true },
  ],
}
const row = {
  id: CATALOG_ID,
  repoUrl: 'https://github.com/acme/widget',
  sourceUrl: 'https://acme.slack.com/archives/C1/p1',
  action: 'ask',
  requestedBy: 'U-REQUESTER',
  projectName: 'Acme Widgets',
  requestQuestion: question,
} as any

function run(overrides: Record<string, unknown> = {}) {
  return runAnswerRequestCommand({
    value: `${CATALOG_ID}|yes`,
    responseUrl: RESPONSE_URL,
    actorId: 'U-REQUESTER',
    temporal,
    log,
    ...overrides,
  })
}

function lastMessage() {
  const calls = vi.mocked(postToResponseUrl).mock.calls
  return calls[calls.length - 1][1]
}

describe('runAnswerRequestCommand', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.mocked(findProjectCatalogById).mockResolvedValue(row)
    vi.mocked(answerProjectCatalogQuestion).mockResolvedValue({ ...row, action: 'evaluate' })
  })

  it('answers the shared question of an organization for every repository at once', async () => {
    const lead = {
      ...row,
      requestHints: { organization: 'acme' },
      requestQuestion: { ...question, messageTs: '1760000000.300000' },
    }
    const sibling = (id: string, messageTs = '1760000000.300000') => ({
      ...lead,
      id,
      repoUrl: `https://github.com/acme/${id}`,
      requestQuestion: { ...question, messageTs },
    })
    vi.mocked(findProjectCatalogById).mockResolvedValue(lead)
    vi.mocked(findProjectCatalogBySourceUrl).mockResolvedValue([
      lead,
      sibling('sib-1'),
      sibling('sib-2'),
      sibling('sib-3', 'other-question'),
    ] as never)
    vi.mocked(answerProjectCatalogQuestion).mockImplementation(async (_qx, id) => ({
      ...lead,
      id,
      action: 'evaluate',
    }))

    await run()

    expect(answerProjectCatalogQuestion).toHaveBeenCalledTimes(3)
    expect(JSON.stringify(lastMessage())).toContain('*github.com/acme* (3 repositories):')
    expect(startWorkflow).toHaveBeenCalledWith(
      'evaluateProjects',
      expect.objectContaining({ args: [{ catalogIds: [CATALOG_ID, 'sib-1', 'sib-2'] }] }),
    )
  })

  it('stores the answer, replaces the question and re-runs the check', async () => {
    await run()

    expect(answerProjectCatalogQuestion).toHaveBeenCalledWith(expect.anything(), CATALOG_ID, {
      answer: 'yes',
      answeredBy: 'U-REQUESTER',
      hints: { pccProject: { projectId: 'p', name: 'Acme', slug: 'a', isLeaf: true } },
      action: 'evaluate',
      reviewReason: null,
      proposedAction: null,
    })
    expect(lastMessage()).toMatchObject({ replace_original: true })
    expect(JSON.stringify(lastMessage())).toContain('<@U-REQUESTER> answered: *Yes, Acme*')
    expect(refreshRequestStatusMessage).toHaveBeenCalled()
    expect(applyRequestStatus).toHaveBeenCalledWith(
      expect.anything(),
      expect.anything(),
      'checking',
    )
    expect(startWorkflow).toHaveBeenCalledWith(
      'evaluateProjects',
      expect.objectContaining({ args: [{ catalogIds: [CATALOG_ID] }] }),
    )
  })

  it('hands the row to the CDP team on a hand-off option', async () => {
    vi.mocked(answerProjectCatalogQuestion).mockResolvedValue({ ...row, action: 'review' })

    await run({ value: `${CATALOG_ID}|no`, actorId: 'U-REVIEWER' })

    expect(answerProjectCatalogQuestion).toHaveBeenCalledWith(
      expect.anything(),
      CATALOG_ID,
      expect.objectContaining({
        action: 'review',
        reviewReason: "the requester said Acme Widgets isn't Acme",
        proposedAction: 'human_review',
      }),
    )
    expect(applyRequestStatus).toHaveBeenCalledWith(
      expect.anything(),
      expect.anything(),
      'withTeam',
    )
  })

  it('refuses answers from anyone but the requester or a reviewer', async () => {
    await run({ actorId: 'U-OTHER' })

    expect(answerProjectCatalogQuestion).not.toHaveBeenCalled()
    expect(lastMessage()).toMatchObject({
      text: 'Only <@U-REQUESTER> or the CDP team can answer this.',
      response_type: 'ephemeral',
    })
  })

  it('tells who already answered when the question is closed', async () => {
    vi.mocked(findProjectCatalogById).mockResolvedValue({
      ...row,
      action: 'evaluate',
      requestQuestion: { ...question, answeredBy: 'U-REQUESTER', answer: 'yes' },
    })

    await run()

    expect(answerProjectCatalogQuestion).not.toHaveBeenCalled()
    expect(lastMessage()).toMatchObject({
      text: '<@U-REQUESTER> already answered this: Yes, Acme.',
      response_type: 'ephemeral',
    })
  })

  it('rejects an invalid value before touching the database', async () => {
    await run({ value: 'nope' })

    expect(findProjectCatalogById).not.toHaveBeenCalled()
    expect(lastMessage()).toMatchObject({ response_type: 'ephemeral' })
  })
})
