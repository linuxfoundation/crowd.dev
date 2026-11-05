import { beforeEach, describe, expect, it, vi } from 'vitest'

import { WorkflowIdReusePolicy } from '@crowd/temporal'

vi.mock('@crowd/data-access-layer', () => ({
  resolveProjectCatalogReview: vi.fn(),
  findProjectCatalogById: vi.fn(async () => null),
  findProjectCatalogBySourceUrl: vi.fn(async () => []),
}))
vi.mock('../../conf', () => ({ API_CONFIG: { frontendUrl: 'https://cdp.test' } }))
vi.mock('./onboardProjectCommand', () => ({
  postToResponseUrl: vi.fn(async () => undefined),
  textMessage: vi.fn((text: string) => ({ text })),
}))
vi.mock('./slackBackground', () => ({
  getBgQx: vi.fn(async () => ({ tx: (run: (tx: unknown) => Promise<unknown>) => run({}) })),
}))
vi.mock('@crowd/slack', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@crowd/slack')>()),
  openSlackView: vi.fn(async () => ({ ok: true })),
  getSlackBotConfig: vi.fn(() => ({ reviewerUserIds: ['U123'] })),
}))
vi.mock('@crowd/project-onboarding', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@crowd/project-onboarding')>()),
  applyRequestStatus: vi.fn(async () => undefined),
  reportRequestOutcome: vi.fn(async () => ({ ok: true })),
  refreshRequestStatusMessage: vi.fn(async () => false),
}))

import {
  findProjectCatalogById,
  findProjectCatalogBySourceUrl,
  resolveProjectCatalogReview,
} from '@crowd/data-access-layer'
import { applyRequestStatus, reportRequestOutcome } from '@crowd/project-onboarding'
import { getSlackBotConfig, openSlackView } from '@crowd/slack'

import { postToResponseUrl } from './onboardProjectCommand'
import {
  postReviewDecisionFailure,
  runReviewDecisionCommand,
  runReviewRejectPrompt,
} from './reviewDecisionCommand'

const CATALOG_ID = '8f1c2a44-5b7e-4d2f-9c1a-3e6f7a8b9c0d'
const RESPONSE_URL = 'https://hooks.slack.com/response'

const log = { info: vi.fn(), warn: vi.fn(), error: vi.fn() } as any
const startWorkflow = vi.fn(async () => undefined)
const temporal = { workflow: { start: startWorkflow } } as never

const reviewedRow = {
  id: CATALOG_ID,
  repoName: 'kops',
  repoUrl: 'https://github.com/kubernetes/kops',
  projectName: 'Kubernetes',
  classificationKind: 'lf_in_cdp',
  proposedAction: 'update_integration',
  matchedProjectName: 'Kubernetes',
  matchedSegmentId: null,
  decisionReason: null,
  decisionNote: null,
  sourceUrl: null,
  reviewedAt: '2026-10-13T07:00:00.000Z',
} as any

function run(overrides: Record<string, unknown> = {}) {
  return runReviewDecisionCommand({
    catalogId: CATALOG_ID,
    decision: 'approve',
    responseUrl: RESPONSE_URL,
    actorId: 'U123',
    triggerId: null,
    temporal,
    log,
    ...overrides,
  })
}

function replacedMessage() {
  return vi.mocked(postToResponseUrl).mock.calls[0][1]
}

describe('runReviewDecisionCommand', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.mocked(resolveProjectCatalogReview).mockResolvedValue(reviewedRow)
  })

  it('approves the row, starts its onboarding right away and replaces the card', async () => {
    await run()

    expect(resolveProjectCatalogReview).toHaveBeenCalledWith(expect.anything(), CATALOG_ID, {
      decision: 'approve',
      reviewedBy: 'U123',
      decisionReason: null,
      decisionNote: null,
    })
    expect(startWorkflow).toHaveBeenCalledWith('onboardProjects', {
      taskQueue: 'automatic-onboarding',
      workflowId: `onboard-review-${CATALOG_ID}-${Date.parse('2026-10-13T07:00:00.000Z')}`,
      workflowIdReusePolicy: WorkflowIdReusePolicy.REJECT_DUPLICATE,
      args: [{ catalogIds: [CATALOG_ID] }],
    })
    expect(replacedMessage()).toMatchObject({ text: 'Approved — kops', replace_original: true })
    expect(JSON.stringify(replacedMessage())).toContain('onboarding started.')
  })

  it('decides every repository of an organization card together and onboards them in one run', async () => {
    const source = 'https://lfx.slack.com/archives/C1/p1'
    const lead = {
      ...reviewedRow,
      sourceUrl: source,
      reviewMessageTs: '1760000000.100000',
      requestHints: { organization: 'kubernetes' },
    }
    const sibling = (id: string) => ({
      ...lead,
      id,
      action: 'review',
      repoUrl: `https://github.com/kubernetes/${id}`,
    })
    vi.mocked(resolveProjectCatalogReview).mockImplementation(async (_qx, id) =>
      id === CATALOG_ID ? lead : sibling(id),
    )
    vi.mocked(findProjectCatalogBySourceUrl).mockResolvedValue([
      { ...lead, action: 'review' },
      sibling('sib-1'),
      sibling('sib-2'),
      { ...sibling('sib-3'), reviewMessageTs: 'other-card' },
      { ...sibling('sib-4'), requestHints: null },
    ] as never)

    await run()

    expect(resolveProjectCatalogReview).toHaveBeenCalledTimes(3)
    expect(startWorkflow).toHaveBeenCalledWith(
      'onboardProjects',
      expect.objectContaining({ args: [{ catalogIds: [CATALOG_ID, 'sib-1', 'sib-2'] }] }),
    )
    expect(replacedMessage()).toMatchObject({
      text: 'Approved — github.com/kubernetes (3 repositories)',
    })
  })

  it('rejects the row without starting any workflow', async () => {
    await run({ decision: 'reject' })

    expect(resolveProjectCatalogReview).toHaveBeenCalledWith(expect.anything(), CATALOG_ID, {
      decision: 'reject',
      reviewedBy: 'U123',
      decisionReason: null,
      decisionNote: null,
    })
    expect(startWorkflow).not.toHaveBeenCalled()
    expect(replacedMessage()).toMatchObject({ text: 'Rejected — kops', replace_original: true })
    expect(reportRequestOutcome).not.toHaveBeenCalled()
  })

  it('marks a manual row as done without any workflow and tells the requester', async () => {
    const sourceUrl = 'https://acme.slack.com/archives/C0123ABC/p1760000000123456'
    vi.mocked(resolveProjectCatalogReview).mockResolvedValue({
      ...reviewedRow,
      proposedAction: 'human_review',
      action: 'onboarded',
      provenance: 'slack-bot',
      sourceUrl,
      reviewedBy: 'U123',
    })

    await run({ decision: 'done' })

    expect(resolveProjectCatalogReview).toHaveBeenCalledWith(expect.anything(), CATALOG_ID, {
      decision: 'done',
      reviewedBy: 'U123',
      decisionReason: null,
      decisionNote: null,
    })
    expect(startWorkflow).not.toHaveBeenCalled()
    expect(replacedMessage()).toMatchObject({ text: 'Done — kops', replace_original: true })
    expect(JSON.stringify(replacedMessage())).toContain('Marked as done by <@U123>')
    expect(reportRequestOutcome).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ sourceUrl }),
      expect.stringContaining('was added to LFX Insights by hand by <@U123>.'),
    )
    expect(applyRequestStatus).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ id: CATALOG_ID, sourceUrl }),
      'added',
    )
  })

  it('refreshes the card and explains when approve is refused on a row that needs a person', async () => {
    vi.mocked(resolveProjectCatalogReview).mockResolvedValue(null)
    vi.mocked(findProjectCatalogById).mockResolvedValue({
      ...reviewedRow,
      action: 'review',
      proposedAction: 'human_review',
    })

    await run({ decision: 'approve' })

    expect(startWorkflow).not.toHaveBeenCalled()
    expect(replacedMessage()).toMatchObject({
      text: 'Review needed — kubernetes/kops',
      replace_original: true,
    })
    expect(JSON.stringify(replacedMessage())).toContain('mark_done_review')
    expect(JSON.stringify(replacedMessage())).toContain('https://cdp.test/admin#project-groups')
    expect(postToResponseUrl).toHaveBeenLastCalledWith(
      RESPONSE_URL,
      expect.objectContaining({
        text: 'This request needs a person: add the repo by hand in CDP, then use Mark as done. The buttons were refreshed.',
        response_type: 'ephemeral',
      }),
      log,
    )
  })

  it('tells the requester in the Slack thread when a bot request is rejected', async () => {
    const sourceUrl = 'https://acme.slack.com/archives/C0123ABC/p1760000000123456'
    vi.mocked(resolveProjectCatalogReview).mockResolvedValue({
      ...reviewedRow,
      provenance: 'slack-bot',
      sourceUrl,
      reviewedBy: 'U123',
      skipReason: 'rejected in review by U123',
    })

    await run({ decision: 'reject' })

    expect(reportRequestOutcome).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ sourceUrl }),
      expect.stringContaining('<@U123> reviewed it and rejected the request.'),
    )
  })

  it('does not reply in the thread when a bot request is approved', async () => {
    vi.mocked(resolveProjectCatalogReview).mockResolvedValue({
      ...reviewedRow,
      provenance: 'slack-bot',
      sourceUrl: 'https://acme.slack.com/archives/C0123ABC/p1760000000123456',
    })

    await run()

    expect(reportRequestOutcome).not.toHaveBeenCalled()
  })

  it('starts a new workflow for a later review cycle of the same row', async () => {
    vi.mocked(resolveProjectCatalogReview).mockResolvedValue({
      ...reviewedRow,
      reviewedAt: '2026-10-14T07:00:00.000Z',
    })

    await run()

    expect(startWorkflow).toHaveBeenCalledWith(
      'onboardProjects',
      expect.objectContaining({
        workflowId: `onboard-review-${CATALOG_ID}-${Date.parse('2026-10-14T07:00:00.000Z')}`,
      }),
    )
  })

  it('tolerates an onboarding workflow that was already started', async () => {
    const alreadyStarted = new Error('already started')
    alreadyStarted.name = 'WorkflowExecutionAlreadyStartedError'
    startWorkflow.mockRejectedValueOnce(alreadyStarted)

    await run()

    expect(replacedMessage()).toMatchObject({ text: 'Approved — kops' })
  })

  it('leaves the approved row to the schedule when temporal is not configured', async () => {
    await run({ temporal: null })

    expect(startWorkflow).not.toHaveBeenCalled()
    expect(log.warn).toHaveBeenCalled()
    expect(replacedMessage()).toMatchObject({ text: 'Approved — kops' })
    expect(JSON.stringify(replacedMessage())).toContain('queued for the next onboarding run.')
  })

  it('keeps the approval when starting the onboarding workflow fails', async () => {
    startWorkflow.mockRejectedValueOnce(new Error('temporal down'))

    await run()

    expect(log.error).toHaveBeenCalled()
    const calls = vi.mocked(postToResponseUrl).mock.calls
    const last = calls[calls.length - 1][1]
    expect(last).toMatchObject({ text: 'Approved — kops' })
    expect(JSON.stringify(last)).toContain('queued for the next onboarding run.')
  })

  it('replaces the card before the onboarding workflow starts', async () => {
    await run()

    const replaced = vi.mocked(postToResponseUrl).mock.invocationCallOrder[0]
    const started = startWorkflow.mock.invocationCallOrder[0]
    expect(replaced).toBeLessThan(started)
    expect(postToResponseUrl).toHaveBeenCalledTimes(1)
  })

  it('tells the reviewer when the row is no longer pending review', async () => {
    vi.mocked(resolveProjectCatalogReview).mockResolvedValue(null)
    vi.mocked(findProjectCatalogById).mockResolvedValue(null)

    await run()

    expect(startWorkflow).not.toHaveBeenCalled()
    expect(replacedMessage()).toMatchObject({
      text: 'This request was already reviewed or is no longer pending review.',
      response_type: 'ephemeral',
      replace_original: false,
    })
  })

  it('says who already decided, when and why, without touching the card', async () => {
    vi.mocked(resolveProjectCatalogReview).mockResolvedValue(null)
    vi.mocked(findProjectCatalogById).mockResolvedValue({
      ...reviewedRow,
      action: 'skip',
      reviewedBy: 'U777',
      reviewedAt: '2026-10-15T14:39:00.000Z',
      decisionReason: 'archived_or_fork',
    })

    await run({ decision: 'approve' })

    expect(postToResponseUrl).toHaveBeenCalledTimes(1)
    expect(replacedMessage()).toMatchObject({
      text: '<@U777> already rejected this at <!date^1792075140^{time}|14:39 UTC> (Archived, fork or example repo).',
      response_type: 'ephemeral',
      replace_original: false,
    })
  })

  it('stores the rejection reason and passes it with the note to the requester', async () => {
    const sourceUrl = 'https://acme.slack.com/archives/C0123ABC/p1760000000123456'
    vi.mocked(resolveProjectCatalogReview).mockResolvedValue({
      ...reviewedRow,
      action: 'skip',
      provenance: 'slack-bot',
      sourceUrl,
      reviewedBy: 'U123',
      reviewedAt: '2026-10-15T14:39:00.000Z',
      decisionReason: 'wrong_project',
      decisionNote: 'Belongs to another project.',
    })

    await run({
      decision: 'reject',
      reason: { decisionReason: 'wrong_project', decisionNote: 'Belongs to another project.' },
    })

    expect(resolveProjectCatalogReview).toHaveBeenCalledWith(expect.anything(), CATALOG_ID, {
      decision: 'reject',
      reviewedBy: 'U123',
      decisionReason: 'wrong_project',
      decisionNote: 'Belongs to another project.',
    })
    expect(JSON.stringify(replacedMessage())).toContain(
      'Rejected by <@U123> at <!date^1792075140^{time}|14:39 UTC> · Wrong project',
    )
    expect(reportRequestOutcome).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ sourceUrl }),
      [
        "🚫 *<https://github.com/kubernetes/kops|kubernetes/kops>* wasn't added. <@U123> reviewed it: *Wrong project*.",
        '> Belongs to another project.',
        'Reply here if this looks wrong.',
      ].join('\n'),
    )
  })

  it('rejects an invalid catalog id before touching the database', async () => {
    await run({ catalogId: 'not-a-uuid' })

    expect(resolveProjectCatalogReview).not.toHaveBeenCalled()
    expect(replacedMessage()).toMatchObject({ text: ':no_entry: This request is no longer valid.' })
  })
})

describe('postReviewDecisionFailure', () => {
  it('keeps the review card in place so the reviewer can retry', async () => {
    vi.clearAllMocks()
    await postReviewDecisionFailure(RESPONSE_URL, log)

    const message = vi.mocked(postToResponseUrl).mock.calls[0][1]
    expect(message).not.toHaveProperty('replace_original')
    expect(JSON.stringify(message)).toContain('Please try again')
  })

  it('puts the request back in progress after an approval', async () => {
    vi.clearAllMocks()
    vi.mocked(resolveProjectCatalogReview).mockResolvedValue({
      ...reviewedRow,
      provenance: 'slack-bot',
      sourceUrl: 'https://acme.slack.com/archives/C0123ABC/p1760000000123456',
    })

    await run({ decision: 'approve' })

    expect(applyRequestStatus).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ id: CATALOG_ID }),
      'checking',
    )
    expect(vi.mocked(applyRequestStatus).mock.invocationCallOrder[0]).toBeLessThan(
      startWorkflow.mock.invocationCallOrder[0],
    )
  })

  it('marks the request as not added after a rejection reply', async () => {
    const sourceUrl = 'https://acme.slack.com/archives/C0123ABC/p1760000000123456'
    vi.mocked(resolveProjectCatalogReview).mockResolvedValue({
      ...reviewedRow,
      provenance: 'slack-bot',
      sourceUrl,
      reviewedBy: 'U123',
    })

    await run({ decision: 'reject' })

    expect(applyRequestStatus).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ id: CATALOG_ID, sourceUrl }),
      'notAdded',
    )
  })

  it('ignores a click from outside the reviewer allowlist with an ephemeral notice', async () => {
    vi.clearAllMocks()
    vi.mocked(getSlackBotConfig).mockReturnValue({ reviewerUserIds: ['U999'] } as never)

    await run({ decision: 'approve', actorId: 'U123' })

    expect(resolveProjectCatalogReview).not.toHaveBeenCalled()
    expect(openSlackView).not.toHaveBeenCalled()
    expect(postToResponseUrl).toHaveBeenCalledWith(
      RESPONSE_URL,
      expect.objectContaining({
        text: 'Only the CDP team can approve, reject or mark requests as done. If you need this one moved along, ask in this thread.',
        response_type: 'ephemeral',
        replace_original: false,
      }),
      log,
    )
  })

  it('shows the not-allowed notice as a modal when the click carries a trigger id', async () => {
    vi.clearAllMocks()
    vi.mocked(getSlackBotConfig).mockReturnValue({ reviewerUserIds: ['U999'] } as never)

    await run({ decision: 'approve', actorId: 'U123', triggerId: 'trigger-1' })

    expect(openSlackView).toHaveBeenCalledWith(
      'trigger-1',
      expect.objectContaining({ callback_id: 'review_not_allowed_modal' }),
    )
    expect(postToResponseUrl).not.toHaveBeenCalled()
  })

  it('falls back to the ephemeral notice when the modal cannot be opened', async () => {
    vi.clearAllMocks()
    vi.mocked(getSlackBotConfig).mockReturnValue({ reviewerUserIds: ['U999'] } as never)
    vi.mocked(openSlackView).mockResolvedValueOnce({ ok: false, error: 'expired_trigger_id' })

    await run({ decision: 'approve', actorId: 'U123', triggerId: 'trigger-1' })

    expect(postToResponseUrl).toHaveBeenCalledWith(
      RESPONSE_URL,
      expect.objectContaining({ response_type: 'ephemeral' }),
      log,
    )
  })

  it('refuses every decision when no reviewer is configured', async () => {
    vi.clearAllMocks()
    vi.mocked(getSlackBotConfig).mockReturnValue({ reviewerUserIds: [] } as never)

    await run({ decision: 'approve', actorId: 'U123' })

    expect(resolveProjectCatalogReview).not.toHaveBeenCalled()
    expect(log.warn).toHaveBeenCalledWith(
      'CROWD_SLACK_REVIEWER_USER_IDS is not set, every review decision is refused.',
    )
  })
})

describe('runReviewRejectPrompt', () => {
  function prompt(overrides: Record<string, unknown> = {}) {
    return runReviewRejectPrompt({
      catalogId: CATALOG_ID,
      responseUrl: RESPONSE_URL,
      actorId: 'U123',
      triggerId: 'trigger-1',
      log,
      ...overrides,
    })
  }

  beforeEach(() => {
    vi.clearAllMocks()
    vi.mocked(getSlackBotConfig).mockReturnValue({ reviewerUserIds: ['U123'] } as never)
    vi.mocked(findProjectCatalogById).mockResolvedValue({ ...reviewedRow, action: 'review' })
  })

  it('opens the reason modal for a reviewer and leaves the row untouched', async () => {
    await prompt()

    expect(openSlackView).toHaveBeenCalledWith(
      'trigger-1',
      expect.objectContaining({
        callback_id: 'reject_review_modal',
        private_metadata: JSON.stringify({
          catalogId: CATALOG_ID,
          repoName: 'kops',
          responseUrl: RESPONSE_URL,
        }),
      }),
    )
    expect(resolveProjectCatalogReview).not.toHaveBeenCalled()
    expect(postToResponseUrl).not.toHaveBeenCalled()
  })

  it('shows the not-allowed modal to anyone outside the allowlist', async () => {
    await prompt({ actorId: 'U999' })

    expect(openSlackView).toHaveBeenCalledWith(
      'trigger-1',
      expect.objectContaining({ callback_id: 'review_not_allowed_modal' }),
    )
    expect(findProjectCatalogById).not.toHaveBeenCalled()
  })

  it('explains an already decided row instead of opening the modal', async () => {
    vi.mocked(findProjectCatalogById).mockResolvedValue({
      ...reviewedRow,
      action: 'onboarded',
      reviewedBy: 'U777',
      reviewedAt: '2026-10-15T14:41:00.000Z',
    })

    await prompt()

    expect(openSlackView).not.toHaveBeenCalled()
    expect(replacedMessage()).toMatchObject({
      text: '<@U777> already approved this at <!date^1792075260^{time}|14:41 UTC>.',
      response_type: 'ephemeral',
    })
  })

  it('asks to retry when the modal cannot be opened', async () => {
    vi.mocked(openSlackView).mockResolvedValueOnce({ ok: false, error: 'expired_trigger_id' })

    await prompt()

    expect(replacedMessage()).toMatchObject({
      text: 'The reject form could not be opened. Please try again.',
      response_type: 'ephemeral',
    })
  })

  it('rejects an invalid catalog id before touching the database', async () => {
    await prompt({ catalogId: 'not-a-uuid' })

    expect(findProjectCatalogById).not.toHaveBeenCalled()
    expect(openSlackView).not.toHaveBeenCalled()
  })
})
