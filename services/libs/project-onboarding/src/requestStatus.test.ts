import { beforeEach, describe, expect, it, vi } from 'vitest'

const {
  findSlackOnboardingRequest,
  findProjectCatalogBySourceUrl,
  claimSlackOnboardingRequestDone,
  lockSlackOnboardingRequest,
  updateSlackMessage,
  postSlackContentInThread,
} = vi.hoisted(() => ({
  findSlackOnboardingRequest: vi.fn(),
  findProjectCatalogBySourceUrl: vi.fn(),
  claimSlackOnboardingRequestDone: vi.fn(),
  lockSlackOnboardingRequest: vi.fn(async () => undefined),
  updateSlackMessage: vi.fn(),
  postSlackContentInThread: vi.fn(),
}))
vi.mock('@crowd/data-access-layer', () => ({
  findSlackOnboardingRequest,
  findProjectCatalogBySourceUrl,
  claimSlackOnboardingRequestDone,
  lockSlackOnboardingRequest,
}))

const tx = { marker: 'tx' }
const qx = { tx: async (run: (inner: unknown) => Promise<unknown>) => run(tx) } as never
vi.mock('@crowd/slack', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@crowd/slack')>()),
  updateSlackMessage,
  postSlackContentInThread,
}))

import {
  RequestStatusRow,
  buildAllDoneReply,
  buildRequestStatusContent,
  isRequestSettled,
  refreshRequestStatusMessage,
  reportRequestOutcome,
  requestStatusLine,
  requestSummaryLine,
} from './requestStatus'

function row(overrides: Partial<RequestStatusRow> & { repoUrl: string }): RequestStatusRow {
  return {
    action: 'auto',
    classificationKind: null,
    reviewReason: null,
    skipReason: null,
    evaluationReason: null,
    requestQuestion: null,
    matchedProjectName: null,
    reviewedBy: null,
    decisionReason: null,
    decisionNote: null,
    requestHints: null,
    ...overrides,
  }
}

const ONE = 'https://github.com/acme/one'
const TWO = 'https://github.com/acme/two'
const sourceUrl = 'https://acme.slack.com/archives/C1/p1760000000123456'
const request = { id: 'row-1', repoUrl: ONE, sourceUrl }

describe('requestStatusLine', () => {
  it('shows one labelled line per state', () => {
    expect(requestStatusLine(row({ repoUrl: ONE }))).toBe(
      '⏳ *<https://github.com/acme/one|acme/one>* · checking…',
    )
    expect(
      requestStatusLine(
        row({ repoUrl: ONE, action: 'review', reviewReason: 'Acme needs a <look>' }),
      ),
    ).toBe(
      '👀 *<https://github.com/acme/one|acme/one>* · with the CDP team: Acme needs a &lt;look&gt;',
    )
    expect(
      requestStatusLine(row({ repoUrl: ONE, action: 'onboarded', matchedProjectName: 'Acme' })),
    ).toBe('✅ *<https://github.com/acme/one|acme/one>* · added to *Acme*')
    expect(
      requestStatusLine(
        row({ repoUrl: ONE, action: 'skip', classificationKind: 'already_tracked' }),
      ),
    ).toBe('📘 *<https://github.com/acme/one|acme/one>* · already in LFX Insights')
    expect(
      requestStatusLine(
        row({
          repoUrl: ONE,
          action: 'skip',
          skipReason: 'rejected in review by U123',
          reviewedBy: 'U123',
          decisionReason: 'wrong_project',
          decisionNote: 'Belongs to <Beta>.',
        }),
      ),
    ).toBe(
      '🚫 *<https://github.com/acme/one|acme/one>* · not added: <@U123> reviewed it: Wrong project\n> Belongs to &lt;Beta&gt;.',
    )
    expect(requestStatusLine(row({ repoUrl: ONE, action: 'error' }))).toBe(
      '⚠️ *<https://github.com/acme/one|acme/one>* · something went wrong on our side',
    )
    expect(
      requestStatusLine(row({ repoUrl: ONE, action: 'skip', evaluationReason: 'a docs repo' })),
    ).toBe('🚫 *<https://github.com/acme/one|acme/one>* · not added: a docs repo')
    expect(
      requestStatusLine(
        row({
          repoUrl: ONE,
          action: 'ask',
          requestQuestion: { kind: 'did_you_mean', text: 'Did you mean <Acme>?', options: [] },
        }),
      ),
    ).toBe(
      '❓ *<https://github.com/acme/one|acme/one>* · needs your input: Did you mean &lt;Acme&gt;?',
    )
  })

  it('shortens a long reason', () => {
    const line = requestStatusLine(
      row({ repoUrl: ONE, action: 'review', reviewReason: 'x'.repeat(300) }),
    )

    expect(line).toContain(`${'x'.repeat(160)}…`)
  })
})

describe('summary and settlement', () => {
  const rows = [
    row({ repoUrl: ONE, action: 'onboarded' }),
    row({ repoUrl: TWO, action: 'review' }),
    row({ repoUrl: 'https://github.com/acme/three', action: 'skip' }),
  ]

  it('counts the states in a fixed order and includes the tracked repositories', () => {
    expect(requestSummaryLine(rows, 1)).toBe(
      '1 added, 1 already there, 1 with the CDP team, 1 not added',
    )
    expect(requestSummaryLine([row({ repoUrl: ONE })])).toBe('1 checking')
    expect(buildAllDoneReply(rows)).toBe('✅ All done: 1 added, 1 with the CDP team, 1 not added.')
  })

  it('is settled only when nothing is being checked, reviewed or asked', () => {
    expect(isRequestSettled(rows)).toBe(false)
    expect(isRequestSettled([rows[0], row({ repoUrl: TWO, action: 'ask' })])).toBe(false)
    expect(requestSummaryLine([row({ repoUrl: TWO, action: 'ask' })])).toBe('1 waiting for you')
    expect(isRequestSettled([rows[0], rows[2], row({ repoUrl: TWO, action: 'error' })])).toBe(true)
  })
})

describe('buildRequestStatusContent', () => {
  const header = {
    lines: [
      '📘 *<https://github.com/acme/zero|acme/zero>* is already in LFX Insights under *Acme*.',
    ],
    buttons: [{ text: 'View Acme in LFX Insights', url: 'https://insights.test/acme' }],
    alreadyTracked: 1,
  }

  it('keeps the header lines, adds one line per row, a summary and the buttons', () => {
    const content = buildRequestStatusContent({
      header,
      rows: [row({ repoUrl: ONE, action: 'onboarded' }), row({ repoUrl: TWO })],
      alreadyTracked: 1,
    })

    expect(content.text.split('\n')).toEqual([
      header.lines[0],
      '✅ *<https://github.com/acme/one|acme/one>* · added',
      '⏳ *<https://github.com/acme/two|acme/two>* · checking…',
      '_1 added, 1 already there, 1 checking_',
    ])
    expect(JSON.stringify(content.blocks)).toContain('"action_id":"view_insights_project:0"')
  })

  it('keeps a single repository of an organization as a repository line', () => {
    const content = buildRequestStatusContent({
      header: { lines: [], buttons: [], alreadyTracked: 0 },
      rows: [row({ repoUrl: ONE, requestHints: { organization: 'acme' } })],
    })

    expect(content.text).toBe('⏳ *<https://github.com/acme/one|acme/one>* · checking…')
  })

  it('folds the repositories of an organization request into one line', () => {
    const org = (repoUrl: string, overrides: Partial<RequestStatusRow> = {}) =>
      row({ repoUrl, requestHints: { organization: 'acme' }, ...overrides })
    const waiting = buildRequestStatusContent({
      header: { lines: [], buttons: [], alreadyTracked: 0 },
      rows: [
        org(ONE, { action: 'review', reviewReason: 'Acme is in LFX Insights, but needs a check' }),
        row({ repoUrl: 'https://github.com/other/solo' }),
        org(TWO, { action: 'review', reviewReason: 'Acme is in LFX Insights, but needs a check' }),
      ],
    })
    expect(waiting.text.split('\n')).toEqual([
      '👀 *github.com/acme* · 2 repositories · with the CDP team: Acme is in LFX Insights, but needs a check',
      '⏳ *<https://github.com/other/solo|other/solo>* · checking…',
      '_2 with the CDP team, 1 checking_',
    ])

    const mixed = buildRequestStatusContent({
      header: { lines: [], buttons: [], alreadyTracked: 0 },
      rows: [
        org(ONE, { action: 'onboarded', matchedProjectName: 'Acme' }),
        org(TWO, { action: 'skip' }),
      ],
    })
    expect(mixed.text.split('\n')).toEqual([
      '✅ *github.com/acme* · 2 repositories: 1 added, 1 not added',
      '_1 added, 1 not added_',
    ])

    const nothingAdded = buildRequestStatusContent({
      header: { lines: [], buttons: [], alreadyTracked: 0 },
      rows: [
        org(ONE, { action: 'skip', classificationKind: 'already_tracked' }),
        org(TWO, { action: 'skip' }),
      ],
    })
    expect(nothingAdded.text.split('\n')[0]).toBe(
      '📘 *github.com/acme* · 2 repositories: 1 already there, 1 not added',
    )

    const rejected = buildRequestStatusContent({
      header: { lines: [], buttons: [], alreadyTracked: 0 },
      rows: [
        org(ONE, {
          action: 'skip',
          skipReason: 'rejected in review by U1',
          decisionNote: 'Use the mirror',
        }),
        org(TWO, {
          action: 'skip',
          skipReason: 'rejected in review by U1',
          decisionNote: 'Use the mirror',
        }),
      ],
    })
    expect(rejected.text).toContain('🚫 *github.com/acme* · 2 repositories · not added: ')
    expect(rejected.text).toContain('\n> Use the mirror')
  })

  it('keeps the whole message under the Slack block limit', () => {
    const content = buildRequestStatusContent({
      header: {
        lines: [],
        buttons: Array.from({ length: 300 }, (_, i) => ({ text: `b${i}`, url: `https://x/${i}` })),
        alreadyTracked: 300,
      },
      rows: [row({ repoUrl: ONE })],
    })

    expect(content.blocks?.length).toBeLessThanOrEqual(50)
  })

  it('skips the summary for a single repository and adds the closing line when asked', () => {
    const content = buildRequestStatusContent({
      header: { lines: [], buttons: [], alreadyTracked: 0 },
      rows: [row({ repoUrl: ONE })],
      closing: 'I will update this message.',
    })

    expect(content).toEqual({
      text: '⏳ *<https://github.com/acme/one|acme/one>* · checking…\nI will update this message.',
    })
  })
})

describe('refreshRequestStatusMessage and reportRequestOutcome', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    updateSlackMessage.mockResolvedValue({ ok: true })
    postSlackContentInThread.mockResolvedValue({ ok: true })
    claimSlackOnboardingRequestDone.mockResolvedValue(true)
  })

  it('edits the status message from the rows of the request', async () => {
    findSlackOnboardingRequest.mockResolvedValue({
      sourceUrl,
      channel: 'C1',
      messageTs: '1.1',
      header: { lines: [], buttons: [], alreadyTracked: 0 },
    })
    findProjectCatalogBySourceUrl.mockResolvedValue([row({ repoUrl: ONE, action: 'onboarded' })])

    expect(await reportRequestOutcome(qx, request, 'fallback')).toEqual({ ok: true })

    expect(updateSlackMessage).toHaveBeenCalledWith({
      channel: 'C1',
      ts: '1.1',
      text: '✅ *<https://github.com/acme/one|acme/one>* · added',
    })
    expect(lockSlackOnboardingRequest).toHaveBeenCalledWith(tx, sourceUrl)
    expect(findProjectCatalogBySourceUrl).toHaveBeenCalledWith(tx, sourceUrl)
    expect(postSlackContentInThread).not.toHaveBeenCalled()
  })

  it('counts the tracked repositories of the request in the summary and the all-done reply', async () => {
    findSlackOnboardingRequest.mockResolvedValue({
      sourceUrl,
      channel: 'C1',
      messageTs: '1.1',
      header: { lines: ['📘 tracked'], buttons: [], alreadyTracked: 1 },
    })
    findProjectCatalogBySourceUrl.mockResolvedValue([row({ repoUrl: ONE, action: 'onboarded' })])

    await refreshRequestStatusMessage(qx, request)

    expect(updateSlackMessage).toHaveBeenCalledWith(
      expect.objectContaining({ text: expect.stringContaining('_1 added, 1 already there_') }),
    )
    expect(postSlackContentInThread).toHaveBeenCalledWith(sourceUrl, {
      text: '✅ All done: 1 added, 1 already there.',
    })
  })

  it('announces once when a multi-repo request is fully settled', async () => {
    findSlackOnboardingRequest.mockResolvedValue({
      sourceUrl,
      channel: 'C1',
      messageTs: '1.1',
      header: { lines: [], buttons: [], alreadyTracked: 0 },
    })
    findProjectCatalogBySourceUrl.mockResolvedValue([
      row({ repoUrl: ONE, action: 'onboarded' }),
      row({ repoUrl: TWO, action: 'skip' }),
    ])

    await refreshRequestStatusMessage(qx, request)
    expect(claimSlackOnboardingRequestDone).toHaveBeenCalledWith(tx, sourceUrl)

    findSlackOnboardingRequest.mockResolvedValue({
      sourceUrl,
      channel: 'C1',
      messageTs: '1.1',
      header: { lines: [], buttons: [], alreadyTracked: 0 },
      doneNotifiedAt: '2026-10-15T12:00:00.000Z',
    })
    await refreshRequestStatusMessage(qx, request)

    expect(postSlackContentInThread).toHaveBeenCalledTimes(1)
    expect(postSlackContentInThread).toHaveBeenCalledWith(sourceUrl, {
      text: '✅ All done: 1 added, 1 not added.',
    })
  })

  it('leaves the all-done reply unclaimed when Slack refuses it, so it is retried', async () => {
    findSlackOnboardingRequest.mockResolvedValue({
      sourceUrl,
      channel: 'C1',
      messageTs: '1.1',
      header: { lines: [], buttons: [], alreadyTracked: 0 },
      doneNotifiedAt: null,
    })
    findProjectCatalogBySourceUrl.mockResolvedValue([
      row({ repoUrl: ONE, action: 'onboarded' }),
      row({ repoUrl: TWO, action: 'skip' }),
    ])
    postSlackContentInThread.mockResolvedValueOnce({ ok: false, error: 'ratelimited' })

    await refreshRequestStatusMessage(qx, request)
    expect(claimSlackOnboardingRequestDone).not.toHaveBeenCalled()

    await refreshRequestStatusMessage(qx, request)
    expect(postSlackContentInThread).toHaveBeenCalledTimes(2)
    expect(claimSlackOnboardingRequestDone).toHaveBeenCalledTimes(1)
  })

  it('falls back to a thread reply for requests without a status message', async () => {
    findSlackOnboardingRequest.mockResolvedValue(null)

    expect(await reportRequestOutcome(qx, request, { text: 'hello' })).toEqual({
      ok: true,
    })

    expect(updateSlackMessage).not.toHaveBeenCalled()
    expect(postSlackContentInThread).toHaveBeenCalledWith(sourceUrl, { text: 'hello' })
  })

  it('falls back when the edit fails and reports rows without a Slack origin', async () => {
    findSlackOnboardingRequest.mockResolvedValue({
      sourceUrl,
      channel: 'C1',
      messageTs: '1.1',
      header: { lines: [], buttons: [], alreadyTracked: 0 },
    })
    findProjectCatalogBySourceUrl.mockResolvedValue([row({ repoUrl: ONE })])
    updateSlackMessage.mockResolvedValue({ ok: false, error: 'message_not_found' })

    expect(await reportRequestOutcome(qx, request, 'hello')).toEqual({ ok: true })
    expect(postSlackContentInThread).toHaveBeenCalledWith(sourceUrl, { text: 'hello' })

    expect(await reportRequestOutcome(qx, { ...request, sourceUrl: null }, 'x')).toEqual({
      ok: false,
      error: 'request has no Slack origin',
    })
  })
})
