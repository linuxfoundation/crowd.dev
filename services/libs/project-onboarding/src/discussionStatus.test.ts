import { beforeEach, describe, expect, it, vi } from 'vitest'

import { GithubPublicClientError } from '@crowd/common'

vi.mock('@crowd/data-access-layer', () => ({
  claimDiscussionOnboardingRequestDone: vi.fn(),
  findDiscussionOnboardingRequest: vi.fn(),
  findProjectCatalogBySourceUrl: vi.fn(),
  lockDiscussionOnboardingRequest: vi.fn(),
  upsertDiscussionOnboardingRequest: vi.fn(),
}))

import {
  claimDiscussionOnboardingRequestDone,
  findDiscussionOnboardingRequest,
  findProjectCatalogBySourceUrl,
  lockDiscussionOnboardingRequest,
  upsertDiscussionOnboardingRequest,
} from '@crowd/data-access-layer'
import { IDbProjectCatalog } from '@crowd/data-access-layer/src/project-catalog/types'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import {
  IDiscussionCommentClient,
  buildDiscussionAllDoneMarkdown,
  buildDiscussionStatusMarkdown,
  refreshDiscussionStatusComment,
} from './discussionStatus'
import { RequestStatusRow } from './requestStatus'

const sourceUrl = 'https://github.com/linuxfoundation/insights/discussions/2496'

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

const group = { project: 'Apache HugeGraph', mainRepoUrl: 'https://github.com/apache/hugegraph' }

describe('buildDiscussionStatusMarkdown', () => {
  it('lists one line per repository with its state and the closing line while pending', () => {
    const markdown = buildDiscussionStatusMarkdown([
      row({ repoUrl: 'https://github.com/acme/one' }),
      row({
        repoUrl: 'https://github.com/acme/two',
        action: 'review',
        reviewReason:
          'Acme is in LFX Insights, but adding to it needs a quick check by the CDP team.',
      }),
      row({
        repoUrl: 'https://github.com/acme/three',
        action: 'onboarded',
        matchedProjectName: 'Acme',
      }),
    ])

    expect(markdown.split('\n')).toEqual([
      '**LFX Insights onboarding status**',
      '',
      '- ⏳ [acme/one](https://github.com/acme/one) · checking…',
      '- 👀 [acme/two](https://github.com/acme/two) · with the CDP team: Acme is in LFX Insights, but adding to it needs a quick check by the CDP team',
      '- ✅ [acme/three](https://github.com/acme/three) · added to **Acme**',
      '',
      '_1 added, 1 with the CDP team, 1 checking_',
      '',
      '_Updated automatically as each repository is checked._',
    ])
  })

  it('folds a project group into one heading with its repositories', () => {
    const markdown = buildDiscussionStatusMarkdown([
      row({
        repoUrl: 'https://github.com/apache/hugegraph',
        requestHints: group,
        action: 'review',
        reviewReason: 'needs a look',
      }),
      row({
        repoUrl: 'https://github.com/apache/hugegraph-ai',
        requestHints: group,
        action: 'review',
        reviewReason: 'needs a look',
      }),
    ])

    expect(markdown).toContain(
      '- **Apache HugeGraph** · 2 repositories · 👀 with the CDP team: needs a look',
    )
    expect(markdown).toContain('  - [apache/hugegraph](https://github.com/apache/hugegraph)')
    expect(markdown).toContain('  - [apache/hugegraph-ai](https://github.com/apache/hugegraph-ai)')
  })

  it('explains a rejection and a skip without Slack markup, and drops the closing line once settled', () => {
    const markdown = buildDiscussionStatusMarkdown([
      row({
        repoUrl: 'https://github.com/acme/one',
        action: 'skip',
        skipReason: 'rejected in review by U1',
        reviewedBy: 'U1',
        decisionReason: 'not_lf_project',
      }),
      row({ repoUrl: 'https://github.com/acme/two', action: 'skip', skipReason: 'it is a fork.' }),
    ])

    expect(markdown).toContain('not added: the CDP team reviewed it: ')
    expect(markdown).toContain(
      '- 🚫 [acme/two](https://github.com/acme/two) · not added: it is a fork',
    )
    expect(markdown).not.toContain('<@U1>')
    expect(markdown).not.toContain('Updated automatically')
    expect(buildDiscussionAllDoneMarkdown([row({ repoUrl: 'u', action: 'skip' })])).toBe(
      '✅ All done: 1 not added.',
    )
  })
})

describe('refreshDiscussionStatusComment', () => {
  const qx = {
    tx: (fn: (tx: QueryExecutor) => Promise<unknown>) => fn(qx),
  } as unknown as QueryExecutor
  const client: IDiscussionCommentClient = {
    fetchDiscussionId: vi.fn(async () => 'D_1'),
    addComment: vi.fn(async () => ({ id: 'DC_new', url: 'https://github.com/c/1' })),
    updateComment: vi.fn(async () => ({ id: 'DC_1', url: 'https://github.com/c/1' })),
  }
  const request = { id: 'row-1', repoUrl: 'https://github.com/acme/one', sourceUrl }
  const rows = [
    row({ repoUrl: 'https://github.com/acme/one', action: 'onboarded' }),
    row({ repoUrl: 'https://github.com/acme/two', action: 'review' }),
  ] as IDbProjectCatalog[]

  beforeEach(() => {
    vi.clearAllMocks()
    vi.mocked(findProjectCatalogBySourceUrl).mockResolvedValue(rows)
    vi.mocked(findDiscussionOnboardingRequest).mockResolvedValue(null)
  })

  it('creates the status comment on the first refresh and remembers it', async () => {
    expect(await refreshDiscussionStatusComment(qx, request, client)).toBe(true)

    expect(lockDiscussionOnboardingRequest).toHaveBeenCalledWith(qx, sourceUrl)
    expect(client.fetchDiscussionId).toHaveBeenCalledWith({
      owner: 'linuxfoundation',
      repo: 'insights',
      number: 2496,
    })
    expect(client.addComment).toHaveBeenCalledWith('D_1', expect.stringContaining('acme/one'))
    expect(upsertDiscussionOnboardingRequest).toHaveBeenCalledWith(qx, {
      sourceUrl,
      discussionId: 'D_1',
      commentId: 'DC_new',
    })
  })

  it('edits the existing comment and announces completion once when every row is settled', async () => {
    const status = { sourceUrl, discussionId: 'D_1', commentId: 'DC_1', doneNotifiedAt: null }
    vi.mocked(findDiscussionOnboardingRequest).mockResolvedValue(status as never)
    vi.mocked(findProjectCatalogBySourceUrl).mockResolvedValue([
      { ...rows[0] },
      { ...rows[1], action: 'skip' },
    ] as IDbProjectCatalog[])

    expect(await refreshDiscussionStatusComment(qx, request, client)).toBe(true)

    expect(client.updateComment).toHaveBeenCalledWith('DC_1', expect.stringContaining('acme/two'))
    expect(client.addComment).toHaveBeenCalledWith('D_1', '✅ All done: 1 added, 1 not added.')
    expect(claimDiscussionOnboardingRequestDone).toHaveBeenCalledWith(qx, sourceUrl)

    vi.mocked(findDiscussionOnboardingRequest).mockResolvedValue({
      ...status,
      doneNotifiedAt: '2026-11-10T10:00:00Z',
    } as never)
    vi.mocked(client.addComment).mockClear()
    await refreshDiscussionStatusComment(qx, request, client)
    expect(client.addComment).not.toHaveBeenCalled()
  })

  it('posts a new comment when the stored one was deleted on GitHub', async () => {
    vi.mocked(findDiscussionOnboardingRequest).mockResolvedValue({
      sourceUrl,
      discussionId: 'D_1',
      commentId: 'DC_gone',
      doneNotifiedAt: null,
    } as never)
    vi.mocked(client.updateComment).mockRejectedValueOnce(
      new GithubPublicClientError('NOT_FOUND', 'Could not resolve to a node'),
    )

    expect(await refreshDiscussionStatusComment(qx, request, client)).toBe(true)

    expect(client.fetchDiscussionId).not.toHaveBeenCalled()
    expect(client.addComment).toHaveBeenCalledWith('D_1', expect.stringContaining('acme/one'))
    expect(upsertDiscussionOnboardingRequest).toHaveBeenCalledWith(qx, {
      sourceUrl,
      discussionId: 'D_1',
      commentId: 'DC_new',
    })
  })

  it('ignores rows without a discussion origin and reports a failed GitHub call', async () => {
    expect(
      await refreshDiscussionStatusComment(
        qx,
        { ...request, sourceUrl: 'https://acme.slack.com/archives/C1/p1' },
        client,
      ),
    ).toBe(false)
    expect(await refreshDiscussionStatusComment(qx, { ...request, sourceUrl: null }, client)).toBe(
      false,
    )

    vi.mocked(client.addComment).mockRejectedValueOnce(new Error('boom'))
    expect(await refreshDiscussionStatusComment(qx, request, client)).toBe(false)
    expect(upsertDiscussionOnboardingRequest).not.toHaveBeenCalled()
  })
})
