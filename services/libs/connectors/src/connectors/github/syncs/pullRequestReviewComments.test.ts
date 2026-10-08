import { describe, expect, it, vi } from 'vitest'

import type { Logger } from '@crowd/logging'

import type { ConnectorHttp } from '../../../http/client'
import type { SyncContext } from '../../../types'
import {
  COMMENTS_FOR_THREADS_QUERY,
  REVIEW_THREADS_FOR_PRS_QUERY,
} from '../graphql/pullRequestChildren'
import type { GithubActivity } from '../schemas'
import { pullRequestReviewCommentsSync } from './pullRequestReviewComments'

vi.mock('@crowd/integrations', () => ({
  GithubActivityType: {
    PULL_REQUEST_REVIEW_THREAD_COMMENT: 'pull_request-review-thread-comment',
  },
  GITHUB_GRID: { 'pull_request-review-thread-comment': { score: 1 } },
}))

const CHANNEL = { channelId: 'c1', channelName: 'https://github.com/openclaw/openclaw' }
const PR_NUMBER = 1
const PR_ID = 'pr-1'
const THREAD_ID = 'thread-1'

interface RequestLog {
  kind: 'threads' | 'comments'
  id: string
  after: string | null
}

interface HarnessOptions {
  missingPrNode?: boolean
  nullThreadsConnectionCalls?: number
  nullCommentsConnectionCalls?: number
  commentPages?: number
  commentsPerPage?: number
  commentsTotalCount?: number
}

function commentNode(id: string) {
  return {
    id,
    body: 'looks good',
    createdAt: '2026-09-20T00:00:00Z',
    url: `https://github.com/openclaw/openclaw/pull/1#discussion_${id}`,
    author: { __typename: 'User', login: 'someone' },
  }
}

function makeHarness(opts: HarnessOptions = {}) {
  const requests: RequestLog[] = []
  const emitted: GithubActivity[] = []
  const warn = vi.fn()
  let prListServed = false
  let threadsCalls = 0
  let commentsCalls = 0

  const http = {
    request: async (config: { data: { query: string; variables: Record<string, unknown> } }) => {
      const { query, variables } = config.data

      if (query.includes('pullRequests(')) {
        const nodes = prListServed
          ? []
          : [
              {
                id: PR_ID,
                number: PR_NUMBER,
                updatedAt: '2026-09-20T00:00:00Z',
                title: 'Add feature',
                url: 'https://github.com/openclaw/openclaw/pull/1',
                state: 'OPEN',
              },
            ]
        prListServed = true
        return {
          data: {
            repository: {
              pullRequests: { pageInfo: { hasNextPage: false, endCursor: null }, nodes },
            },
          },
        }
      }

      const { ids, after } = variables as { ids: string[]; after: string | null }

      if (query === REVIEW_THREADS_FOR_PRS_QUERY) {
        threadsCalls += 1
        requests.push({ kind: 'threads', id: ids[0], after })
        if (opts.missingPrNode) {
          return { data: { nodes: [null] } }
        }
        const reviewThreads =
          threadsCalls <= (opts.nullThreadsConnectionCalls ?? 0)
            ? null
            : {
                totalCount: 1,
                pageInfo: { endCursor: null, hasNextPage: false },
                edges: [{ node: { id: THREAD_ID, isResolved: false } }],
              }
        return { data: { nodes: [{ id: PR_ID, number: PR_NUMBER, reviewThreads }] } }
      }

      if (query === COMMENTS_FOR_THREADS_QUERY) {
        commentsCalls += 1
        requests.push({ kind: 'comments', id: ids[0], after })
        if (commentsCalls <= (opts.nullCommentsConnectionCalls ?? 0)) {
          return { data: { nodes: [{ id: THREAD_ID, isResolved: false, comments: null }] } }
        }
        const pageIndex = after ? Number(after.slice(1)) : 1
        const totalPages = opts.commentPages ?? 1
        const perPage = opts.commentsPerPage ?? 2
        const hasNextPage = pageIndex < totalPages
        return {
          data: {
            nodes: [
              {
                id: THREAD_ID,
                isResolved: false,
                comments: {
                  totalCount: opts.commentsTotalCount ?? totalPages * perPage,
                  pageInfo: {
                    endCursor: hasNextPage ? `p${pageIndex + 1}` : null,
                    hasNextPage,
                  },
                  edges: Array.from({ length: perPage }, (_, index) => ({
                    node: commentNode(`c-${pageIndex}-${index + 1}`),
                  })),
                },
              },
            ],
          },
        }
      }

      throw new Error(`unexpected query: ${query}`)
    },
  } as unknown as ConnectorHttp

  const ctx: SyncContext = {
    channel: CHANNEL,
    watermark: { phase: 'backfill', since: null, cursor: null },
    emit: async (records) => {
      emitted.push(...(records as GithubActivity[]))
    },
    commitWatermark: async () => {},
    hasRunBudget: () => true,
    http,
    log: {
      child: () => ctx.log,
      info: () => {},
      warn,
      error: () => {},
    } as unknown as Logger,
  }

  return { ctx, requests, emitted, warn }
}

describe('pullRequestReviewCommentsSync', () => {
  it('emits every comment of every review thread', async () => {
    const { ctx, requests, emitted, warn } = makeHarness()

    await pullRequestReviewCommentsSync.run(ctx)

    expect(requests).toEqual([
      { kind: 'threads', id: PR_ID, after: null },
      { kind: 'comments', id: THREAD_ID, after: null },
    ])
    expect(emitted.map((record) => record.sourceId)).toEqual(['c-1-1', 'c-1-2'])
    expect(emitted[0].sourceParentId).toBe(PR_ID)
    expect(warn).not.toHaveBeenCalled()
  })

  it('gives up on a pull request whose node github cannot resolve, without retrying', async () => {
    const { ctx, requests, emitted, warn } = makeHarness({ missingPrNode: true })

    await pullRequestReviewCommentsSync.run(ctx)

    expect(requests).toEqual([{ kind: 'threads', id: PR_ID, after: null }])
    expect(emitted).toHaveLength(0)
    expect(warn).toHaveBeenCalledTimes(1)
    expect(warn).toHaveBeenCalledWith('github returned no pull request node for review threads')
  })

  it('retries once when the review threads connection is null and recovers', async () => {
    const { ctx, requests, emitted, warn } = makeHarness({ nullThreadsConnectionCalls: 1 })

    await pullRequestReviewCommentsSync.run(ctx)

    expect(requests.filter((request) => request.kind === 'threads')).toEqual([
      { kind: 'threads', id: PR_ID, after: null },
      { kind: 'threads', id: PR_ID, after: null },
    ])
    expect(emitted).toHaveLength(2)
    expect(warn).toHaveBeenCalledTimes(1)
    expect(warn).toHaveBeenCalledWith(
      'github returned no review threads connection for pull request, retrying',
    )
  })

  it('gives up on a pull request when the review threads connection stays null after retry', async () => {
    const { ctx, requests, emitted, warn } = makeHarness({ nullThreadsConnectionCalls: 2 })

    await pullRequestReviewCommentsSync.run(ctx)

    expect(requests).toEqual([
      { kind: 'threads', id: PR_ID, after: null },
      { kind: 'threads', id: PR_ID, after: null },
    ])
    expect(emitted).toHaveLength(0)
    expect(warn.mock.calls).toEqual([
      ['github returned no review threads connection for pull request, retrying'],
      ['github still returned no review threads connection for pull request after retry'],
    ])
  })

  it('retries once when a thread comments connection is null and recovers', async () => {
    const { ctx, requests, emitted, warn } = makeHarness({ nullCommentsConnectionCalls: 1 })

    await pullRequestReviewCommentsSync.run(ctx)

    expect(requests.filter((request) => request.kind === 'comments')).toEqual([
      { kind: 'comments', id: THREAD_ID, after: null },
      { kind: 'comments', id: THREAD_ID, after: null },
    ])
    expect(emitted.map((record) => record.sourceId)).toEqual(['c-1-1', 'c-1-2'])
    expect(warn).toHaveBeenCalledTimes(1)
    expect(warn).toHaveBeenCalledWith(
      'github returned no comments connection for review thread, retrying',
    )
  })

  it('warns when github reports more thread comments than were fetched', async () => {
    const { ctx, emitted, warn } = makeHarness({ commentsPerPage: 1, commentsTotalCount: 3 })

    await pullRequestReviewCommentsSync.run(ctx)

    expect(emitted).toHaveLength(1)
    expect(warn).toHaveBeenCalledWith(
      { prNumber: PR_NUMBER, threadId: THREAD_ID, expectedCount: 3, fetchedCount: 1 },
      'fetched fewer review thread comments than github reports for review thread',
    )
  })

  it('does not warn when every reported comment was fetched across pages', async () => {
    const { ctx, requests, emitted, warn } = makeHarness({ commentPages: 2, commentsPerPage: 1 })

    await pullRequestReviewCommentsSync.run(ctx)

    expect(requests.filter((request) => request.kind === 'comments')).toEqual([
      { kind: 'comments', id: THREAD_ID, after: null },
      { kind: 'comments', id: THREAD_ID, after: 'p2' },
    ])
    expect(emitted.map((record) => record.sourceId)).toEqual(['c-1-1', 'c-2-1'])
    expect(warn).not.toHaveBeenCalled()
  })
})
