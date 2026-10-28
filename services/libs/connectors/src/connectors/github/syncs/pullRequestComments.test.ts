import { describe, expect, it, vi } from 'vitest'

import type { Logger } from '@crowd/logging'

import type { ConnectorHttp } from '../../../http/client'
import type { SyncContext } from '../../../types'
import { COMMENTS_FOR_PRS_QUERY } from '../graphql/pullRequestChildren'
import type { GithubActivity } from '../schemas'
import { pullRequestCommentsSync } from './pullRequestComments'

vi.mock('@crowd/integrations', () => ({
  GithubActivityType: {
    PULL_REQUEST_COMMENT: 'pull_request-comment',
  },
  GITHUB_GRID: { 'pull_request-comment': { score: 1 } },
}))

const CHANNEL = { channelId: 'c1', channelName: 'https://github.com/openclaw/openclaw' }
const PR_NUMBER = 176438
const PR_ID = 'pr-176438'
const PR_UPDATED_AT = '2026-10-18T20:44:00Z'
const SINCE = '2026-10-18T12:00:00Z'
const OLD_COMMENT_CREATED_AT = '2026-10-18T09:00:00Z'

interface RequestLog {
  id: string
  after: string | null
}

interface HarnessOptions {
  incremental?: boolean
  missingPrNode?: boolean
  nullConnectionCalls?: number
  nullConnectionOnPage?: number
  commentPages?: number
  commentsPerPage?: number
  commentsTotalCount?: number
  nullNodeEdgeOnPage?: number
}

function commentNode(id: string) {
  return {
    id,
    body: 'thanks for the review',
    createdAt: OLD_COMMENT_CREATED_AT,
    url: `https://github.com/openclaw/openclaw/pull/${PR_NUMBER}#issuecomment-${id}`,
    author: { __typename: 'User', login: 'someone' },
  }
}

function makeHarness(opts: HarnessOptions = {}) {
  const requests: RequestLog[] = []
  const emitted: GithubActivity[] = []
  const committed: Record<string, unknown>[] = []
  const warn = vi.fn()
  let prListServed = false
  let commentsCalls = 0
  let nullPageServed = false

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
                updatedAt: PR_UPDATED_AT,
                title: 'Add feature',
                url: `https://github.com/openclaw/openclaw/pull/${PR_NUMBER}`,
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

      if (query !== COMMENTS_FOR_PRS_QUERY) {
        throw new Error(`unexpected query: ${query}`)
      }

      const { ids, after } = variables as { ids: string[]; after: string | null }
      commentsCalls += 1
      requests.push({ id: ids[0], after })

      if (opts.missingPrNode) {
        return { data: { nodes: [null] } }
      }
      if (commentsCalls <= (opts.nullConnectionCalls ?? 0)) {
        return { data: { nodes: [{ id: PR_ID, number: PR_NUMBER, comments: null }] } }
      }

      const pageIndex = after ? Number(after.slice(1)) : 1
      if (opts.nullConnectionOnPage === pageIndex && !nullPageServed) {
        nullPageServed = true
        return { data: { nodes: [{ id: PR_ID, number: PR_NUMBER, comments: null }] } }
      }

      const totalPages = opts.commentPages ?? 1
      const perPage = opts.commentsPerPage ?? 2
      const hasNextPage = pageIndex < totalPages
      const edges: { node: ReturnType<typeof commentNode> | null }[] = Array.from(
        { length: perPage },
        (_, index) => ({ node: commentNode(`c-${pageIndex}-${index + 1}`) }),
      )
      if (opts.nullNodeEdgeOnPage === pageIndex) {
        edges.push({ node: null })
      }
      return {
        data: {
          nodes: [
            {
              id: PR_ID,
              number: PR_NUMBER,
              comments: {
                totalCount: opts.commentsTotalCount ?? totalPages * perPage,
                pageInfo: {
                  endCursor: hasNextPage ? `p${pageIndex + 1}` : null,
                  hasNextPage,
                },
                edges,
              },
            },
          ],
        },
      }
    },
  } as unknown as ConnectorHttp

  const ctx: SyncContext = {
    channel: CHANNEL,
    integrationId: 'integration-1',
    watermark: opts.incremental
      ? { phase: 'incremental', since: SINCE, cursor: null }
      : { phase: 'backfill', since: null, cursor: null },
    emit: async (records) => {
      emitted.push(...(records as GithubActivity[]))
    },
    commitWatermark: async (watermark) => {
      committed.push(watermark)
    },
    hasRunBudget: () => true,
    http,
    log: {
      child: () => ctx.log,
      info: () => {},
      warn,
      error: () => {},
    } as unknown as Logger,
    getCredential: () => {
      throw new Error('not implemented in test')
    },
    deps: undefined,
  }

  return { ctx, requests, emitted, committed, warn }
}

describe('pullRequestCommentsSync', () => {
  it('emits comments older than since on a walked pull request', async () => {
    const { ctx, requests, emitted, committed, warn } = makeHarness({ incremental: true })

    const outcome = await pullRequestCommentsSync.run(ctx)

    expect(outcome).toEqual({ complete: true })
    expect(requests).toEqual([{ id: PR_ID, after: null }])
    expect(emitted.map((record) => record.sourceId)).toEqual(['c-1-1', 'c-1-2'])
    expect(emitted.every((record) => record.timestamp === OLD_COMMENT_CREATED_AT)).toBe(true)
    expect(emitted[0].sourceParentId).toBe(PR_ID)
    expect(emitted[0].type).toBe('pull_request-comment')
    expect(committed[committed.length - 1]).toMatchObject({ phase: 'incremental', cursor: null })
    expect(warn).not.toHaveBeenCalled()
  })

  it('gives up on a pull request whose node github cannot resolve, without retrying', async () => {
    const { ctx, requests, emitted, warn } = makeHarness({ missingPrNode: true })

    const outcome = await pullRequestCommentsSync.run(ctx)

    expect(outcome).toEqual({ complete: true })
    expect(requests).toEqual([{ id: PR_ID, after: null }])
    expect(emitted).toHaveLength(0)
    expect(warn).toHaveBeenCalledTimes(1)
    expect(warn).toHaveBeenCalledWith('github returned no pull request node for comments')
  })

  it('retries once when the comments connection is null and recovers', async () => {
    const { ctx, requests, emitted, warn } = makeHarness({ nullConnectionCalls: 1 })

    await pullRequestCommentsSync.run(ctx)

    expect(requests).toEqual([
      { id: PR_ID, after: null },
      { id: PR_ID, after: null },
    ])
    expect(emitted.map((record) => record.sourceId)).toEqual(['c-1-1', 'c-1-2'])
    expect(warn).toHaveBeenCalledTimes(1)
    expect(warn).toHaveBeenCalledWith(
      'github returned no comments connection for pull request, retrying',
    )
  })

  it('emits nothing for a pull request whose comments connection stays null after retry', async () => {
    const { ctx, requests, emitted, warn } = makeHarness({ nullConnectionCalls: 2 })

    const outcome = await pullRequestCommentsSync.run(ctx)

    expect(outcome).toEqual({ complete: true })
    expect(requests).toEqual([
      { id: PR_ID, after: null },
      { id: PR_ID, after: null },
    ])
    expect(emitted).toHaveLength(0)
    expect(warn.mock.calls).toEqual([
      ['github returned no comments connection for pull request, retrying'],
      ['github still returned no comments connection for pull request after retry'],
    ])
  })

  it('retries once when a later comments page is null and recovers', async () => {
    const { ctx, requests, emitted, warn } = makeHarness({
      commentPages: 2,
      commentsPerPage: 1,
      nullConnectionOnPage: 2,
    })

    await pullRequestCommentsSync.run(ctx)

    expect(requests).toEqual([
      { id: PR_ID, after: null },
      { id: PR_ID, after: 'p2' },
      { id: PR_ID, after: 'p2' },
    ])
    expect(emitted.map((record) => record.sourceId)).toEqual(['c-1-1', 'c-2-1'])
    expect(warn).toHaveBeenCalledTimes(1)
    expect(warn).toHaveBeenCalledWith(
      'github returned no comments connection for pull request, retrying',
    )
  })

  it('warns when github reports more comments than were fetched', async () => {
    const { ctx, emitted, warn } = makeHarness({
      commentsPerPage: 1,
      commentsTotalCount: 3,
      nullNodeEdgeOnPage: 1,
    })

    await pullRequestCommentsSync.run(ctx)

    expect(emitted.map((record) => record.sourceId)).toEqual(['c-1-1'])
    expect(warn).toHaveBeenCalledTimes(1)
    expect(warn).toHaveBeenCalledWith(
      { prNumber: PR_NUMBER, prId: PR_ID, expectedCount: 3, fetchedCount: 1 },
      'fetched fewer comments than github reports for pull request',
    )
  })

  it('does not warn when every reported comment was fetched across pages', async () => {
    const { ctx, requests, emitted, warn } = makeHarness({ commentPages: 2, commentsPerPage: 1 })

    await pullRequestCommentsSync.run(ctx)

    expect(requests).toEqual([
      { id: PR_ID, after: null },
      { id: PR_ID, after: 'p2' },
    ])
    expect(emitted.map((record) => record.sourceId)).toEqual(['c-1-1', 'c-2-1'])
    expect(warn).not.toHaveBeenCalled()
  })
})
