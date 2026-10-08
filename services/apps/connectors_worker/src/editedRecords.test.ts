import { describe, expect, it, vi } from 'vitest'

import type { ConnectorHttp } from '@crowd/connectors'
import type { Logger } from '@crowd/logging'

import { dropConfirmedEditedRecords, hasEditedRecordCandidates } from './editedRecords'
import { IShadowDiffMismatch } from './shadowDiff'

const log = { info: vi.fn(), warn: vi.fn(), error: vi.fn() } as unknown as Logger

function bodyMismatch(sourceId: string, type = 'issue-comment'): IShadowDiffMismatch {
  return {
    sourceId,
    type,
    kind: 'field_mismatch',
    severity: 'high',
    fields: [{ field: 'body', shadowValue: 'old', nangoValue: 'new' }],
  }
}

function httpWithHandler(handler: (ids: string[]) => Promise<unknown>): ConnectorHttp {
  return {
    request: async (config: { data: { variables: { ids: string[] } } }) =>
      handler(config.data.variables.ids),
    requestCount: () => 0,
  } as unknown as ConnectorHttp
}

function editedResponse(editedIds: string[]) {
  return async (ids: string[]) => ({
    data: {
      nodes: ids.map((id) => ({
        id,
        lastEditedAt: editedIds.includes(id) ? '2026-10-07T00:00:00Z' : null,
      })),
    },
  })
}

describe('hasEditedRecordCandidates', () => {
  it('is false for the pull-request-commits sync', () => {
    expect(
      hasEditedRecordCandidates('pull-request-commits', [
        bodyMismatch('abc123', 'authored-commit'),
      ]),
    ).toBe(false)
  })

  it('is false when the mismatch involves fields other than body', () => {
    const mismatch: IShadowDiffMismatch = {
      ...bodyMismatch('IC_abc'),
      fields: [
        { field: 'body', shadowValue: 'old', nangoValue: 'new' },
        { field: 'url', shadowValue: 'a', nangoValue: 'b' },
      ],
    }
    expect(hasEditedRecordCandidates('issue-comments', [mismatch])).toBe(false)
  })

  it('is false for missing_in_shadow mismatches', () => {
    expect(
      hasEditedRecordCandidates('issue-comments', [
        { sourceId: 'IC_abc', type: 'issue-comment', kind: 'missing_in_shadow', severity: 'high' },
      ]),
    ).toBe(false)
  })

  it('is true for a body-only mismatch on a real node id', () => {
    expect(hasEditedRecordCandidates('issue-comments', [bodyMismatch('IC_abc')])).toBe(true)
  })

  it('is true for a body-only mismatch on a synthetic id carrying a parent node', () => {
    expect(
      hasEditedRecordCandidates('pull-requests', [
        bodyMismatch(
          'gen-RRE_PR_kwDOI7xefs8AAAABGaViEg_alice_bob_2026-10-06T15:07:23.000Z',
          'pull_request-review-requested',
        ),
      ]),
    ).toBe(true)
  })

  it('is false for a reviewed event whose body belongs to the review, not the parent PR', () => {
    expect(
      hasEditedRecordCandidates('pull-requests', [
        bodyMismatch(
          'gen-PRR_PR_kwDOI7xefs8AAAABGaViEg_alice_2026-10-06T15:07:23.000Z',
          'pull_request-reviewed',
        ),
      ]),
    ).toBe(false)
  })
})

describe('dropConfirmedEditedRecords', () => {
  it('drops body mismatches whose node was edited and keeps the never-edited ones', async () => {
    const mismatches = [bodyMismatch('IC_edited'), bodyMismatch('IC_pristine')]
    const http = httpWithHandler(editedResponse(['IC_edited']))

    const result = await dropConfirmedEditedRecords('issue-comments', mismatches, http, log)

    expect(result.mismatches).toEqual([bodyMismatch('IC_pristine')])
    expect(result.confirmedEditedCount).toBe(1)
    expect(result.unconfirmedCount).toBe(0)
  })

  it('resolves synthetic timeline ids to their parent node and queries each parent once', async () => {
    const prId = 'PR_kwDOI7xefs8AAAABGaViEg'
    const mismatches = [
      bodyMismatch(
        `gen-RRE_${prId}_alice_bob_2026-10-06T15:07:23.000Z`,
        'pull_request-review-requested',
      ),
      bodyMismatch(
        `gen-RRE_${prId}_alice_carol_2026-10-06T15:07:23.000Z`,
        'pull_request-review-requested',
      ),
      bodyMismatch(prId, 'pull_request-opened'),
    ]
    const seen: string[][] = []
    const http = httpWithHandler(async (ids) => {
      seen.push(ids)
      return editedResponse([prId])(ids)
    })

    const result = await dropConfirmedEditedRecords('pull-requests', mismatches, http, log)

    expect(seen).toEqual([[prId]])
    expect(result.mismatches).toEqual([])
    expect(result.confirmedEditedCount).toBe(3)
  })

  it('keeps mismatches and counts them unconfirmed when the request fails', async () => {
    const mismatches = [bodyMismatch('IC_unknown')]
    const http = httpWithHandler(async () => {
      throw new Error('network exploded')
    })

    const result = await dropConfirmedEditedRecords('issue-comments', mismatches, http, log)

    expect(result.mismatches).toEqual(mismatches)
    expect(result.confirmedEditedCount).toBe(0)
    expect(result.unconfirmedCount).toBe(1)
  })

  it('keeps mismatches whose node resolved to null', async () => {
    const mismatches = [bodyMismatch('IC_gone')]
    const http = httpWithHandler(async (ids) => ({ data: { nodes: ids.map(() => null) } }))

    const result = await dropConfirmedEditedRecords('issue-comments', mismatches, http, log)

    expect(result.mismatches).toEqual(mismatches)
    expect(result.confirmedEditedCount).toBe(0)
    expect(result.unconfirmedCount).toBe(0)
  })

  it('leaves non-candidate mismatches untouched', async () => {
    const other: IShadowDiffMismatch = {
      sourceId: 'IC_other',
      type: 'issue-comment',
      kind: 'missing_in_nango',
      severity: 'high',
    }
    const http = httpWithHandler(editedResponse(['IC_edited']))

    const result = await dropConfirmedEditedRecords(
      'issue-comments',
      [other, bodyMismatch('IC_edited')],
      http,
      log,
    )

    expect(result.mismatches).toEqual([other])
  })
})
