import { describe, expect, it, vi } from 'vitest'

import type { ConnectorHttp } from '@crowd/connectors'
import type { Logger } from '@crowd/logging'

import { dropConfirmedDeletedRecords, hasDeletedRecordCandidates } from './deletedRecords'
import { IShadowDiffMismatch } from './shadowDiff'

const log = { info: vi.fn(), warn: vi.fn(), error: vi.fn() } as unknown as Logger

function missingMismatch(sourceId: string, type = 'issue-comment'): IShadowDiffMismatch {
  return { sourceId, type, kind: 'missing_in_shadow', severity: 'high' }
}

function httpWithHandler(handler: (ids: string[]) => Promise<unknown>): ConnectorHttp {
  return {
    request: async (config: { data: { variables: { ids: string[] } } }) =>
      handler(config.data.variables.ids),
    requestCount: () => 0,
  } as unknown as ConnectorHttp
}

describe('hasDeletedRecordCandidates', () => {
  it('is false for the pull-request-commits sync regardless of candidates', () => {
    expect(hasDeletedRecordCandidates('pull-request-commits', [missingMismatch('IC_abc')])).toBe(
      false,
    )
  })

  it('is false when the only missing_in_shadow records have synthetic sourceIds', () => {
    expect(
      hasDeletedRecordCandidates('pull-requests', [missingMismatch('gen-CE_PR_1_bob_2026')]),
    ).toBe(false)
  })

  it('is true when a real node-id missing_in_shadow record exists', () => {
    expect(hasDeletedRecordCandidates('issue-comments', [missingMismatch('IC_abc')])).toBe(true)
  })
})

describe('dropConfirmedDeletedRecords', () => {
  it('drops records confirmed deleted (null node with NOT_FOUND) and keeps records still alive', async () => {
    const mismatches = [missingMismatch('IC_deleted'), missingMismatch('IC_alive')]
    const http = httpWithHandler(async (ids) => ({
      data: { nodes: ids.map((id) => (id === 'IC_alive' ? { id } : null)) },
      errors: ids.flatMap((id, i) =>
        id === 'IC_alive' ? [] : [{ type: 'NOT_FOUND', path: ['nodes', i] }],
      ),
    }))

    const result = await dropConfirmedDeletedRecords('issue-comments', mismatches, http, log)

    expect(result.mismatches).toEqual([missingMismatch('IC_alive')])
    expect(result.confirmedDeletedCount).toBe(1)
    expect(result.keptUnconfirmedCount).toBe(0)
  })

  it('keeps records when the graphql request errors', async () => {
    const mismatches = [missingMismatch('IC_unknown')]
    const http = httpWithHandler(async () => {
      throw new Error('network exploded')
    })

    const result = await dropConfirmedDeletedRecords('issue-comments', mismatches, http, log)

    expect(result.mismatches).toEqual(mismatches)
    expect(result.confirmedDeletedCount).toBe(0)
    expect(result.keptUnconfirmedCount).toBe(1)
  })

  it('keeps records when the response has no top-level data', async () => {
    const mismatches = [missingMismatch('IC_unknown')]
    const http = httpWithHandler(async () => ({ errors: [{ type: 'FORBIDDEN' }] }))

    const result = await dropConfirmedDeletedRecords('issue-comments', mismatches, http, log)

    expect(result.mismatches).toEqual(mismatches)
    expect(result.confirmedDeletedCount).toBe(0)
    expect(result.keptUnconfirmedCount).toBe(1)
  })

  it('treats NOT_FOUND per-id errors alongside data as a valid confirmation', async () => {
    const mismatches = [missingMismatch('IC_deleted')]
    const http = httpWithHandler(async (ids) => ({
      data: { nodes: ids.map(() => null) },
      errors: [{ type: 'NOT_FOUND', message: 'Could not resolve to a node', path: ['nodes', 0] }],
    }))

    const result = await dropConfirmedDeletedRecords('issue-comments', mismatches, http, log)

    expect(result.mismatches).toEqual([])
    expect(result.confirmedDeletedCount).toBe(1)
  })

  it('keeps null nodes whose error is not NOT_FOUND (e.g. FORBIDDEN)', async () => {
    const mismatches = [missingMismatch('IC_forbidden'), missingMismatch('IC_deleted')]
    const http = httpWithHandler(async (ids) => ({
      data: { nodes: ids.map(() => null) },
      errors: [
        { type: 'FORBIDDEN', path: ['nodes', 0] },
        { type: 'NOT_FOUND', path: ['nodes', 1] },
      ],
    }))

    const result = await dropConfirmedDeletedRecords('issue-comments', mismatches, http, log)

    expect(result.mismatches).toEqual([missingMismatch('IC_forbidden')])
    expect(result.confirmedDeletedCount).toBe(1)
    expect(result.keptUnconfirmedCount).toBe(1)
  })

  it('keeps null nodes that have no matching error entry at all', async () => {
    const mismatches = [missingMismatch('IC_unresolved')]
    const http = httpWithHandler(async (ids) => ({
      data: { nodes: ids.map(() => null) },
    }))

    const result = await dropConfirmedDeletedRecords('issue-comments', mismatches, http, log)

    expect(result.mismatches).toEqual(mismatches)
    expect(result.confirmedDeletedCount).toBe(0)
    expect(result.keptUnconfirmedCount).toBe(1)
  })

  it('never checks pull-request-commits candidates and leaves them untouched', async () => {
    const mismatches = [missingMismatch('deadbeef', 'authored-commit')]
    const requestSpy = vi.fn()
    const http = { request: requestSpy, requestCount: () => 0 } as unknown as ConnectorHttp

    const result = await dropConfirmedDeletedRecords('pull-request-commits', mismatches, http, log)

    expect(result.mismatches).toEqual(mismatches)
    expect(requestSpy).not.toHaveBeenCalled()
  })

  it('ignores synthetic gen- sourceIds since they are not graphql node ids', async () => {
    const genMismatch = missingMismatch('gen-CE_PR_1_bob_2026', 'pull_request-closed')
    const nodeMismatch = missingMismatch('PR_1', 'pull_request-opened')
    const requestSpy = vi.fn(async (config: { data: { variables: { ids: string[] } } }) => ({
      data: { nodes: config.data.variables.ids.map(() => null) },
      errors: config.data.variables.ids.map((_, i) => ({
        type: 'NOT_FOUND',
        path: ['nodes', i],
      })),
    }))
    const http = { request: requestSpy, requestCount: () => 0 } as unknown as ConnectorHttp

    const result = await dropConfirmedDeletedRecords(
      'pull-requests',
      [genMismatch, nodeMismatch],
      http,
      log,
    )

    expect(result.mismatches).toEqual([genMismatch])
    expect(requestSpy).toHaveBeenCalledTimes(1)
    expect(requestSpy.mock.calls[0][0].data.variables.ids).toEqual(['PR_1'])
  })

  it('keeps every candidate unconfirmed once the time budget is already exhausted', async () => {
    const manyIds = Array.from({ length: 150 }, (_, i) => `IC_${i}`)
    const mismatches = manyIds.map((id) => missingMismatch(id))
    const requestSpy = vi.fn()
    const http = { request: requestSpy, requestCount: () => 0 } as unknown as ConnectorHttp

    vi.spyOn(Date, 'now').mockReturnValueOnce(0).mockReturnValue(120_000)
    const result = await dropConfirmedDeletedRecords('issue-comments', mismatches, http, log)
    vi.restoreAllMocks()

    expect(requestSpy).not.toHaveBeenCalled()
    expect(result.confirmedDeletedCount).toBe(0)
    expect(result.keptUnconfirmedCount).toBe(manyIds.length)
    expect(result.mismatches).toEqual(mismatches)
  })
})
