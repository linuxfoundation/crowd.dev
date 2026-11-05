import { describe, expect, it, vi } from 'vitest'

import type { ConnectorHttp } from '@crowd/connectors'
import type { Logger } from '@crowd/logging'

import { dropConfirmedForcePushedCommits, hasForcePushCandidates } from './forcePushedCommits'
import { IShadowDiffMismatch } from './shadowDiff'

const log = { info: vi.fn(), warn: vi.fn(), error: vi.fn() } as unknown as Logger

function missingMismatch(sourceId: string, sourceParentId?: string): IShadowDiffMismatch {
  const mismatch: IShadowDiffMismatch = {
    sourceId,
    type: 'commit',
    kind: 'missing_in_shadow',
    severity: 'high',
  }
  return sourceParentId === undefined ? mismatch : { ...mismatch, sourceParentId }
}

function shasOf(variables: Record<string, string>): string[] {
  return Object.keys(variables)
    .filter((key) => key.startsWith('oid'))
    .sort((a, b) => Number(a.slice(3)) - Number(b.slice(3)))
    .map((key) => variables[key])
}

function httpWithHandler(
  handler: (shas: string[], query: string) => Promise<unknown>,
  onRequest?: () => void,
): ConnectorHttp {
  return {
    request: async (config: { data: { query: string; variables: Record<string, string> } }) => {
      onRequest?.()
      return handler(shasOf(config.data.variables), config.data.query)
    },
    requestCount: () => 0,
  } as unknown as ConnectorHttp
}

type Association = number | null | undefined | { totalCount: number; ids: (string | null)[] }

function toAssociationNode(association: Association) {
  if (association === null) {
    return null
  }
  if (association === undefined) {
    return {}
  }
  if (typeof association === 'number') {
    return { associatedPullRequests: { totalCount: association, nodes: [] } }
  }
  return {
    associatedPullRequests: {
      totalCount: association.totalCount,
      nodes: association.ids.map((id) => (id === null ? null : { id })),
    },
  }
}

function repositoryOf(associations: Association[]) {
  return {
    data: {
      repository: Object.fromEntries(
        associations.map((association, i) => [`c${i}`, toAssociationNode(association)]),
      ),
    },
  }
}

describe('hasForcePushCandidates', () => {
  it('is false for syncs other than pull-request-commits', () => {
    expect(hasForcePushCandidates('issues', [missingMismatch('abc')])).toBe(false)
  })

  it('is true when pull-request-commits has a missing_in_shadow record', () => {
    expect(hasForcePushCandidates('pull-request-commits', [missingMismatch('abc')])).toBe(true)
  })
})

describe('dropConfirmedForcePushedCommits', () => {
  it('drops commits with no associated pull request and keeps commits still on their pull request', async () => {
    const mismatches = [missingMismatch('orphan', 'PR_gone'), missingMismatch('live', 'PR_live')]
    const queries: string[] = []
    const http = httpWithHandler(async (shas, query) => {
      queries.push(query)
      return repositoryOf(
        shas.map((sha) => (sha === 'orphan' ? 0 : { totalCount: 1, ids: ['PR_live'] })),
      )
    })

    const result = await dropConfirmedForcePushedCommits(
      'pull-request-commits',
      mismatches,
      'o',
      'r',
      http,
      log,
    )

    expect(result.mismatches).toEqual([missingMismatch('live', 'PR_live')])
    expect(result.skippedCount).toBe(1)
    expect(result.staleParentCount).toBe(0)
    expect(result.failedCount).toBe(0)
    expect(queries[0]).toContain('associatedPullRequests(first: 5) { totalCount nodes { id } }')
  })

  it('drops commits whose pull request no longer lists them and keeps commits without a known parent', async () => {
    const mismatches = [
      missingMismatch('rebased', 'PR_old'),
      missingMismatch('live', 'PR_live'),
      missingMismatch('unknown'),
    ]
    const http = httpWithHandler(async () =>
      repositoryOf([
        { totalCount: 1, ids: ['PR_other'] },
        { totalCount: 2, ids: ['PR_other', 'PR_live'] },
        { totalCount: 1, ids: ['PR_other'] },
      ]),
    )

    const result = await dropConfirmedForcePushedCommits(
      'pull-request-commits',
      mismatches,
      'o',
      'r',
      http,
      log,
    )

    expect(result.mismatches).toEqual([
      missingMismatch('live', 'PR_live'),
      missingMismatch('unknown'),
    ])
    expect(result.skippedCount).toBe(0)
    expect(result.staleParentCount).toBe(1)
    expect(result.failedCount).toBe(0)
  })

  it('leaves commits unconfirmed when not every association could be read', async () => {
    const mismatches = [missingMismatch('busy', 'PR_old'), missingMismatch('hidden', 'PR_old')]
    const http = httpWithHandler(async () =>
      repositoryOf([
        { totalCount: 6, ids: ['PR_1', 'PR_2', 'PR_3', 'PR_4', 'PR_5'] },
        { totalCount: 2, ids: ['PR_other', null] },
      ]),
    )

    const result = await dropConfirmedForcePushedCommits(
      'pull-request-commits',
      mismatches,
      'o',
      'r',
      http,
      log,
    )

    expect(result.mismatches).toEqual([])
    expect(result.skippedCount).toBe(0)
    expect(result.staleParentCount).toBe(0)
    expect(result.failedCount).toBe(2)
  })

  it('excludes commits GitHub cannot resolve and counts them as unconfirmed', async () => {
    const mismatches = [missingMismatch('gone'), missingMismatch('odd'), missingMismatch('live')]
    const http = httpWithHandler(async () => repositoryOf([null, undefined, 1]))

    const result = await dropConfirmedForcePushedCommits(
      'pull-request-commits',
      mismatches,
      'o',
      'r',
      http,
      log,
    )

    expect(result.mismatches).toEqual([missingMismatch('live')])
    expect(result.skippedCount).toBe(0)
    expect(result.staleParentCount).toBe(0)
    expect(result.failedCount).toBe(2)
  })

  it('excludes the whole batch and counts it as unconfirmed when the request errors', async () => {
    const mismatches = [missingMismatch('a'), missingMismatch('b')]
    const http = httpWithHandler(async () => {
      throw new Error('network exploded')
    })

    const result = await dropConfirmedForcePushedCommits(
      'pull-request-commits',
      mismatches,
      'o',
      'r',
      http,
      log,
    )

    expect(result.mismatches).toEqual([])
    expect(result.skippedCount).toBe(0)
    expect(result.staleParentCount).toBe(0)
    expect(result.failedCount).toBe(2)
  })

  it('confirms many candidates in batches instead of one request per commit', async () => {
    const shas = Array.from({ length: 120 }, (_, i) => `sha${i}`)
    const requests = vi.fn()
    const http = httpWithHandler(async (batch) => repositoryOf(batch.map(() => 0)), requests)

    const result = await dropConfirmedForcePushedCommits(
      'pull-request-commits',
      shas.map((sha) => missingMismatch(sha)),
      'o',
      'r',
      http,
      log,
    )

    expect(requests).toHaveBeenCalledTimes(3)
    expect(result.mismatches).toEqual([])
    expect(result.skippedCount).toBe(120)
    expect(result.staleParentCount).toBe(0)
    expect(result.failedCount).toBe(0)
  })

  it('leaves non-missing_in_shadow mismatches untouched', async () => {
    const fieldMismatch: IShadowDiffMismatch = {
      sourceId: 'x',
      type: 'commit',
      kind: 'field_mismatch',
      severity: 'high',
    }
    const http = httpWithHandler(async (shas) => repositoryOf(shas.map(() => 0)))

    const result = await dropConfirmedForcePushedCommits(
      'pull-request-commits',
      [fieldMismatch, missingMismatch('orphan')],
      'o',
      'r',
      http,
      log,
    )

    expect(result.mismatches).toEqual([fieldMismatch])
  })
})
