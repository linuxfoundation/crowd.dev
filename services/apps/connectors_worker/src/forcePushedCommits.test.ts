import { describe, expect, it, vi } from 'vitest'

import type { ConnectorHttp } from '@crowd/connectors'
import type { Logger } from '@crowd/logging'

import { dropConfirmedForcePushedCommits, hasForcePushCandidates } from './forcePushedCommits'
import { IShadowDiffMismatch } from './shadowDiff'

const log = { info: vi.fn(), warn: vi.fn(), error: vi.fn() } as unknown as Logger

function missingMismatch(sourceId: string): IShadowDiffMismatch {
  return { sourceId, type: 'commit', kind: 'missing_in_shadow', severity: 'high' }
}

function shasOf(variables: Record<string, string>): string[] {
  return Object.keys(variables)
    .filter((key) => key.startsWith('oid'))
    .sort((a, b) => Number(a.slice(3)) - Number(b.slice(3)))
    .map((key) => variables[key])
}

function httpWithHandler(
  handler: (shas: string[]) => Promise<unknown>,
  onRequest?: () => void,
): ConnectorHttp {
  return {
    request: async (config: { data: { variables: Record<string, string> } }) => {
      onRequest?.()
      return handler(shasOf(config.data.variables))
    },
    requestCount: () => 0,
  } as unknown as ConnectorHttp
}

function repositoryOf(counts: (number | null | undefined)[]) {
  return {
    data: {
      repository: Object.fromEntries(
        counts.map((count, i) => [
          `c${i}`,
          count === null
            ? null
            : count === undefined
              ? {}
              : { associatedPullRequests: { totalCount: count } },
        ]),
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
  it('drops commits with no associated pull request and keeps commits still on a pull request', async () => {
    const mismatches = [missingMismatch('orphan'), missingMismatch('live')]
    const http = httpWithHandler(async (shas) =>
      repositoryOf(shas.map((sha) => (sha === 'orphan' ? 0 : 1))),
    )

    const result = await dropConfirmedForcePushedCommits(
      'pull-request-commits',
      mismatches,
      'o',
      'r',
      http,
      log,
    )

    expect(result.mismatches).toEqual([missingMismatch('live')])
    expect(result.skippedCount).toBe(1)
    expect(result.failedCount).toBe(0)
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
    expect(result.failedCount).toBe(2)
  })

  it('confirms many candidates in batches instead of one request per commit', async () => {
    const shas = Array.from({ length: 120 }, (_, i) => `sha${i}`)
    const requests = vi.fn()
    const http = httpWithHandler(async (batch) => repositoryOf(batch.map(() => 0)), requests)

    const result = await dropConfirmedForcePushedCommits(
      'pull-request-commits',
      shas.map(missingMismatch),
      'o',
      'r',
      http,
      log,
    )

    expect(requests).toHaveBeenCalledTimes(3)
    expect(result.mismatches).toEqual([])
    expect(result.skippedCount).toBe(120)
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
