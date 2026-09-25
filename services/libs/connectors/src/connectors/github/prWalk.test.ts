import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import type { Logger } from '@crowd/logging'

import type { ConnectorHttp } from '../../http/client'
import type { SyncContext } from '../../types'
import type { CoveredWindow } from './paging'
import { PR_PAGE_SIZE, runDualPhasePrSync } from './prWalk'

interface FakePr {
  id: string
  updatedAt: string
}

const ANCHOR = Date.parse('2026-09-22T12:00:00Z')
const CHANNEL = { channelId: 'c1', channelName: 'https://github.com/openclaw/openclaw' }

function iso(minutesAgo: number): string {
  return new Date(ANCHOR - minutesAgo * 60_000).toISOString()
}

function makePrs(count: number): FakePr[] {
  return Array.from({ length: count }, (_, i) => ({ id: `pr-${i}`, updatedAt: iso(i * 10) }))
}

interface HarnessOptions {
  failAfterPages?: number
  budgetPages?: number
}

function makeHarness(
  prs: FakePr[],
  watermark: Record<string, unknown> | null,
  opts: HarnessOptions = {},
) {
  let pagesServed = 0
  const commits: Record<string, unknown>[] = []
  const processed: string[] = []

  const http = {
    request: async (config: { data: { variables: { cursor: string | null } } }) => {
      if (opts.failAfterPages !== undefined && pagesServed >= opts.failAfterPages) {
        throw new Error('simulated 502 chain exhaustion')
      }
      const start = config.data.variables.cursor ? Number(config.data.variables.cursor) : 0
      const page = prs.slice(start, start + PR_PAGE_SIZE)
      pagesServed += 1
      return {
        data: {
          repository: {
            pullRequests: {
              pageInfo: {
                hasNextPage: start + PR_PAGE_SIZE < prs.length,
                endCursor: String(start + PR_PAGE_SIZE),
              },
              nodes: page,
            },
          },
        },
      }
    },
  } as unknown as ConnectorHttp

  const ctx: SyncContext = {
    channel: CHANNEL,
    watermark,
    emit: async () => {},
    commitWatermark: async (w) => {
      commits.push(w)
    },
    hasRunBudget: () => pagesServed < (opts.budgetPages ?? Number.MAX_SAFE_INTEGER),
    http,
    log: { info: () => {}, warn: () => {}, error: () => {} } as unknown as Logger,
  }

  const handler = async (batch: unknown[]) => {
    processed.push(...batch.map((pr) => (pr as FakePr).id))
  }

  return {
    ctx,
    handler,
    processed,
    commits,
    lastCommit: () => commits[commits.length - 1],
  }
}

describe('runDualPhasePrSync', () => {
  beforeEach(() => {
    vi.useFakeTimers({ now: ANCHOR })
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  describe('backfill phase', () => {
    it('walks every page, commits the cursor per page, and flips to incremental on completion', async () => {
      const prs = makePrs(500)
      const harness = makeHarness(prs, null)

      const outcome = await runDualPhasePrSync(harness.ctx, harness.handler)

      expect(outcome).toEqual({ complete: true })
      expect(harness.processed).toHaveLength(500)
      expect(harness.commits).toHaveLength(10)
      expect(harness.commits[0]).toEqual({
        phase: 'backfill',
        since: prs[49].updatedAt,
        cursor: '50',
      })
      expect(harness.lastCommit()).toEqual({
        phase: 'incremental',
        since: prs[499].updatedAt,
        cursor: null,
      })
    })

    it('stops on budget exhaustion with the last page cursor committed', async () => {
      const prs = makePrs(500)
      const harness = makeHarness(prs, null, { budgetPages: 3 })

      const outcome = await runDualPhasePrSync(harness.ctx, harness.handler)

      expect(outcome).toEqual({ complete: false })
      expect(harness.processed).toHaveLength(150)
      expect(harness.lastCommit()).toEqual({
        phase: 'backfill',
        since: prs[149].updatedAt,
        cursor: '150',
      })
    })
  })

  describe('incremental phase', () => {
    it('processes only PRs updated since the watermark and advances since on completion', async () => {
      const prs = makePrs(500)
      const since = prs[99].updatedAt
      const harness = makeHarness(prs, { phase: 'incremental', since })

      const outcome = await runDualPhasePrSync(harness.ctx, harness.handler)

      expect(outcome).toEqual({ complete: true })
      expect(harness.processed).toHaveLength(100)
      expect(harness.processed).toContain('pr-0')
      expect(harness.processed).toContain('pr-99')
      expect(harness.processed).not.toContain('pr-100')
      expect(harness.lastCommit()).toEqual({
        phase: 'incremental',
        since: iso(0),
        cursor: null,
      })
    })
  })

  describe('interrupted incremental runs', () => {
    it('commits the confirmed window and keeps since when the provider dies mid-walk', async () => {
      const prs = makePrs(500)
      const since = iso(6000)
      const harness = makeHarness(prs, { phase: 'incremental', since }, { failAfterPages: 4 })

      await expect(runDualPhasePrSync(harness.ctx, harness.handler)).rejects.toThrow(
        'simulated 502 chain exhaustion',
      )

      expect(harness.processed).toHaveLength(200)
      expect(harness.lastCommit()).toEqual({
        phase: 'incremental',
        since,
        cursor: null,
        coveredWindows: [{ confirmedThrough: prs[199].updatedAt, coveredUntil: prs[0].updatedAt }],
      })
    })

    it('commits the confirmed window when the run budget expires mid-walk', async () => {
      const prs = makePrs(500)
      const since = iso(6000)
      const harness = makeHarness(prs, { phase: 'incremental', since }, { budgetPages: 3 })

      const outcome = await runDualPhasePrSync(harness.ctx, harness.handler)

      expect(outcome).toEqual({ complete: false })
      expect(harness.lastCommit()).toEqual({
        phase: 'incremental',
        since,
        cursor: null,
        coveredWindows: [{ confirmedThrough: prs[149].updatedAt, coveredUntil: prs[0].updatedAt }],
      })
    })

    it('keeps a non-contiguous prior window as a separate entry when the run dies above it', async () => {
      const prs = makePrs(500)
      const since = iso(6000)
      const staleWindow = {
        confirmedThrough: prs[400].updatedAt,
        coveredUntil: prs[300].updatedAt,
      }
      const harness = makeHarness(
        prs,
        { phase: 'incremental', since, coveredWindows: [staleWindow] },
        { failAfterPages: 2 },
      )

      await expect(runDualPhasePrSync(harness.ctx, harness.handler)).rejects.toThrow(
        'simulated 502 chain exhaustion',
      )

      expect(harness.lastCommit()).toEqual({
        phase: 'incremental',
        since,
        cursor: null,
        coveredWindows: [
          { confirmedThrough: prs[99].updatedAt, coveredUntil: prs[0].updatedAt },
          staleWindow,
        ],
      })
    })

    it('does not merge into the prior window when the run dies exactly on its ceiling mid-tie', async () => {
      const prs = makePrs(500)
      const since = iso(6000)
      const tieTimestamp = iso(995)
      const priorWindow = {
        confirmedThrough: prs[300].updatedAt,
        coveredUntil: tieTimestamp,
      }
      const tiedPrs = [
        ...prs.slice(0, 99),
        { id: 'ceil-tie-a', updatedAt: tieTimestamp },
        { id: 'ceil-tie-b', updatedAt: tieTimestamp },
        ...prs.slice(100),
      ]

      const firstResume = makeHarness(
        tiedPrs,
        { phase: 'incremental', since, coveredWindows: [priorWindow] },
        { failAfterPages: 2 },
      )
      await expect(runDualPhasePrSync(firstResume.ctx, firstResume.handler)).rejects.toThrow(
        'simulated 502 chain exhaustion',
      )

      expect(firstResume.processed).toContain('ceil-tie-a')
      expect(firstResume.processed).not.toContain('ceil-tie-b')
      expect(firstResume.lastCommit()).toEqual({
        phase: 'incremental',
        since,
        cursor: null,
        coveredWindows: [
          { confirmedThrough: tieTimestamp, coveredUntil: tiedPrs[0].updatedAt },
          priorWindow,
        ],
      })

      const secondResume = makeHarness(tiedPrs, {
        phase: 'incremental',
        since,
        coveredWindows: [
          { confirmedThrough: tieTimestamp, coveredUntil: tiedPrs[0].updatedAt },
          priorWindow,
        ],
      })
      const outcome = await runDualPhasePrSync(secondResume.ctx, secondResume.handler)

      expect(outcome).toEqual({ complete: true })
      expect(secondResume.processed).toContain('ceil-tie-b')
    })

    it('merges progress into the closest prior window once the walk passes below its ceiling', async () => {
      const prs = makePrs(500)
      const since = iso(6000)
      const priorWindow = {
        confirmedThrough: prs[199].updatedAt,
        coveredUntil: prs[100].updatedAt,
      }
      const harness = makeHarness(
        prs,
        { phase: 'incremental', since, coveredWindows: [priorWindow] },
        { failAfterPages: 4 },
      )

      await expect(runDualPhasePrSync(harness.ctx, harness.handler)).rejects.toThrow(
        'simulated 502 chain exhaustion',
      )

      expect(harness.lastCommit()).toEqual({
        phase: 'incremental',
        since,
        cursor: null,
        coveredWindows: [{ confirmedThrough: prs[199].updatedAt, coveredUntil: prs[0].updatedAt }],
      })
    })

    it('carries the window forward across two runs that each die above the prior ceiling, instead of resetting it', async () => {
      const prs = makePrs(900)
      const since = iso(20000)

      const firstRun = makeHarness(prs, { phase: 'incremental', since }, { budgetPages: 3 })
      const firstOutcome = await runDualPhasePrSync(firstRun.ctx, firstRun.handler)
      expect(firstOutcome).toEqual({ complete: false })
      const afterFirst = firstRun.lastCommit() as { coveredWindows: CoveredWindow[] }
      expect(afterFirst.coveredWindows).toEqual([
        { confirmedThrough: prs[149].updatedAt, coveredUntil: prs[0].updatedAt },
      ])

      const secondRun = makeHarness(prs, afterFirst, { budgetPages: 3 })
      const secondOutcome = await runDualPhasePrSync(secondRun.ctx, secondRun.handler)
      expect(secondOutcome).toEqual({ complete: false })
      const afterSecond = secondRun.lastCommit() as { coveredWindows: CoveredWindow[] }

      // a second run that dies above the same ceiling must not lose the first run's window
      expect(afterSecond.coveredWindows).toContainEqual(afterFirst.coveredWindows[0])
    })
  })

  describe('resuming from a confirmed window', () => {
    it('skips covered PRs, replays exclusive boundaries, and catches PRs updated between runs', async () => {
      const prs = makePrs(500)
      const since = iso(6000)
      const window = {
        confirmedThrough: prs[199].updatedAt,
        coveredUntil: prs[0].updatedAt,
      }
      const updatedPr = { id: 'pr-50-v2', updatedAt: iso(-5) }
      const prsAfter = [updatedPr, ...prs.filter((pr) => pr.id !== 'pr-50')]
      const harness = makeHarness(prsAfter, {
        phase: 'incremental',
        since,
        coveredWindows: [window],
      })

      const outcome = await runDualPhasePrSync(harness.ctx, harness.handler)

      expect(outcome).toEqual({ complete: true })
      expect(harness.processed).toContain('pr-50-v2')
      expect(harness.processed).not.toContain('pr-1')
      expect(harness.processed).not.toContain('pr-198')
      expect(harness.processed).toContain('pr-0')
      expect(harness.processed).toContain('pr-199')
      expect(harness.processed).toContain('pr-200')
      expect(harness.processed.filter((id) => !id.endsWith('-v2'))).toHaveLength(302)
      expect(harness.lastCommit()).toEqual({
        phase: 'incremental',
        since: iso(0),
        cursor: null,
      })
    })

    it('replays timestamp ties sitting exactly on the window floor', async () => {
      const prs = makePrs(500)
      const since = iso(6000)
      const tiedPrs = [
        ...prs.slice(0, 49),
        { id: 'tie-a', updatedAt: iso(490) },
        { id: 'tie-b', updatedAt: iso(490) },
        ...prs.slice(51),
      ]
      const window = {
        confirmedThrough: iso(490),
        coveredUntil: tiedPrs[0].updatedAt,
      }
      const harness = makeHarness(tiedPrs, {
        phase: 'incremental',
        since,
        coveredWindows: [window],
      })

      const outcome = await runDualPhasePrSync(harness.ctx, harness.handler)

      expect(outcome).toEqual({ complete: true })
      expect(harness.processed).toContain('tie-a')
      expect(harness.processed).toContain('tie-b')
    })

    it('reads a legacy flat-field watermark as a single covered window', async () => {
      const prs = makePrs(500)
      const since = iso(6000)
      const window = {
        confirmedThrough: prs[199].updatedAt,
        coveredUntil: prs[0].updatedAt,
      }
      const harness = makeHarness(prs, { phase: 'incremental', since, ...window })

      const outcome = await runDualPhasePrSync(harness.ctx, harness.handler)

      expect(outcome).toEqual({ complete: true })
      expect(harness.processed).not.toContain('pr-1')
      expect(harness.processed).not.toContain('pr-198')
      expect(harness.processed).toContain('pr-0')
      expect(harness.processed).toContain('pr-199')
      expect(harness.processed).toContain('pr-200')
    })
  })

  describe('covered window cap', () => {
    it('drops the oldest window once the cap is exceeded, never dropping a PR from the active window', async () => {
      const prs = makePrs(500)
      const since = iso(6000)
      const priorWindows = Array.from({ length: 8 }, (_, i) => ({
        confirmedThrough: iso(2000 + i * 100 + 50),
        coveredUntil: iso(2000 + i * 100),
      }))
      const harness = makeHarness(
        prs,
        { phase: 'incremental', since, coveredWindows: priorWindows },
        { failAfterPages: 2 },
      )

      await expect(runDualPhasePrSync(harness.ctx, harness.handler)).rejects.toThrow(
        'simulated 502 chain exhaustion',
      )

      const commit = harness.lastCommit() as { coveredWindows: unknown[] }
      expect(commit.coveredWindows).toHaveLength(8)
      expect(commit.coveredWindows[0]).toEqual({
        confirmedThrough: prs[99].updatedAt,
        coveredUntil: prs[0].updatedAt,
      })
    })
  })
})
