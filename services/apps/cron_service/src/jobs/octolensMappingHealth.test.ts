import { describe, expect, it, vi } from 'vitest'

import type {
  IDeactivatedOctolensKeywordMapping,
  IOctolensMappingDiscoveryState,
} from '@crowd/data-access-layer'
import { SlackPersona } from '@crowd/slack'

import {
  DEACTIVATIONS_CURSOR_KEY,
  IOctolensMappingHealthAlert,
  IOctolensMappingHealthDeps,
  isDeactivationBurst,
  runOctolensMappingHealthCheck,
  staleAlertedKey,
} from './octolensMappingHealth'

const CHECKED_AT = new Date('2026-10-15T12:30:00.000Z')
const UNTIL = '2026-10-15T12:25:00.000Z'

function state(
  overrides: Partial<IOctolensMappingDiscoveryState> = {},
): IOctolensMappingDiscoveryState {
  return {
    integrationId: 'int-1',
    activeMappings: 20,
    lastDiscoveredAt: new Date('2026-10-15T12:05:00.000Z'),
    integrationCreatedAt: new Date('2026-09-01T00:00:00.000Z'),
    checkedAt: CHECKED_AT,
    ...overrides,
  }
}

function deactivation(
  keywordId: number,
  overrides: Partial<IDeactivatedOctolensKeywordMapping> = {},
): IDeactivatedOctolensKeywordMapping {
  return {
    integrationId: 'int-1',
    keywordId,
    keyword: `keyword-${keywordId}`,
    segmentId: 'seg-1',
    segmentName: 'Project One',
    segmentSlug: 'project-one',
    deactivatedAt: new Date('2026-10-15T12:05:00.000Z'),
    ...overrides,
  }
}

function setup(options: {
  states: IOctolensMappingDiscoveryState[]
  deactivations?: IDeactivatedOctolensKeywordMapping[]
  store?: Record<string, string>
  sendSucceeds?: boolean
}) {
  const store = new Map(Object.entries(options.store ?? {}))
  const ttls = new Map<string, number | undefined>()
  const sent: IOctolensMappingHealthAlert[] = []
  const listDeactivatedBetween = vi.fn(async () => options.deactivations ?? [])
  const deps: IOctolensMappingHealthDeps = {
    listStates: async () => options.states,
    listDeactivatedBetween,
    getKey: async (key) => store.get(key) ?? null,
    setKey: async (key, value, expireAfterSeconds) => {
      store.set(key, value)
      ttls.set(key, expireAfterSeconds)
    },
    sendAlert: async (alert) => {
      sent.push(alert)
      return options.sendSucceeds ?? true
    },
  }
  return { deps, store, ttls, sent, listDeactivatedBetween }
}

function alertText(alert: IOctolensMappingHealthAlert): string {
  return alert.sections.map((s) => `${s.title}\n${s.text}`).join('\n')
}

describe('runOctolensMappingHealthCheck', () => {
  it('does nothing when no integration has mappings yet', async () => {
    const { deps, store, sent, listDeactivatedBetween } = setup({ states: [] })

    await runOctolensMappingHealthCheck(deps)

    expect(sent).toEqual([])
    expect(store.size).toBe(0)
    expect(listDeactivatedBetween).not.toHaveBeenCalled()
  })

  it('sends nothing for a healthy integration and moves the cursor', async () => {
    const { deps, store, sent } = setup({ states: [state()] })

    await runOctolensMappingHealthCheck(deps)

    expect(sent).toEqual([])
    expect(store.get(DEACTIVATIONS_CURSOR_KEY)).toBe(UNTIL)
  })

  it('looks back 24h on the first run and continues from the cursor afterwards', async () => {
    const first = setup({ states: [state()] })
    await runOctolensMappingHealthCheck(first.deps)
    expect(first.listDeactivatedBetween).toHaveBeenCalledWith(
      new Date('2026-10-14T12:25:00.000Z'),
      new Date(UNTIL),
    )

    const next = setup({
      states: [state()],
      store: { [DEACTIVATIONS_CURSOR_KEY]: '2026-10-15T11:25:00.000Z' },
    })
    await runOctolensMappingHealthCheck(next.deps)
    expect(next.listDeactivatedBetween).toHaveBeenCalledWith(
      new Date('2026-10-15T11:25:00.000Z'),
      new Date(UNTIL),
    )
  })

  it('alerts on a stale integration once, then again only after the reminder expires', async () => {
    const stale = state({ lastDiscoveredAt: new Date('2026-10-15T08:00:00.000Z') })
    const { deps, store, ttls, sent } = setup({ states: [stale] })

    await runOctolensMappingHealthCheck(deps)
    await runOctolensMappingHealthCheck(deps)

    expect(sent).toHaveLength(1)
    expect(sent[0].persona).toBe(SlackPersona.WARNING_PROPAGATOR)
    expect(alertText(sent[0])).toContain('2026-10-15 08:00 UTC (4h ago)')
    expect(alertText(sent[0])).toContain('Active mappings: 20')
    expect(alertText(sent[0])).not.toContain('No active mappings')
    expect(ttls.get(staleAlertedKey('int-1'))).toBe(24 * 60 * 60)

    store.delete(staleAlertedKey('int-1'))
    await runOctolensMappingHealthCheck(deps)

    expect(sent).toHaveLength(2)
  })

  it('gives a never-discovered integration 3h from creation before alerting', async () => {
    const { deps, sent } = setup({
      states: [
        state({
          activeMappings: 0,
          lastDiscoveredAt: null,
          integrationCreatedAt: new Date('2026-10-15T10:00:00.000Z'),
        }),
      ],
    })

    await runOctolensMappingHealthCheck(deps)

    expect(sent).toEqual([])
  })

  it('alerts once on an integration whose discovery never completed', async () => {
    const { deps, store, sent } = setup({
      states: [
        state({
          activeMappings: 0,
          lastDiscoveredAt: null,
          integrationCreatedAt: new Date('2026-10-15T08:00:00.000Z'),
        }),
      ],
    })

    await runOctolensMappingHealthCheck(deps)
    await runOctolensMappingHealthCheck(deps)

    expect(sent).toHaveLength(1)
    expect(alertText(sent[0])).toContain(
      'Discovery has never completed a run since the integration was created at 2026-10-15 08:00 UTC (4h ago)',
    )
    expect(alertText(sent[0])).not.toContain('Last successful discovery')
    expect(store.get(staleAlertedKey('int-1'))).toBe('never:2026-10-15T08:00:00.000Z')
  })

  it('alerts again when a new stale episode starts', async () => {
    const { deps, sent } = setup({
      states: [state({ lastDiscoveredAt: new Date('2026-10-15T08:00:00.000Z') })],
      store: { [staleAlertedKey('int-1')]: '2026-10-14T08:00:00.000Z' },
    })

    await runOctolensMappingHealthCheck(deps)

    expect(sent).toHaveLength(1)
  })

  it('does not treat discovery within the stale threshold as stale', async () => {
    const { deps, sent } = setup({
      states: [state({ lastDiscoveredAt: new Date('2026-10-15T09:30:00.000Z') })],
    })

    await runOctolensMappingHealthCheck(deps)

    expect(sent).toEqual([])
  })

  it('keeps alerting a stale integration while the Slack send fails', async () => {
    const { deps, store, sent } = setup({
      states: [state({ lastDiscoveredAt: new Date('2026-10-15T08:00:00.000Z') })],
      sendSucceeds: false,
    })

    await runOctolensMappingHealthCheck(deps)
    await runOctolensMappingHealthCheck(deps)

    expect(sent).toHaveLength(2)
    expect(store.has(staleAlertedKey('int-1'))).toBe(false)
  })

  it('reports a burst of deactivations as a warning listing every keyword', async () => {
    const { deps, store, sent } = setup({
      states: [state()],
      deactivations: [deactivation(1), deactivation(2), deactivation(3)],
    })

    await runOctolensMappingHealthCheck(deps)

    expect(sent).toHaveLength(1)
    expect(sent[0].persona).toBe(SlackPersona.WARNING_PROPAGATOR)
    const text = alertText(sent[0])
    expect(text).toContain('3 of 23 mappings deactivated')
    for (const keywordId of [1, 2, 3]) {
      expect(text).toContain(
        `keyword-${keywordId} (${keywordId}) → Project One (project-one), deactivated 2026-10-15 12:05 UTC`,
      )
    }
    expect(text).toContain('re-fetches its whole history automatically')
    expect(store.get(DEACTIVATIONS_CURSOR_KEY)).toBe(UNTIL)
  })

  it('retries from the first lookback when the first send fails without a cursor', async () => {
    const options = {
      states: [state()],
      deactivations: [deactivation(1)],
      sendSucceeds: false,
    }
    const { deps, store, listDeactivatedBetween } = setup(options)

    await runOctolensMappingHealthCheck(deps)
    expect(store.get(DEACTIVATIONS_CURSOR_KEY)).toBe('2026-10-14T12:25:00.000Z')

    options.states = [state({ checkedAt: new Date('2026-10-15T14:30:00.000Z') })]
    options.sendSucceeds = true
    await runOctolensMappingHealthCheck(deps)

    expect(listDeactivatedBetween).toHaveBeenLastCalledWith(
      new Date('2026-10-14T12:25:00.000Z'),
      new Date('2026-10-15T14:25:00.000Z'),
    )
    expect(store.get(DEACTIVATIONS_CURSOR_KEY)).toBe('2026-10-15T14:25:00.000Z')
  })

  it('caps listed deactivations per integration but keeps the full count', async () => {
    const { deps, sent } = setup({
      states: [state()],
      deactivations: Array.from({ length: 30 }, (_, i) => deactivation(i + 1)),
    })

    await runOctolensMappingHealthCheck(deps)

    const text = alertText(sent[0])
    expect(text).toContain('30 of 50 mappings deactivated')
    expect(text).toContain('keyword-25 (25)')
    expect(text).not.toContain('keyword-26 (26)')
    expect(text).toContain('… and 5 more')
  })

  it('flags a stale integration that has no active mappings left', async () => {
    const { deps, sent } = setup({
      states: [
        state({ activeMappings: 0, lastDiscoveredAt: new Date('2026-10-15T08:00:00.000Z') }),
      ],
    })

    await runOctolensMappingHealthCheck(deps)

    expect(alertText(sent[0])).toContain(
      'No active mappings: every keyword is unmapped, so no mentions are being ingested.',
    )
  })

  it('reports a single deactivation as info', async () => {
    const { deps, sent } = setup({ states: [state()], deactivations: [deactivation(1)] })

    await runOctolensMappingHealthCheck(deps)

    expect(sent).toHaveLength(1)
    expect(sent[0].persona).toBe(SlackPersona.INFO_NOTIFIER)
    expect(alertText(sent[0])).toContain('Window: 2026-10-14 12:25 UTC → 2026-10-15 12:25 UTC')
  })

  it('keeps the cursor when the deactivation alert fails so the next run retries', async () => {
    const cursor = '2026-10-15T11:25:00.000Z'
    const { deps, store, sent } = setup({
      states: [state()],
      deactivations: [deactivation(1)],
      store: { [DEACTIVATIONS_CURSOR_KEY]: cursor },
      sendSucceeds: false,
    })

    const result = await runOctolensMappingHealthCheck(deps)

    expect(sent).toHaveLength(1)
    expect(result.sendFailures).toBe(1)
    expect(store.get(DEACTIVATIONS_CURSOR_KEY)).toBe(cursor)
  })
})

describe('isDeactivationBurst', () => {
  it('flags three or more deactivations regardless of size', () => {
    expect(isDeactivationBurst(3, 1000)).toBe(true)
  })

  it('flags a quarter or more of the mappings', () => {
    expect(isDeactivationBurst(1, 3)).toBe(true)
    expect(isDeactivationBurst(2, 6)).toBe(true)
    expect(isDeactivationBurst(2, 7)).toBe(false)
  })
})
