import { describe, expect, it, vi } from 'vitest'

import type {
  IDeactivatedOctolensKeywordMapping,
  IOctolensMappingDiscoveryHealth,
} from '@crowd/data-access-layer/src/octolens/health'
import type { Logger } from '@crowd/logging'
import { SlackPersona } from '@crowd/slack'

import {
  DEACTIVATIONS_CURSOR_KEY,
  IOctolensMappingHealthAlert,
  IOctolensMappingHealthDeps,
  MAX_SECTION_CHARS,
  STALE_ALERTED_KEY,
  capLines,
  formatName,
  hasDeactivationBurst,
  isDeactivationBurst,
  runOctolensMappingHealthCheck,
} from './octolensMappingHealth'

const CHECKED_AT = new Date('2026-10-15T12:30:00.000Z')
const UNTIL = '2026-10-15T12:25:00.000Z'
const FIRST_LOOKBACK = '2026-10-14T12:25:00.000Z'
const RUN_AT = new Date('2026-10-15T12:00:00.000Z')
const STALE_AT = new Date('2026-10-15T08:00:00.000Z')

function health(
  overrides: Partial<IOctolensMappingDiscoveryHealth> = {},
): IOctolensMappingDiscoveryHealth {
  return {
    activeMappings: 20,
    integrations: 4,
    lastDiscoveredAt: RUN_AT,
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
    deactivatedAt: RUN_AT,
    integrationDeleted: false,
    nowMappedToSegmentName: null,
    ...overrides,
  }
}

function setup(options: {
  health: IOctolensMappingDiscoveryHealth | null
  deactivations?: IDeactivatedOctolensKeywordMapping[]
  store?: Record<string, string>
  sendSucceeds?: boolean
}) {
  const store = new Map(Object.entries(options.store ?? {}))
  const ttls = new Map<string, number | undefined>()
  const sent: IOctolensMappingHealthAlert[] = []
  const log = { warn: vi.fn(), error: vi.fn() }
  const listDeactivatedBetween = vi.fn(async () => options.deactivations ?? [])
  const deps: IOctolensMappingHealthDeps = {
    log: log as unknown as Logger,
    getHealth: async () => options.health,
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
  return { deps, store, ttls, sent, log, listDeactivatedBetween }
}

function alertText(alert: IOctolensMappingHealthAlert): string {
  return alert.sections.map((s) => `${s.title}\n${s.text}`).join('\n')
}

describe('runOctolensMappingHealthCheck', () => {
  it('stays quiet and keeps no state while there are no active mappings', async () => {
    const { deps, store, sent, listDeactivatedBetween } = setup({ health: null })

    await runOctolensMappingHealthCheck(deps)

    expect(sent).toEqual([])
    expect(store.size).toBe(0)
    expect(listDeactivatedBetween).not.toHaveBeenCalled()
  })

  it('sends nothing when healthy and moves the cursor', async () => {
    const { deps, store, sent } = setup({ health: health() })

    await runOctolensMappingHealthCheck(deps)

    expect(sent).toEqual([])
    expect(store.get(DEACTIVATIONS_CURSOR_KEY)).toBe(UNTIL)
  })

  it.each([
    { name: 'first run looks back 24h', cursor: undefined, since: FIRST_LOOKBACK },
    { name: 'later runs continue from the cursor', cursor: '2026-10-15T11:25:00.000Z' },
    { name: 'an unreadable cursor looks back 24h', cursor: 'not-a-date', since: FIRST_LOOKBACK },
  ])('queries the window: $name', async ({ cursor, since }) => {
    const { deps, store, listDeactivatedBetween } = setup({
      health: health(),
      store: cursor ? { [DEACTIVATIONS_CURSOR_KEY]: cursor } : {},
    })

    await runOctolensMappingHealthCheck(deps)

    expect(listDeactivatedBetween).toHaveBeenCalledWith(
      new Date(since ?? (cursor as string)),
      new Date(UNTIL),
    )
    expect(store.get(DEACTIVATIONS_CURSOR_KEY)).toBe(UNTIL)
  })

  it('does not query when the cursor is already at the window end', async () => {
    const { deps, listDeactivatedBetween } = setup({
      health: health(),
      store: { [DEACTIVATIONS_CURSOR_KEY]: UNTIL },
    })

    await runOctolensMappingHealthCheck(deps)

    expect(listDeactivatedBetween).not.toHaveBeenCalled()
  })

  it.each([
    { name: 'first alert of an episode', lastDiscoveredAt: STALE_AT, alerted: undefined, sends: 1 },
    {
      name: 'same episode already alerted',
      lastDiscoveredAt: STALE_AT,
      alerted: STALE_AT,
      sends: 0,
    },
    {
      name: 'new episode after an older one',
      lastDiscoveredAt: STALE_AT,
      alerted: new Date('2026-10-14T08:00:00.000Z'),
      sends: 1,
    },
    {
      name: 'within the threshold',
      lastDiscoveredAt: new Date('2026-10-15T09:30:00.000Z'),
      alerted: undefined,
      sends: 0,
    },
  ])('staleness: $name', async ({ lastDiscoveredAt, alerted, sends }) => {
    const { deps, store, ttls, sent } = setup({
      health: health({ lastDiscoveredAt }),
      store: alerted ? { [STALE_ALERTED_KEY]: alerted.toISOString() } : {},
    })

    await runOctolensMappingHealthCheck(deps)

    expect(sent).toHaveLength(sends)
    if (sends > 0) {
      expect(sent[0].persona).toBe(SlackPersona.WARNING_PROPAGATOR)
      expect(store.get(STALE_ALERTED_KEY)).toBe(lastDiscoveredAt.toISOString())
      expect(ttls.get(STALE_ALERTED_KEY)).toBe(24 * 60 * 60)
    }
  })

  it('describes the stale state and where to look', async () => {
    const { deps, sent } = setup({ health: health({ lastDiscoveredAt: STALE_AT }) })

    await runOctolensMappingHealthCheck(deps)

    const text = alertText(sent[0])
    expect(text).toContain('Last write: 2026-10-15 08:00 UTC (4h ago)')
    expect(text).toContain('Active mappings: 20 across 4 projects')
    expect(text).toContain('CROWD_OCTOLENS_API_TOKEN')
    expect(text).toContain('`--dry-run` first')
    expect(text).toContain(
      '`pnpm --filter @crowd/connectors-worker run script:backfill-octolens-mentions -- --from 2026-10-15T08:00:00.000Z`',
    )
    expect(text).toContain('script:backfill-octolens-mentions -- --keywords KEYWORD_ID,KEYWORD_ID')
    expect(text).not.toContain('nothing needs recovering')
  })

  it('sends the stale alert before the deactivation alert', async () => {
    const { deps, sent } = setup({
      health: health({ lastDiscoveredAt: STALE_AT }),
      deactivations: [deactivation(1)],
    })

    await runOctolensMappingHealthCheck(deps)

    expect(sent.map((a) => a.title)).toEqual([
      'Octolens mapping discovery looks stale',
      'Octolens keyword mappings deactivated',
    ])
  })

  it('runs both checks when one throws', async () => {
    const { deps, sent, log, store } = setup({
      health: health({ lastDiscoveredAt: STALE_AT }),
      deactivations: [deactivation(1)],
    })
    const failing = { ...deps, listDeactivatedBetween: vi.fn().mockRejectedValue(new Error('db')) }

    const result = await runOctolensMappingHealthCheck(failing)

    expect(sent.map((a) => a.title)).toEqual(['Octolens mapping discovery looks stale'])
    expect(result.failures).toBe(1)
    expect(log.error).toHaveBeenCalledOnce()
    expect(store.get(DEACTIVATIONS_CURSOR_KEY)).toBe(FIRST_LOOKBACK)

    const staleThrows = { ...deps, getKey: vi.fn(deps.getKey) }
    staleThrows.getKey.mockRejectedValueOnce(new Error('redis'))
    await runOctolensMappingHealthCheck(staleThrows)

    expect(sent[sent.length - 1].title).toBe('Octolens keyword mappings deactivated')
  })

  it.each([
    {
      name: 'stale alert stays unmarked',
      health: health({ lastDiscoveredAt: STALE_AT }),
      deactivations: [],
      store: {},
      key: STALE_ALERTED_KEY,
      expected: undefined,
    },
    {
      name: 'cursor stays put',
      health: health(),
      deactivations: [deactivation(1)],
      store: { [DEACTIVATIONS_CURSOR_KEY]: '2026-10-15T11:25:00.000Z' },
      key: DEACTIVATIONS_CURSOR_KEY,
      expected: '2026-10-15T11:25:00.000Z',
    },
    {
      name: 'first lookback is pinned',
      health: health(),
      deactivations: [deactivation(1)],
      store: {},
      key: DEACTIVATIONS_CURSOR_KEY,
      expected: FIRST_LOOKBACK,
    },
  ])('a failed send leaves state for a retry: $name', async (c) => {
    const { deps, store, log } = setup({ ...c, sendSucceeds: false })

    const result = await runOctolensMappingHealthCheck(deps)

    expect(result.failures).toBe(1)
    expect(store.get(c.key)).toBe(c.expected)
    expect(log.warn).toHaveBeenCalledOnce()
  })

  it('logs the window start when the deactivation alert fails to send', async () => {
    const { deps, log } = setup({
      health: health(),
      deactivations: [deactivation(1)],
      sendSucceeds: false,
    })

    await runOctolensMappingHealthCheck(deps)

    expect(log.warn).toHaveBeenCalledWith(
      { windowStart: FIRST_LOOKBACK, deactivations: 1 },
      expect.any(String),
    )
  })

  it('reports a burst in one run as a warning listing every keyword and project', async () => {
    const { deps, store, sent } = setup({
      health: health(),
      deactivations: [
        deactivation(1),
        deactivation(2),
        deactivation(3, {
          integrationId: 'int-2',
          segmentName: 'Project Two',
          segmentSlug: 'project-two',
          integrationDeleted: true,
        }),
      ],
    })

    await runOctolensMappingHealthCheck(deps)

    expect(sent).toHaveLength(1)
    expect(sent[0].persona).toBe(SlackPersona.WARNING_PROPAGATOR)
    expect(sent[0].title).toBe('Octolens keyword mappings deactivated in bulk')
    const text = alertText(sent[0])
    expect(text).toContain('Window: 2026-10-14 12:25 UTC → 2026-10-15 12:25 UTC')
    expect(text).toContain('Deactivated mappings (3, 20 still active)')
    expect(text).toContain(
      '`keyword-1` (1) → `Project One` (`project-one`), deactivated 2026-10-15 12:00 UTC',
    )
    expect(text).toContain(
      '`keyword-3` (3) → `Project Two` (`project-two`), deactivated 2026-10-15 12:00 UTC, project has no Octolens keywords left',
    )
    expect(text).toContain('Nothing is re-fetched automatically')
    expect(text).toContain('script:backfill-octolens-mentions -- --keywords KEYWORD_ID,KEYWORD_ID')
    expect(store.get(DEACTIVATIONS_CURSOR_KEY)).toBe(UNTIL)
  })

  it('reports deactivations spread over runs as info', async () => {
    const { deps, sent } = setup({
      health: health(),
      deactivations: [10, 11, 12].map((hour, i) =>
        deactivation(i + 1, { deactivatedAt: new Date(`2026-10-15T${hour}:00:00.000Z`) }),
      ),
    })

    await runOctolensMappingHealthCheck(deps)

    expect(sent[0].persona).toBe(SlackPersona.INFO_NOTIFIER)
  })

  it('names the new project of a moved keyword and leaves moves out of the burst', async () => {
    const { deps, sent } = setup({
      health: health(),
      deactivations: [1, 2, 3].map((id) =>
        deactivation(id, { nowMappedToSegmentName: id === 1 ? 'Project Two' : null }),
      ),
    })

    await runOctolensMappingHealthCheck(deps)

    expect(sent[0].persona).toBe(SlackPersona.INFO_NOTIFIER)
    expect(alertText(sent[0])).toContain(
      '`keyword-1` (1) → `Project One` (`project-one`), deactivated 2026-10-15 12:00 UTC, now mapped to `Project Two`',
    )
  })

  it('caps the listed mappings by count and keeps the full count', async () => {
    const { deps, sent } = setup({
      health: health(),
      deactivations: Array.from({ length: 30 }, (_, i) => deactivation(i + 1)),
    })

    await runOctolensMappingHealthCheck(deps)

    const text = alertText(sent[0])
    expect(text).toContain('Deactivated mappings (30, 20 still active)')
    expect(text).toContain('`keyword-25` (25)')
    expect(text).not.toContain('`keyword-26` (26)')
    expect(text).toContain('… and 5 more')
  })

  it('lists the newest deactivations first when the list is capped', async () => {
    const at = (minute: number) => new Date(RUN_AT.getTime() + minute * 60 * 1000)
    const { deps, sent } = setup({
      health: health(),
      deactivations: Array.from({ length: 30 }, (_, i) =>
        deactivation(i + 1, { deactivatedAt: at(i) }),
      ),
    })

    await runOctolensMappingHealthCheck(deps)

    const lines = sent[0].sections[1].text.split('\n')
    expect(lines[0]).toContain('`keyword-30` (30)')
    expect(lines[24]).toContain('`keyword-6` (6)')
    expect(lines[25]).toBe('… and 5 more')
  })

  it('keeps every section under the Slack block size after escaping long names', async () => {
    const { deps, sent } = setup({
      health: health(),
      deactivations: Array.from({ length: 25 }, (_, i) =>
        deactivation(i + 1, { keyword: '&'.repeat(500), segmentName: '<'.repeat(500) }),
      ),
    })

    await runOctolensMappingHealthCheck(deps)

    for (const section of sent[0].sections) {
      expect(section.text.length).toBeLessThanOrEqual(MAX_SECTION_CHARS)
    }
    expect(JSON.stringify(sent[0]).length).toBeLessThan(20 * 1024)
    expect(alertText(sent[0])).toMatch(/… and \d+ more$/)
  })
})

describe('formatName', () => {
  it.each([
    { name: 'plain', input: 'kubernetes', expected: '`kubernetes`' },
    { name: 'mrkdwn markers', input: '*bold* _it_ ~st~', expected: '`*bold* _it_ ~st~`' },
    { name: 'backtick', input: 'a`b', expected: '`aˋb`' },
    { name: 'html entities', input: '<a&b>', expected: '`&lt;a&amp;b&gt;`' },
    { name: 'newlines', input: 'a\nb', expected: '`a b`' },
  ])('$name', ({ input, expected }) => {
    expect(formatName(input)).toBe(expected)
  })
})

describe('capLines', () => {
  it.each([
    { name: 'fits', lines: ['a', 'b'], maxLines: 5, maxChars: 100, expected: 'a\nb' },
    {
      name: 'line cap',
      lines: ['a', 'b', 'c'],
      maxLines: 2,
      maxChars: 100,
      expected: 'a\nb\n… and 1 more',
    },
    {
      name: 'char cap',
      lines: ['x'.repeat(30), 'y'.repeat(30)],
      maxLines: 5,
      maxChars: 60,
      expected: `${'x'.repeat(30)}\n… and 1 more`,
    },
  ])('$name', ({ lines, maxLines, maxChars, expected }) => {
    expect(capLines(lines, maxLines, maxChars)).toBe(expected)
  })
})

describe('isDeactivationBurst', () => {
  it.each([
    [3, 1000, true],
    [1, 3, true],
    [2, 6, true],
    [2, 7, false],
  ])('%i deactivated with %i still active is a burst: %s', (deactivated, active, expected) => {
    expect(isDeactivationBurst(deactivated, active)).toBe(expected)
  })
})

describe('hasDeactivationBurst', () => {
  it('judges each run separately by its shared deactivation timestamp', () => {
    const at = (hour: number) => new Date(`2026-10-15T${hour}:00:00.000Z`)
    const spread = [10, 11, 12].map((hour, i) => deactivation(i + 1, { deactivatedAt: at(hour) }))
    const oneRun = [1, 2, 3].map((id) => deactivation(id, { deactivatedAt: at(10) }))

    expect(hasDeactivationBurst(spread, 100)).toBe(false)
    expect(hasDeactivationBurst(oneRun, 100)).toBe(true)
  })
})
