import { describe, expect, it } from 'vitest'

import type { IOctolensMentionPollUnit } from '@crowd/data-access-layer/src/octolens'

import { parseOctolensBackfillArgs, planOctolensBackfill } from './octolensBackfill'
import type { IOctolensBackfillState, OctolensBackfillArgs } from './octolensBackfill'

const NOW = new Date('2026-10-16T12:00:00.000Z')

function parse(...argv: string[]): OctolensBackfillArgs {
  return parseOctolensBackfillArgs(argv, NOW)
}

function unit(overrides: Partial<IOctolensMentionPollUnit> = {}): IOctolensMentionPollUnit {
  return {
    id: 'unit-1',
    integrationId: 'anchor-1',
    status: 'active',
    lockedAt: null,
    watermark: {
      startDate: '2026-10-16T11:55:00.000Z',
      cursor: null,
      pendingMaxTimestamp: null,
      backfill: [{ keywordId: 7, cursor: 'k7-page2' }],
    },
    ...overrides,
  }
}

function state(overrides: Partial<IOctolensBackfillState> = {}): IOctolensBackfillState {
  return {
    units: [unit()],
    activeMappingCounts: new Map([
      [7, 1],
      [8, 1],
      [9, 1],
    ]),
    ...overrides,
  }
}

describe('parseOctolensBackfillArgs', () => {
  it('parses keyword ids, dropping the pnpm separator and repeated ids', () => {
    expect(parse('--', '--keywords', '9,8, 9', '--dry-run')).toEqual({
      mode: 'keywords',
      keywordIds: [9, 8],
      dryRun: true,
    })
  })

  it('normalizes --from to an ISO timestamp', () => {
    expect(parse('--from', '2026-09-01')).toEqual({
      mode: 'from',
      startDate: '2026-09-01T00:00:00.000Z',
      dryRun: false,
    })
    expect(parse('--from', '2026-09-01T10:30:00+02:00')).toMatchObject({
      startDate: '2026-09-01T08:30:00.000Z',
    })
  })

  it.each([
    [[], 'exactly one'],
    [['--keywords', '1', '--from', '2026-09-01'], 'exactly one'],
    [['--keywords'], 'needs a value'],
    [['--keywords', '--dry-run'], 'needs a value'],
    [['--keywords', '1', '--keywords', '2'], 'more than once'],
    [['--keywords', '1', '--dry-run', '--dry-run'], 'more than once'],
    [['--keywords', '1', '--force'], 'unknown argument'],
    [['--keywords', '0'], 'positive integer'],
    [['--keywords', '-3'], 'positive integer'],
    [['--keywords', '1.5'], 'positive integer'],
    [['--keywords', '1,,2'], 'positive integer'],
    [['--keywords', 'abc'], 'positive integer'],
    [['--keywords', '2147483648'], 'positive integer'],
    [['--from', 'yesterday'], 'expected YYYY-MM-DD'],
    [['--from', '2026-09-01T10:00:00'], 'expected YYYY-MM-DD'],
    [['--from', '2026-02-30'], 'not a real date'],
    [['--from', '2026-13-01'], 'not a real date'],
    [['--from', '2026-10-17'], 'in the future'],
    [['--from', '2025-10-16T11:59:59Z'], 'more than 1 year back'],
    [['--from', '2026-09-01T24:00Z'], 'expected YYYY-MM-DD'],
    [['--from', '2026-09-01T10:60Z'], 'expected YYYY-MM-DD'],
  ])('rejects %j', (argv, message) => {
    expect(() => parse(...argv)).toThrow(message)
  })
})

describe('planOctolensBackfill', () => {
  it('queues only keywords that are not already in the backfill queue', () => {
    const plan = planOctolensBackfill(parse('--keywords', '7,9'), state())

    expect(plan).toMatchObject({ mode: 'keywords', keywordIdsToQueue: [9], alreadyQueued: [7] })
  })

  it('queues keywords whose only queue entry is one the poll would drop', () => {
    const watermark = {
      startDate: '2026-10-16T11:55:00.000Z',
      backfill: [{ keywordId: 7 }, { keywordId: 8, cursor: 42 }, { keywordId: 9, cursor: null }],
    }
    const plan = planOctolensBackfill(
      parse('--keywords', '7,8,9'),
      state({ units: [unit({ watermark })] }),
    )

    expect(plan).toMatchObject({ keywordIdsToQueue: [7, 8], alreadyQueued: [9] })
  })

  it('accepts --from exactly one year back', () => {
    expect(parse('--from', '2025-10-16T12:00:00Z')).toMatchObject({
      startDate: '2025-10-16T12:00:00.000Z',
    })
  })

  it('refuses a keyword mapped to more than one subproject', () => {
    const counts = new Map([
      [9, 1],
      [8, 2],
    ])

    expect(() =>
      planOctolensBackfill(parse('--keywords', '9,8'), state({ activeMappingCounts: counts })),
    ).toThrow('keyword 8 is mapped to more than one subproject')
  })

  it('refuses keywords without an active subproject mapping', () => {
    expect(() => planOctolensBackfill(parse('--keywords', '9,42,43'), state())).toThrow(
      'keywords without an active subproject mapping: 42, 43',
    )
  })

  it('rewinds to a date earlier than the current start date', () => {
    const plan = planOctolensBackfill(parse('--from', '2026-09-01'), state())

    expect(plan).toMatchObject({
      mode: 'from',
      startDate: '2026-09-01T00:00:00.000Z',
      previousStartDate: '2026-10-16T11:55:00.000Z',
    })
  })

  it('refuses a --from that would move the start date forward', () => {
    const current = unit({ watermark: { startDate: '2026-09-01T00:00:00.000Z' } })

    expect(() =>
      planOctolensBackfill(parse('--from', '2026-09-02'), state({ units: [current] })),
    ).toThrow('not earlier than the current start date')
  })

  it('accepts any past --from on a unit that has never run', () => {
    const fresh = unit({ watermark: null })

    expect(
      planOctolensBackfill(parse('--from', '2026-09-02'), state({ units: [fresh] })),
    ).toMatchObject({ previousStartDate: null })
  })

  it.each([
    [{ units: [] }, 'exactly one octolens mentionPoll sync unit, found 0'],
    [{ units: [unit(), unit({ id: 'unit-2' })] }, 'found 2'],
    [{ units: [unit({ status: 'dead_letter' })] }, 'is dead_letter, not active'],
    [{ units: [unit({ status: 'paused' })] }, 'is paused, not active'],
  ])('refuses state %j', (overrides, message) => {
    expect(() => planOctolensBackfill(parse('--keywords', '9'), state(overrides))).toThrow(message)
  })
})
