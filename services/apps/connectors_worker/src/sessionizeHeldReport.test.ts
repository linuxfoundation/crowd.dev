import { describe, expect, it } from 'vitest'

import type { IStoredSessionizeEventMapping } from '@crowd/data-access-layer/src/sessionize'

import { HELD_REPORT_TOP, buildSessionizeHeldReport } from './sessionizeHeldReport'

const TODAY = '2027-01-10'

function stored(
  eventKey: string,
  overrides: Partial<IStoredSessionizeEventMapping> = {},
): IStoredSessionizeEventMapping {
  return {
    eventKey,
    eventName: `Fake Summit ${eventKey}`,
    eventStart: '2027-04-28',
    eventEnd: '2027-04-29',
    cfpStart: null,
    cfpEnd: null,
    status: 'held',
    holdReason: 'no_match',
    cventEventId: null,
    segmentId: null,
    rule: null,
    ruleVersion: 1,
    evidence: { candidates: [] },
    reviewedAt: null,
    lockedAt: null,
    deactivatedAt: null,
    ...overrides,
  }
}

function section(report: ReturnType<typeof buildSessionizeHeldReport>, prefix: string) {
  return report.sections.find((entry) => entry.title.startsWith(prefix))
}

describe('buildSessionizeHeldReport', () => {
  it('reports no change when the active held set is the same', () => {
    const before = [stored('1'), stored('2', { status: 'matched', holdReason: null })]
    const after = [
      stored('1', { holdReason: 'ambiguous' }),
      stored('2', { status: 'matched', holdReason: null }),
      stored('3', { deactivatedAt: '2027-01-09' }),
    ]

    const report = buildSessionizeHeldReport(before, after, TODAY)

    expect(report.changed).toBe(false)
    expect(report.title).toBe('Sessionize held events: 1 waiting for review')
    expect(section(report, 'Newly held')).toBeUndefined()
    expect(section(report, 'Released')).toBeUndefined()
  })

  it('lists a newly held and a released event, escaping the names', () => {
    const before = [stored('1'), stored('2', { eventName: 'Gone <Fake> & Co' })]
    const after = [
      stored('1'),
      stored('2', { status: 'matched', holdReason: null, segmentId: 'seg-1' }),
      stored('3', { eventName: 'Fake <Day> & Night', eventStart: '2027-02-01' }),
    ]

    const report = buildSessionizeHeldReport(before, after, TODAY)

    expect(report.changed).toBe(true)
    expect(report.title).toBe('Sessionize held events: 1 new, 1 released')
    expect(section(report, 'Newly held (1)')?.text).toBe(
      '• Fake &lt;Day&gt; &amp; Night — 2027-02-01, `no_match`, key `3`',
    )
    expect(section(report, 'Released (1)')?.text).toBe(
      '• Gone &lt;Fake&gt; &amp; Co — 2027-04-28, `no_match`, key `2`',
    )
  })

  it('counts a reactivated held row as newly held', () => {
    const before = [stored('1', { deactivatedAt: '2027-01-01' })]
    const report = buildSessionizeHeldReport(before, [stored('1')], TODAY)

    expect(report.changed).toBe(true)
    expect(section(report, 'Newly held (1)')).toBeDefined()
  })

  it('lists the top held events upcoming first by start date', () => {
    const after = [
      stored('past', { eventStart: '2026-12-01' }),
      stored('later', { eventStart: '2027-06-01' }),
      stored('soon', { eventStart: '2027-01-10' }),
      ...Array.from({ length: HELD_REPORT_TOP }, (_, index) =>
        stored(`bulk-${index}`, { eventStart: '2027-09-01' }),
      ),
    ]

    const top = section(buildSessionizeHeldReport(after, after, TODAY), 'Top')

    expect(top?.title).toBe(`Top ${HELD_REPORT_TOP} held`)
    const keys = top?.text.split('\n').map((line) => line.match(/key `(.+)`$/)?.[1])
    expect(keys?.slice(0, 2)).toEqual(['soon', 'later'])
    expect(keys).toHaveLength(HELD_REPORT_TOP)
    expect(keys).not.toContain('past')
  })
})
