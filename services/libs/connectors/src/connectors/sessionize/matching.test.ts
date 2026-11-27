import { describe, expect, it } from 'vitest'

import type { SessionizeConsoleEvent } from './consoleEvents'
import {
  type CventCatalogueEvent,
  MATCHING_RULE_VERSION,
  matchSessionizeEvent,
  normalizeEventName,
} from './matching'

function sessionizeEvent(eventName: string, eventStart: string): SessionizeConsoleEvent {
  return {
    eventKey: 'key',
    eventName,
    eventStart,
    eventEnd: eventStart,
    cfpStart: null,
    cfpEnd: null,
  }
}

function cventEvent(
  eventId: string,
  eventName: string,
  startDate: string,
  projectSlug: string | null = 'example-project',
  source: string | null = 'cvent',
): CventCatalogueEvent {
  return { eventId, eventName, startDate, projectId: `${projectSlug}-id`, projectSlug, source }
}

const subprojectFor = (projectSlug: string | null) =>
  projectSlug === 'example-project' ? 'segment-example' : null

describe('normalizeEventName', () => {
  it.each([
    ['Example Cloud + NativeCon NA 2025', 'example cloud nativecon north america 2025'],
    [
      'Sample_dev: Open Source Widgets & ML Summit Europe 2024',
      'sample dev open source widgets and ml summit europe 2024',
    ],
    [
      'Example AGL AMM Meetup 2030',
      'example automotive grade linux all member meeting meetup 2030',
    ],
    ['SampleCon Virtual 2026', 'samplecon 2026'],
    ['Open Source Conf EU 2025', 'open source conference europe 2025'],
    ['Example Summit 日本 ２０２７', 'example summit 日本 2027'],
    ['Exämple Constructor Summit', 'exämple constructor summit'],
  ])('%s', (name, expected) => {
    expect(normalizeEventName(name)).toBe(expected)
  })

  it('keeps names that differ only in non-Latin letters apart', () => {
    expect(normalizeEventName('Example Summit 日本 2027')).not.toBe(
      normalizeEventName('Example Summit 中国 2027'),
    )
  })
})

describe('matchSessionizeEvent', () => {
  it('uses rule version 4', () => {
    expect(MATCHING_RULE_VERSION).toBe(4)
  })

  it('keeps every catalogue event within 3 days on a held event, sorted by event id', () => {
    const decision = matchSessionizeEvent(
      sessionizeEvent('Fake Gadget Days 2027', '2027-05-10'),
      [
        cventEvent('cv-30', 'Fake Widget Summit 2027', '2027-05-13'),
        cventEvent('cv-40', 'Fake Widget Forum 2027', '2027-05-14'),
        cventEvent('cv-20', 'Fake Sprocket Meetup', '2027-05-07', 'example-project', 'luma'),
      ],
      subprojectFor,
    )
    expect(decision).toMatchObject({ status: 'held', holdReason: 'no_match' })
    expect(decision.evidence).toEqual({
      candidates: [],
      nearby: [
        {
          eventId: 'cv-20',
          eventName: 'Fake Sprocket Meetup',
          startDate: '2027-05-07',
          projectId: 'example-project-id',
          source: 'luma',
          dayGap: 3,
        },
        {
          eventId: 'cv-30',
          eventName: 'Fake Widget Summit 2027',
          startDate: '2027-05-13',
          projectId: 'example-project-id',
          source: 'cvent',
          dayGap: 3,
        },
      ],
    })
  })

  it('sorts nearby events by plain code-unit order of the event id', () => {
    const decision = matchSessionizeEvent(
      sessionizeEvent('Fake Gadget Days 2027', '2027-05-10'),
      [
        cventEvent('b-1', 'Fake Widget Summit 2027', '2027-05-10'),
        cventEvent('B-2', 'Fake Widget Forum 2027', '2027-05-10'),
        cventEvent('a-3', 'Fake Sprocket Meetup', '2027-05-10'),
      ],
      subprojectFor,
    )
    expect(decision.evidence.nearby.map(({ eventId }) => eventId)).toEqual(['B-2', 'a-3', 'b-1'])
  })

  it('keeps nearby events on other held decisions too', () => {
    const decision = matchSessionizeEvent(
      sessionizeEvent('Summit 2025', '2025-06-10'),
      [
        cventEvent('cv-1', 'Summit 2025', '2025-06-10', 'example-project'),
        cventEvent('cv-2', 'Summit 2025', '2025-06-12', 'unknown'),
      ],
      subprojectFor,
    )
    expect(decision.holdReason).toBe('ambiguous')
    expect(decision.evidence.nearby.map(({ eventId }) => eventId)).toEqual(['cv-1', 'cv-2'])
  })

  it('matches one exact-name candidate within 21 days', () => {
    const decision = matchSessionizeEvent(
      sessionizeEvent('Example Cloud + NativeCon NA 2025', '2025-11-10'),
      [
        cventEvent('cv-1', 'Example Cloud NativeCon North America 2025', '2025-11-10'),
        cventEvent('cv-2', 'Other Summit 2025', '2025-11-10'),
      ],
      subprojectFor,
    )
    expect(decision).toMatchObject({
      status: 'matched',
      holdReason: null,
      cventEventId: 'cv-1',
      catalogueSource: 'cvent',
      segmentId: 'segment-example',
      rule: 'exact_name',
      ruleVersion: 4,
      evidence: { nearby: [] },
    })
    expect(decision.evidence.candidates).toEqual([
      {
        eventId: 'cv-1',
        eventName: 'Example Cloud NativeCon North America 2025',
        startDate: '2025-11-10',
        projectId: 'example-project-id',
        source: 'cvent',
        dayGap: 0,
      },
    ])
  })

  it('accepts a candidate exactly 21 days away and holds at 22', () => {
    const catalogue = (startDate: string) => [cventEvent('cv-1', 'Summit 2025', startDate)]
    const event = sessionizeEvent('Summit 2025', '2025-03-01')
    expect(matchSessionizeEvent(event, catalogue('2025-03-22'), subprojectFor).status).toBe(
      'matched',
    )
    const held = matchSessionizeEvent(event, catalogue('2025-03-23'), subprojectFor)
    expect(held).toMatchObject({ status: 'held', holdReason: 'no_match', cventEventId: null })
    expect(held.evidence.candidates).toEqual([])
  })

  it('computes the day gap on calendar dates across a daylight-saving change', () => {
    const decision = matchSessionizeEvent(
      sessionizeEvent('Summit 2025', '2025-03-01'),
      [cventEvent('cv-1', 'Summit 2025', '2025-03-09T23:30:00.000Z')],
      subprojectFor,
    )
    expect(decision.evidence.candidates[0].dayGap).toBe(8)
  })

  it('holds two cvent namesakes as ambiguous', () => {
    const decision = matchSessionizeEvent(
      sessionizeEvent('Summit 2025', '2025-06-10'),
      [
        cventEvent('cv-1', 'Summit 2025', '2025-06-10', 'example-project'),
        cventEvent('cv-2', 'Summit 2025', '2025-06-12', 'unknown'),
      ],
      subprojectFor,
    )
    expect(decision).toMatchObject({
      status: 'held',
      holdReason: 'ambiguous',
      cventEventId: null,
      segmentId: null,
      ruleVersion: 4,
    })
    expect(decision.evidence.candidates.map(({ eventId }) => eventId)).toEqual(['cv-1', 'cv-2'])
  })

  it('matches the cvent candidate when a namesake comes from another source', () => {
    const decision = matchSessionizeEvent(
      sessionizeEvent('Summit 2025', '2025-06-10'),
      [
        cventEvent('lu-1', 'Summit 2025', '2025-06-11', 'example-project', 'luma'),
        cventEvent('cv-1', 'Summit 2025', '2025-06-10'),
      ],
      subprojectFor,
    )
    expect(decision).toMatchObject({
      status: 'matched',
      holdReason: null,
      cventEventId: 'cv-1',
      catalogueSource: 'cvent',
      segmentId: 'segment-example',
      rule: 'exact_name',
      ruleVersion: 4,
    })
    expect(decision.evidence.candidates.map(({ eventId }) => eventId)).toEqual(['lu-1', 'cv-1'])
  })

  it('holds several non-cvent namesakes as ambiguous', () => {
    const decision = matchSessionizeEvent(
      sessionizeEvent('Summit 2025', '2025-06-10'),
      [
        cventEvent('lu-1', 'Summit 2025', '2025-06-10', 'example-project', 'luma'),
        cventEvent('bv-1', 'Summit 2025', '2025-06-11', 'example-project', 'bevy'),
      ],
      subprojectFor,
    )
    expect(decision).toMatchObject({
      status: 'held',
      holdReason: 'ambiguous',
      cventEventId: null,
      catalogueSource: null,
      ruleVersion: 4,
    })
    expect(decision.evidence.candidates).toHaveLength(2)
  })

  it('holds the cvent candidate as no_segment when its project has no subproject, whatever the namesakes', () => {
    const decision = matchSessionizeEvent(
      sessionizeEvent('Summit 2025', '2025-06-10'),
      [
        cventEvent('cv-9', 'Summit 2025', '2025-06-10', 'unknown'),
        cventEvent('lu-1', 'Summit 2025', '2025-06-10', 'example-project', 'luma'),
      ],
      subprojectFor,
    )
    expect(decision).toMatchObject({
      status: 'held',
      holdReason: 'no_segment',
      cventEventId: 'cv-9',
      catalogueSource: 'cvent',
      segmentId: null,
      ruleVersion: 4,
    })
    expect(decision.evidence.candidates).toHaveLength(2)
  })

  it.each(['backfill', 'bevy', 'regfox', null])(
    'holds a single candidate from source %s as not_cvent',
    (source) => {
      const decision = matchSessionizeEvent(
        sessionizeEvent('Summit 2025', '2025-06-10'),
        [cventEvent('cv-1', 'Summit 2025', '2025-06-10', 'example-project', source)],
        subprojectFor,
      )
      expect(decision).toMatchObject({
        status: 'held',
        holdReason: 'not_cvent',
        cventEventId: 'cv-1',
        catalogueSource: source,
        segmentId: null,
        evidence: { candidates: [{ eventId: 'cv-1', source }] },
      })
    },
  )

  it('holds with no_segment when the project has no subproject', () => {
    const decision = matchSessionizeEvent(
      sessionizeEvent('Summit 2025', '2025-06-10'),
      [cventEvent('cv-9', 'Summit 2025', '2025-06-10', 'unknown')],
      subprojectFor,
    )
    expect(decision).toMatchObject({
      status: 'held',
      holdReason: 'no_segment',
      cventEventId: 'cv-9',
      catalogueSource: 'cvent',
      segmentId: null,
      rule: null,
    })
    expect(decision.evidence.candidates).toHaveLength(1)
  })

  it('does not match across years', () => {
    const decision = matchSessionizeEvent(
      sessionizeEvent('Summit 2025', '2025-12-30'),
      [cventEvent('cv-1', 'Summit 2026', '2026-01-05')],
      subprojectFor,
    )
    expect(decision).toMatchObject({ status: 'held', holdReason: 'no_match' })
  })

  it('never matches a name that normalizes to nothing', () => {
    const decision = matchSessionizeEvent(
      sessionizeEvent('Virtual', '2025-11-10'),
      [cventEvent('cv-1', 'Virtual', '2025-11-10')],
      subprojectFor,
    )
    expect(decision).toMatchObject({
      status: 'held',
      holdReason: 'no_match',
      evidence: {
        candidates: [],
        nearby: [
          {
            eventId: 'cv-1',
            eventName: 'Virtual',
            startDate: '2025-11-10',
            projectId: 'example-project-id',
            source: 'cvent',
            dayGap: 0,
          },
        ],
      },
    })
  })
})
