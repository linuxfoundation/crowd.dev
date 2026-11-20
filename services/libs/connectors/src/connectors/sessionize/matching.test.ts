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
  projectSlug: string | null = 'cncf',
): CventCatalogueEvent {
  return { eventId, eventName, startDate, projectId: `${projectSlug}-id`, projectSlug }
}

const subprojectFor = (projectSlug: string | null) =>
  projectSlug === 'cncf' ? 'segment-cncf' : null

describe('normalizeEventName', () => {
  it.each([
    ['KubeCon + CloudNativeCon NA 2025', 'kubecon cloudnativecon north america 2025'],
    [
      'AI_dev: Open Source GenAI & ML Summit Europe 2024',
      'ai dev open source genai and ml summit europe 2024',
    ],
    ['AGL AMM Japan 2026', 'automotive grade linux all member meeting japan 2026'],
    ['KyvernoCon Virtual 2026', 'kyvernocon 2026'],
    ['Open Source Conf EU 2025', 'open source conference europe 2025'],
  ])('%s', (name, expected) => {
    expect(normalizeEventName(name)).toBe(expected)
  })
})

describe('matchSessionizeEvent', () => {
  it('matches one exact-name candidate within 21 days', () => {
    const decision = matchSessionizeEvent(
      sessionizeEvent('KubeCon + CloudNativeCon NA 2025', '2025-11-10'),
      [
        cventEvent('cv-1', 'KubeCon CloudNativeCon North America 2025', '2025-11-10'),
        cventEvent('cv-2', 'Other Summit 2025', '2025-11-10'),
      ],
      subprojectFor,
    )
    expect(decision).toMatchObject({
      status: 'matched',
      holdReason: null,
      cventEventId: 'cv-1',
      segmentId: 'segment-cncf',
      rule: 'exact_name',
      ruleVersion: MATCHING_RULE_VERSION,
    })
    expect(decision.evidence.candidates).toEqual([
      {
        eventId: 'cv-1',
        eventName: 'KubeCon CloudNativeCon North America 2025',
        startDate: '2025-11-10',
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

  it('holds a twin as ambiguous even when the twin project has no segment', () => {
    const decision = matchSessionizeEvent(
      sessionizeEvent('Summit 2025', '2025-06-10'),
      [
        cventEvent('cv-1', 'Summit 2025', '2025-06-10', 'cncf'),
        cventEvent('cv-2', 'Summit 2025', '2025-06-12', 'unknown'),
      ],
      subprojectFor,
    )
    expect(decision).toMatchObject({
      status: 'held',
      holdReason: 'ambiguous',
      cventEventId: null,
      segmentId: null,
    })
    expect(decision.evidence.candidates.map(({ eventId }) => eventId)).toEqual(['cv-1', 'cv-2'])
  })

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
})
