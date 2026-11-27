import { describe, expect, it } from 'vitest'

import type { SessionizeHoldReason, SessionizeMatchCandidate } from './matching'
import { suggestSessionizeMapping } from './suggestions'
import type { SessionizeSuggestion } from './suggestions'

function event(eventId: string, eventName: string): SessionizeMatchCandidate {
  return {
    eventId,
    eventName,
    startDate: '2026-06-10',
    projectId: `project-${eventId}`,
    source: 'cvent',
    dayGap: 1,
  }
}

const stored = event('stored', 'Example Con Europe 2026')
const similar = event('similar', 'Sample Developer Summit Berlin 2026')
const nearTwin = event('near-twin', 'Acme Cloud Summits Dublin')
const nearRunnerUp = event('near-runner-up', 'Acme Cloud Summit Dubai')
const contained = event('contained', 'Example Con Europe Summit 2026')
const mainEvent = event('main', 'Example Con Europe 2026')
const kubeCon = event('kube', 'KubeCon + CloudNativeCon Europe 2026')
const loose = event('loose', 'Acme Cloud Summit Dublin')
const unrelated = event('unrelated', 'Zephyr Hack Night')

function suggestion(overrides: Partial<SessionizeSuggestion>): SessionizeSuggestion {
  return {
    kind: null,
    reason: null,
    cventEvent: null,
    projectSlug: null,
    hintCventEvent: null,
    ...overrides,
  }
}

interface Row {
  name: string
  eventName: string
  holdReason: SessionizeHoldReason
  candidates?: SessionizeMatchCandidate[]
  nearby?: SessionizeMatchCandidate[]
  expected: SessionizeSuggestion
}

const rows: Row[] = [
  {
    name: 'a single stored candidate held for no_segment is same_name',
    eventName: 'Example Con Europe 2026',
    holdReason: 'no_segment',
    candidates: [stored],
    nearby: [similar],
    expected: suggestion({ kind: 'same_name', cventEvent: stored }),
  },
  {
    name: 'a single stored candidate held for not_cvent is same_name',
    eventName: 'Example Con Europe 2026',
    holdReason: 'not_cvent',
    candidates: [stored],
    expected: suggestion({ kind: 'same_name', cventEvent: stored }),
  },
  {
    name: 'two stored candidates do not make a same_name suggestion',
    eventName: 'Example Con Europe 2026',
    holdReason: 'ambiguous',
    candidates: [stored, mainEvent],
    expected: suggestion({ reason: 'ambiguous' }),
  },
  {
    name: 'a clear similar name is similar_name',
    eventName: 'Sample Dev Summit Berlin 2026',
    holdReason: 'no_match',
    nearby: [unrelated, similar],
    expected: suggestion({ kind: 'similar_name', cventEvent: similar }),
  },
  {
    name: 'a lone similar name is similar_name',
    eventName: 'Sample Dev Summit Berlin 2026',
    holdReason: 'no_match',
    nearby: [similar],
    expected: suggestion({ kind: 'similar_name', cventEvent: similar }),
  },
  {
    name: 'two near-equal similar names are ambiguous with the top one as hint',
    eventName: 'Acme Cloud Summit Dublin',
    holdReason: 'no_match',
    nearby: [nearRunnerUp, nearTwin],
    expected: suggestion({ reason: 'ambiguous', hintCventEvent: nearTwin }),
  },
  {
    name: 'a single contained name is name_contained',
    eventName: 'Example Con Europe 2026',
    holdReason: 'no_match',
    nearby: [contained, unrelated],
    expected: suggestion({ kind: 'name_contained', cventEvent: contained }),
  },
  {
    name: 'two contained names are not name_contained',
    eventName: 'Example Con Europe 2026',
    holdReason: 'no_match',
    nearby: [contained, event('other', 'Example Con Europe Summit Days 2026')],
    expected: suggestion({ reason: 'ambiguous', hintCventEvent: contained }),
  },
  {
    name: 'a sub-event with its main event nearby is sub_event',
    eventName: 'Maintainer Summit: Example Con Europe 2026',
    holdReason: 'no_match',
    nearby: [mainEvent],
    expected: suggestion({ kind: 'sub_event', cventEvent: mainEvent }),
  },
  {
    name: 'a sub-event without its main event is main_event_not_in_cdp',
    eventName: 'Maintainer Summit: Example Con Europe 2026',
    holdReason: 'no_match',
    nearby: [unrelated],
    expected: suggestion({ reason: 'main_event_not_in_cdp' }),
  },
  {
    name: 'a sub-event without nearby events is main_event_not_in_cdp',
    eventName: 'Example Con Europe 2026 - Poster Sessions',
    holdReason: 'no_match',
    expected: suggestion({ reason: 'main_event_not_in_cdp' }),
  },
  {
    name: 'the CNCF co-located rewrite finds the KubeCon event',
    eventName: 'CNCF-hosted Co-located Events Europe 2026',
    holdReason: 'no_match',
    nearby: [unrelated, kubeCon],
    expected: suggestion({ kind: 'sub_event', cventEvent: kubeCon }),
  },
  {
    name: 'a sonic workshop with its main event nearby is sub_event',
    eventName: 'Sonic Workshop at Example Con Europe 2026',
    holdReason: 'no_match',
    nearby: [mainEvent],
    expected: suggestion({ kind: 'sub_event', cventEvent: mainEvent }),
  },
  {
    name: 'a similar name that looks like a sub-event is relabelled sub_event',
    eventName: 'Example Con Europe 2026 ContribFest',
    holdReason: 'no_match',
    nearby: [mainEvent],
    expected: suggestion({ kind: 'sub_event', cventEvent: mainEvent }),
  },
  {
    name: 'an alias name is project_alias',
    eventName: 'PyTorch Day Example 2026',
    holdReason: 'no_match',
    expected: suggestion({ kind: 'project_alias', projectSlug: 'ptproject' }),
  },
  {
    name: 'a sub-event pattern without its main event falls through to the alias',
    eventName: 'Sonic Workshop at Example Con Europe 2026',
    holdReason: 'no_match',
    nearby: [unrelated],
    expected: suggestion({ kind: 'project_alias', projectSlug: 'sonic-foundation' }),
  },
  {
    name: 'a middling similarity is ambiguous with the top event as hint',
    eventName: 'Acme Cloud Forum Lisbon',
    holdReason: 'no_match',
    nearby: [loose, unrelated],
    expected: suggestion({ reason: 'ambiguous', hintCventEvent: loose }),
  },
  {
    name: 'an ambiguous hold without a close event is ambiguous without a hint',
    eventName: 'Zephyr Community Meetup Lisbon 2026',
    holdReason: 'ambiguous',
    nearby: [unrelated],
    expected: suggestion({ reason: 'ambiguous' }),
  },
  {
    name: 'missing nearby events give no_cvent_event',
    eventName: 'Zephyr Community Meetup Lisbon 2026',
    holdReason: 'no_match',
    expected: suggestion({ reason: 'no_cvent_event' }),
  },
  {
    name: 'a name without letters gives no_cvent_event',
    eventName: '🎉',
    holdReason: 'no_match',
    nearby: [mainEvent],
    expected: suggestion({ reason: 'no_cvent_event' }),
  },
]

describe('suggestSessionizeMapping', () => {
  it.each(rows)('$name', ({ eventName, holdReason, candidates = [], nearby, expected }) => {
    expect(
      suggestSessionizeMapping({
        eventName,
        eventStart: '2026-06-09',
        holdReason,
        candidates,
        nearby,
      }),
    ).toStrictEqual(expected)
  })

  it('breaks ties between equally similar events by eventId', () => {
    const first = event('a-event', 'Acme Cloud Summit Dublin')
    const second = event('b-event', 'Acme Cloud Summit Dublin')
    const input = {
      eventName: 'Acme Cloud Forum Lisbon',
      eventStart: '2026-06-09',
      holdReason: 'no_match' as const,
      candidates: [],
    }
    const expected = suggestion({ reason: 'ambiguous', hintCventEvent: first })
    expect(suggestSessionizeMapping({ ...input, nearby: [second, first] })).toStrictEqual(expected)
    expect(suggestSessionizeMapping({ ...input, nearby: [first, second] })).toStrictEqual(expected)
  })
})
