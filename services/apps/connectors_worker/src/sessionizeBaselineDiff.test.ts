import { describe, expect, it } from 'vitest'

import type { SessionizeEventDecision } from '@crowd/connectors/src/connectors/sessionize/matching'

import {
  diffAgainstBaseline,
  formatDifference,
  parseBaseline,
  parseCsv,
} from './sessionizeBaselineDiff'

const HEADER =
  'class,rule,sessionize_key,sessionize_name,start,cvent_event_id,cvent_name,cvent_date,cdp_project_slug,cdp_source_id'

function decision(
  key: string,
  status: 'matched' | 'held',
  cventEventId: string | null,
  holdReason: SessionizeEventDecision['holdReason'] = null,
  projectId: string | null = 'P',
): SessionizeEventDecision {
  const candidates = cventEventId ? [{ eventId: cventEventId, projectId }] : []
  return {
    event: { eventKey: key, eventName: `Event ${key}` },
    status,
    holdReason,
    cventEventId,
    segmentId: null,
    rule: null,
    ruleVersion: 1,
    evidence: { candidates },
  } as unknown as SessionizeEventDecision
}

describe('parseCsv', () => {
  it('keeps commas, doubled quotes and line breaks inside quoted fields', () => {
    expect(parseCsv('a,"b,1","say ""hi""\nthere"\r\nc,d,e\n')).toEqual([
      ['a', 'b,1', 'say "hi"\nthere'],
      ['c', 'd', 'e'],
    ])
  })
})

describe('diffAgainstBaseline', () => {
  const baseline = parseBaseline(
    [
      HEADER,
      'direct,r,k1,"Alpha, Con",2027-01-01,c1,x,x,s,p',
      'direct,r,k2,Beta,2027-01-01,c2,x,x,s,p',
      'best,r,k3,Gamma,2027-01-01,c3,x,x,s,p',
      'unresolved,r,k4,Delta,2027-01-01,,,,,',
      'direct,r,k5,Gone,2027-01-01,c5,x,x,s,p',
      'direct,r,k7,Eta,2027-01-01,c7,x,x,s,p',
    ].join('\n'),
  )

  it('reports flips, changed Cvent ids and projects, missing and new keys and ignores agreement', () => {
    const diff = diffAgainstBaseline(
      [
        decision('k1', 'matched', 'c1'),
        decision('k2', 'matched', 'c9'),
        decision('k3', 'matched', 'c3'),
        decision('k4', 'held', null, 'no_match'),
        decision('k6', 'held', null, 'ambiguous'),
        decision('k7', 'matched', 'c7', null, 'q'),
      ],
      baseline,
    )
    expect(diff.differences.map((d) => d.key)).toEqual(['k2', 'k3', 'k7'])
    expect(diff.missingNow).toEqual(['k5'])
    expect(diff.newNow).toEqual(['k6'])
    expect(formatDifference(diff.differences[1])).toBe(
      'k3 | Event k3 | baseline best/c3 | now matched/-/c3',
    )
    expect(formatDifference(diff.differences[2])).toBe(
      'k7 | Event k7 | baseline direct/c7 | now matched/-/c7 project q (baseline p)',
    )
  })

  it('rejects a CSV with another header', () => {
    expect(() => parseBaseline('a,b\n1,2')).toThrow('baseline header')
  })
})
