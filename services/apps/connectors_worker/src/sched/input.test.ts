import { describe, expect, it } from 'vitest'

import { parseRunInput, selectEvents } from './input'

const EVENT_LINE =
  '{"subdomain":"testevent2021","name":"Test Event 2021","slug":"test-project","sourceId":"src-1"}'

describe('parseRunInput', () => {
  it('reads event lines and key lines from one stream', () => {
    const input = parseRunInput(`${EVENT_LINE}\n\ntestevent2021=secret-key\nother=other-key\n`)

    expect(input.events).toEqual([
      {
        subdomain: 'testevent2021',
        name: 'Test Event 2021',
        segment: { slug: 'test-project', sourceId: 'src-1' },
      },
    ])
    expect([...input.keys]).toEqual([
      ['testevent2021', 'secret-key'],
      ['other', 'other-key'],
    ])
  })

  it('rejects an event without a project', () => {
    expect(() => parseRunInput('{"subdomain":"testevent2021","name":"Test"}')).toThrow(
      'line 1: event is missing slug, sourceId',
    )
  })

  it('rejects the same event twice', () => {
    expect(() => parseRunInput(`${EVENT_LINE}\n${EVENT_LINE}`)).toThrow(
      'line 2: event testevent2021 appears twice',
    )
  })

  it('never puts a malformed line in the error', () => {
    expect(() => parseRunInput('secret-key-without-subdomain')).toThrow(
      /^line 1: neither an event nor a "subdomain=key" line$/,
    )
  })
})

describe('selectEvents', () => {
  const { events } = parseRunInput(EVENT_LINE)

  it('keeps every event when none are named', () => {
    expect(selectEvents(events, null)).toEqual(events)
  })

  it('rejects events that were not received', () => {
    expect(() => selectEvents(events, ['testevent2021', 'missing'])).toThrow(
      'events not in the input: missing',
    )
  })
})
