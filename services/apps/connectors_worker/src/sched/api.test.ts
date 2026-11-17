import { describe, expect, it } from 'vitest'

import { rows, toSpeaker } from './api'

describe('toSpeaker', () => {
  it('reads sessions sent as a list', () => {
    expect(toSpeaker({ username: 'jdoe', sessions: ['a1', 'b2'] })?.sessionIds).toEqual([
      'a1',
      'b2',
    ])
  })

  it('reads sessions sent as a comma-separated string', () => {
    expect(toSpeaker({ username: 'jdoe', sessions: 'a1, b2,' })?.sessionIds).toEqual(['a1', 'b2'])
  })
})

describe('rows', () => {
  it('reads an empty list sent as false', () => {
    expect(rows('testevent2021', 'user/sessions', false)).toEqual([])
  })

  it('rejects any other non-list answer', () => {
    expect(() => rows('testevent2021', 'user/sessions', { error: 'x' })).toThrow(
      'did not return a list',
    )
  })
})
