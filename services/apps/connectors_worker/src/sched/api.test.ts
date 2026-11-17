import { describe, expect, it } from 'vitest'

import { toSpeaker } from './api'

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
