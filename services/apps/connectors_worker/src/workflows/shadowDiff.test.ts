import { describe, expect, it } from 'vitest'

import { describeChannelError } from './shadowDiff'

describe('describeChannelError', () => {
  it('extracts the message from an Error so operators can see why a channel failed', () => {
    expect(describeChannelError(new Error('db exploded'))).toBe('db exploded')
  })

  it('stringifies non-Error throwables instead of losing the detail', () => {
    expect(describeChannelError('raw string failure')).toBe('raw string failure')
    expect(describeChannelError({ code: 'ECONNRESET' })).toBe('{"code":"ECONNRESET"}')
  })

  it('falls back to String() when the thrown value cannot be JSON-serialized', () => {
    const circular: Record<string, unknown> = {}
    circular.self = circular
    expect(describeChannelError(circular)).toBe(String(circular))
  })
})
