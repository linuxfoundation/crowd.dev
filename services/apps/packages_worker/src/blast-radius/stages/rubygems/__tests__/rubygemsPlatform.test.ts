import { describe, expect, it } from 'vitest'

import { RubyGemsVersionItem } from '../../../../rubygems/types'
import { pickPlatform } from '../rubygemsPlatform'

describe('pickPlatform', () => {
  it('prefers the "ruby" platform when both it and a platform-specific artifact exist', () => {
    const entries: RubyGemsVersionItem[] = [
      { number: '1.13.0', created_at: '', platform: 'x86_64-linux' },
      { number: '1.13.0', created_at: '', platform: 'ruby' },
    ]
    expect(pickPlatform(entries, '1.13.0')).toBe('ruby')
  })

  it('treats a missing platform field as "ruby"', () => {
    const entries: RubyGemsVersionItem[] = [{ number: '1.0.0', created_at: '' }]
    expect(pickPlatform(entries, '1.0.0')).toBe('ruby')
  })

  it('falls back to the only available platform when "ruby" was never published', () => {
    const entries: RubyGemsVersionItem[] = [{ number: '2.0.0', created_at: '', platform: 'java' }]
    expect(pickPlatform(entries, '2.0.0')).toBe('java')
  })

  it('returns null when no entry matches the requested version', () => {
    const entries: RubyGemsVersionItem[] = [{ number: '1.0.0', created_at: '', platform: 'ruby' }]
    expect(pickPlatform(entries, '9.9.9')).toBeNull()
  })
})
