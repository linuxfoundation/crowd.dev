import { describe, expect, it } from 'vitest'

import { cleanIdentityValue } from './transformerBase'

describe('cleanIdentityValue', () => {
  it.each(['NONE', 'none', ' None ', 'NULL', 'undefined', 'NA', 'n/a', '-'])(
    'treats the placeholder %j as missing',
    (placeholder) => {
      expect(cleanIdentityValue(placeholder)).toBeNull()
    },
  )

  it.each([null, undefined, '', '   '])('treats %j as missing', (empty) => {
    expect(cleanIdentityValue(empty)).toBeNull()
  })

  it('trims a real LF user id', () => {
    expect(cleanIdentityValue(' 0031T00003abcDE ')).toBe('0031T00003abcDE')
  })

  it('keeps usernames that only contain a placeholder word', () => {
    expect(cleanIdentityValue('nonesuch')).toBe('nonesuch')
  })

  it('stringifies a numeric user id', () => {
    expect(cleanIdentityValue(12345)).toBe('12345')
  })
})
