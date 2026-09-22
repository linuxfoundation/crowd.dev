import { describe, expect, it } from 'vitest'

import { stripNullBytesDeep } from '../stripNullBytesDeep'

describe('stripNullBytesDeep', () => {
  it('removes NUL bytes from nested strings', () => {
    const nul = String.fromCharCode(0)
    const v = stripNullBytesDeep({ a: `x${nul}y`, b: [`p${nul}`, 'q'] })
    expect(v.a).toBe('xy')
    expect(v.b).toEqual(['p', 'q'])
  })

  it('passes through non-string scalars unchanged', () => {
    expect(stripNullBytesDeep(42)).toBe(42)
    expect(stripNullBytesDeep(null)).toBe(null)
    expect(stripNullBytesDeep(true)).toBe(true)
  })
})
