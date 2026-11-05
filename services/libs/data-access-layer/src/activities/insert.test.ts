import { describe, expect, it } from 'vitest'

import { serializeAttributes, trimAttributeStrings, tryToUnwrapAttributes } from './insert'

describe('trimAttributeStrings', () => {
  it('keeps every key of an oversized attributes object instead of wiping it', () => {
    const attributes = {
      labels: Array.from({ length: 50 }, (_, i) => ({ name: `label-${i}`, color: 'ff0000' })),
      state: 'open',
      additions: 12,
      merged: false,
    }

    expect(trimAttributeStrings(attributes)).toEqual(attributes)
  })

  it('trims a long string value to 500 characters plus an ellipsis', () => {
    const trimmed = trimAttributeStrings({ comment: 'x'.repeat(2000) }) as { comment: string }

    expect(trimmed.comment).toBe(`${'x'.repeat(500)}...`)
  })

  it('trims strings inside nested objects and arrays', () => {
    const trimmed = trimAttributeStrings({
      review: { response: 'y'.repeat(800), rating: 4 },
      tags: ['short', 'z'.repeat(800)],
    }) as { review: { response: string; rating: number }; tags: string[] }

    expect(trimmed.review.response).toHaveLength(503)
    expect(trimmed.review.rating).toBe(4)
    expect(trimmed.tags[0]).toBe('short')
    expect(trimmed.tags[1]).toHaveLength(503)
  })

  it('leaves numbers, booleans, nulls and array lengths untouched', () => {
    const attributes = { count: 7, flag: true, missing: null, list: Array(1000).fill(1) }

    expect(trimAttributeStrings(attributes)).toEqual(attributes)
  })

  it('does not split a surrogate pair at the cut point', () => {
    const value = `${'a'.repeat(499)}😀tail`

    const trimmed = (trimAttributeStrings({ v: value }) as { v: string }).v

    expect(trimmed).toBe(`${'a'.repeat(499)}...`)
  })
})

describe('tryToUnwrapAttributes', () => {
  it('returns an object as is', () => {
    const attributes = { url: 'https://example.com' }
    expect(tryToUnwrapAttributes(attributes)).toBe(attributes)
  })

  it('unwraps attributes encoded as JSON, also when encoded more than once', () => {
    const attributes = { state: 'open' }
    expect(tryToUnwrapAttributes(JSON.stringify(attributes))).toEqual(attributes)
    expect(tryToUnwrapAttributes(JSON.stringify(JSON.stringify(attributes)))).toEqual(attributes)
  })

  it.each(['5', 'true', 'null', JSON.stringify('5')])(
    'returns empty attributes for %s instead of looping forever',
    (encoded) => {
      expect(tryToUnwrapAttributes(encoded)).toEqual({})
    },
  )
})

describe('serializeAttributes', () => {
  const MAX_BYTES = 16 * 1024
  const expectUnderCap = (json: string) =>
    expect(Buffer.byteLength(json)).toBeLessThanOrEqual(MAX_BYTES)

  it('serializes attributes under the cap exactly as before, with only the per-string trim', () => {
    const result = serializeAttributes({
      state: 'open',
      additions: 12,
      merged: false,
      missing: null,
      labels: [{ name: 'bug' }],
      comment: 'x'.repeat(600),
    })

    expect(result.pruned).toBe(false)
    expect(result.json).toBe(
      `{"state":"open","additions":12,"merged":false,"missing":null,"labels":[{"name":"bug"}],"comment":"${'x'.repeat(500)}..."}`,
    )
  })

  it('shrinks the largest value and leaves small attributes like isIndirectFork intact', () => {
    const attributes = {
      state: 'open',
      count: 3,
      isIndirectFork: true,
      url: 'https://github.com/org/repo/pull/1',
      bigNumbers: Array(5000).fill(123456),
      mediumNumbers: Array(1000).fill(1),
    }

    const result = serializeAttributes(attributes)
    const parsed = JSON.parse(result.json)

    expect(result.pruned).toBe(true)
    expect(result.originalBytes).toBeGreaterThan(MAX_BYTES)
    expectUnderCap(result.json)
    expect(parsed.bigNumbers.length).toBeLessThan(5000)
    expect(parsed.bigNumbers).toEqual(attributes.bigNumbers.slice(0, parsed.bigNumbers.length))
    expect(parsed).toEqual({ ...attributes, bigNumbers: parsed.bigNumbers })
  })

  it('drops the largest strings first, alphabetically on ties, before small scalars', () => {
    const attributes: Record<string, unknown> = Object.fromEntries(
      Array.from({ length: 40 }, (_, i) => [`s${String(i).padStart(2, '0')}`, 's'.repeat(1000)]),
    )
    attributes.isIndirectFork = true

    const parsed = JSON.parse(serializeAttributes(attributes).json)

    expect(parsed.isIndirectFork).toBe(true)
    expect(parsed.s00).toBeUndefined()
    expect(parsed.s39).toBe(`${'s'.repeat(500)}...`)
  })

  it('shrinks a single nested value over the cap instead of dropping it', () => {
    const result = serializeAttributes({
      payload: { items: Array.from({ length: 3000 }, (_, i) => ({ i })), note: 'n' },
    })
    const parsed = JSON.parse(result.json)

    expectUnderCap(result.json)
    expect(parsed.payload.note).toBe('n')
    expect(parsed.payload.items.length).toBeGreaterThan(1000)
  })

  it('keeps a top-level array an array, dropping items from the end', () => {
    const result = serializeAttributes(
      Array.from({ length: 3000 }, (_, i) => ({ id: i, name: `item-${i}` })),
    )
    const parsed = JSON.parse(result.json)

    expectUnderCap(result.json)
    expect(Array.isArray(parsed)).toBe(true)
    expect(parsed[0]).toEqual({ id: 0, name: 'item-0' })
  })

  it('measures the cap in UTF-8 bytes, not characters', () => {
    const result = serializeAttributes(
      Object.fromEntries(Array.from({ length: 20 }, (_, i) => [`k${i}`, 'é'.repeat(500)])),
    )

    expect(result.pruned).toBe(true)
    expectUnderCap(result.json)
  })

  it('gives the same output regardless of input key order', () => {
    const build = (order: string[]) =>
      Object.fromEntries(order.map((k) => [k, Array(3000).fill(1)]))

    const a = serializeAttributes(build(['b', 'a', 'c'])).json
    const reordered = serializeAttributes(build(['c', 'b', 'a'])).json

    expect(serializeAttributes(build(['b', 'a', 'c'])).json).toBe(a)
    expect(JSON.parse(reordered)).toEqual(JSON.parse(a))
    expect(JSON.parse(a).a.length).toBeLessThan(3000)
    expect(JSON.parse(a).b).toHaveLength(3000)
  })

  it('always returns valid JSON under the cap for randomly shaped oversized input', () => {
    let seed = 42
    const rand = () => {
      seed = (seed * 1103515245 + 12345) % 2147483648
      return seed / 2147483648
    }
    const gen = (depth: number): unknown => {
      const r = rand()
      if (depth > 2 || r < 0.3) return 'ü'.repeat(Math.floor(rand() * 800))
      if (r < 0.4) return r < 0.35 ? undefined : Math.floor(rand() * 1e9)
      if (r < 0.7) return Array.from({ length: Math.floor(rand() * 40) }, () => gen(depth + 1))
      return Object.fromEntries(
        Array.from({ length: Math.floor(rand() * 40) }, (_, i) => [`key-${i}`, gen(depth + 1)]),
      )
    }

    for (let i = 0; i < 30; i++) {
      const { json } = serializeAttributes({ root: gen(0), other: gen(1) })
      expectUnderCap(json)
      expect(() => JSON.parse(json)).not.toThrow()
    }
  })
})
