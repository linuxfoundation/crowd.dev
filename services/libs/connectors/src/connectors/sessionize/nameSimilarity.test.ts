import { describe, expect, it } from 'vitest'

import {
  MAX_COMPARED_NAME_LENGTH,
  isNameContained,
  nameSimilarity,
  similarityRatio,
} from './nameSimilarity'

const overLongName = `Example Con ${'a'.repeat(MAX_COMPARED_NAME_LENGTH)}`

describe('similarityRatio', () => {
  it.each([
    ['abcd', 'bcde', 0.75],
    ['kitten', 'sitting', 0.6153846153846154],
    ['open summit europe', 'open source summit europe', 0.8372093023255814],
    ['', 'abc', 0],
    ['', '', 1],
  ])('similarityRatio(%s, %s) = %f', (a, b, expected) =>
    expect(similarityRatio(a, b)).toBeCloseTo(expected, 10),
  )
})

describe('nameSimilarity', () => {
  it('expands synonyms and ignores years', () => {
    expect(nameSimilarity('Example Conf EU 2025', 'Example Conference Europe 2024')).toBe(1)
  })

  it('scores names that differ by an unknown abbreviation below one', () => {
    expect(nameSimilarity('Example Con EU 2025', 'Example Conference Europe 2024')).toBeCloseTo(
      0.8372093023255814,
      10,
    )
  })

  it('returns 0 when a name normalizes to empty', () => {
    expect(nameSimilarity('🎉', 'Example Con')).toBe(0)
  })

  it('returns 0 when a name is longer than the compared length bound', () => {
    expect(nameSimilarity(overLongName, overLongName)).toBe(0)
  })
})

describe('isNameContained', () => {
  it('accepts a name whose tokens are all in the other name', () => {
    expect(
      isNameContained('Example Community Day NA', 'Example Community Day North America 2026'),
    ).toBe(true)
  })

  it('rejects names with fewer than 3 tokens', () => {
    expect(isNameContained('Example Day', 'Example Day Europe')).toBe(false)
  })

  it('rejects names longer than the compared length bound', () => {
    expect(isNameContained(overLongName, overLongName)).toBe(false)
  })
})
