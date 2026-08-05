import { describe, expect, it } from 'vitest'

import { toBareNpmName, toDbCargoName } from '../packageIdentifier'

describe('toBareNpmName', () => {
  it('returns a bare name unchanged', () => {
    expect(toBareNpmName('lodash')).toBe('lodash')
  })

  it('returns a scoped bare name unchanged', () => {
    expect(toBareNpmName('@babel/core')).toBe('@babel/core')
  })

  it('strips the pkg:npm/ prefix', () => {
    expect(toBareNpmName('pkg:npm/lodash')).toBe('lodash')
  })

  it('decodes an encoded scope separator', () => {
    expect(toBareNpmName('pkg:npm/%40babel/core')).toBe('@babel/core')
  })

  it('strips a trailing version', () => {
    expect(toBareNpmName('pkg:npm/lodash@4.17.21')).toBe('lodash')
  })

  it('strips a trailing version from a scoped purl', () => {
    expect(toBareNpmName('pkg:npm/%40babel/core@7.24.0')).toBe('@babel/core')
  })

  it('strips qualifiers and subpath', () => {
    expect(toBareNpmName('pkg:npm/lodash@4.17.21?foo=bar#sub')).toBe('lodash')
  })
})

describe('toDbCargoName', () => {
  it('leaves an already-underscored name unchanged', () => {
    expect(toDbCargoName('serde_json')).toBe('serde_json')
  })

  it('converts hyphens to underscores, matching packages.name for hyphenated crates', () => {
    expect(toDbCargoName('serde-json')).toBe('serde_json')
  })

  it('lowercases mixed-case names', () => {
    expect(toDbCargoName('Actix-Web')).toBe('actix_web')
  })
})
