import { describe, expect, it } from 'vitest'

import { toBareGemName, toBareNpmName, toBareNuGetId, toDbCargoName } from '../packageIdentifier'

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

describe('toBareNuGetId', () => {
  it('returns a bare id unchanged, preserving casing', () => {
    expect(toBareNuGetId('Newtonsoft.Json')).toBe('Newtonsoft.Json')
  })

  it('strips the pkg:nuget/ prefix', () => {
    expect(toBareNuGetId('pkg:nuget/Newtonsoft.Json')).toBe('Newtonsoft.Json')
  })

  it('strips a trailing version', () => {
    expect(toBareNuGetId('pkg:nuget/Newtonsoft.Json@13.0.1')).toBe('Newtonsoft.Json')
  })

  it('strips qualifiers and subpath', () => {
    expect(toBareNuGetId('pkg:nuget/Newtonsoft.Json@13.0.1?foo=bar#sub')).toBe('Newtonsoft.Json')
  })

  it('does not lowercase the id — DB lookups are case-sensitive', () => {
    expect(toBareNuGetId('pkg:nuget/Microsoft.AspNetCore.Mvc@2.2.0')).toBe(
      'Microsoft.AspNetCore.Mvc',
    )
  })
})

describe('toBareGemName', () => {
  it('returns a bare name unchanged', () => {
    expect(toBareGemName('rack')).toBe('rack')
  })

  it('strips the pkg:gem/ prefix', () => {
    expect(toBareGemName('pkg:gem/rack')).toBe('rack')
  })

  it('strips a trailing version', () => {
    expect(toBareGemName('pkg:gem/rack@3.0.8')).toBe('rack')
  })

  it('strips qualifiers and subpath', () => {
    expect(toBareGemName('pkg:gem/rack@3.0.8?foo=bar#sub')).toBe('rack')
  })

  it('does not lowercase the name — preserves non-lowercase published spellings', () => {
    expect(toBareGemName('pkg:gem/RedCloth@4.3.2')).toBe('RedCloth')
  })
})
