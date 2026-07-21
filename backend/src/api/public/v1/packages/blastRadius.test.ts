import { describe, expect, it } from 'vitest'

import {
  blastRadiusJobRequestSchema,
  isSupportedBlastRadiusEcosystem,
  toBlastRadiusJobEntry,
} from './blastRadius'

describe('blastRadiusJobRequestSchema', () => {
  it('accepts a minimal advisory-wide request and defaults force to false', () => {
    const result = blastRadiusJobRequestSchema.parse({ advisoryId: 'GHSA-jf85-cpcp-j695' })
    expect(result).toEqual({
      advisoryId: 'GHSA-jf85-cpcp-j695',
      ecosystem: undefined,
      package: undefined,
      force: false,
    })
  })

  it('accepts a request scoped to a package with ecosystem and force set', () => {
    const result = blastRadiusJobRequestSchema.parse({
      advisoryId: 'GHSA-jf85-cpcp-j695',
      ecosystem: 'npm',
      package: 'pkg:npm/lodash',
      force: true,
    })
    expect(result).toEqual({
      advisoryId: 'GHSA-jf85-cpcp-j695',
      ecosystem: 'npm',
      package: 'pkg:npm/lodash',
      force: true,
    })
  })

  it('accepts explicit null for package/ecosystem (advisory-wide, explicit)', () => {
    const result = blastRadiusJobRequestSchema.parse({
      advisoryId: 'GHSA-jf85-cpcp-j695',
      ecosystem: null,
      package: null,
    })
    expect(result.ecosystem).toBeNull()
    expect(result.package).toBeNull()
  })

  it('rejects a missing advisoryId', () => {
    const result = blastRadiusJobRequestSchema.safeParse({})
    expect(result.success).toBe(false)
  })

  it('rejects an empty advisoryId', () => {
    const result = blastRadiusJobRequestSchema.safeParse({ advisoryId: '   ' })
    expect(result.success).toBe(false)
  })
})

describe('isSupportedBlastRadiusEcosystem', () => {
  it('accepts npm', () => {
    expect(isSupportedBlastRadiusEcosystem('npm')).toBe(true)
  })

  it('rejects any other ecosystem', () => {
    expect(isSupportedBlastRadiusEcosystem('pypi')).toBe(false)
  })

  it('rejects null and undefined', () => {
    expect(isSupportedBlastRadiusEcosystem(null)).toBe(false)
    expect(isSupportedBlastRadiusEcosystem(undefined)).toBe(false)
  })
})

describe('toBlastRadiusJobEntry', () => {
  it('builds a pending job entry echoing the request fields', () => {
    const entry = toBlastRadiusJobEntry({
      analysisId: 'br_01h',
      advisoryId: 'GHSA-jf85-cpcp-j695',
      package: 'pkg:npm/lodash',
      ecosystem: 'npm',
    })
    expect(entry).toEqual({
      analysisId: 'br_01h',
      advisoryId: 'GHSA-jf85-cpcp-j695',
      package: 'pkg:npm/lodash',
      ecosystem: 'npm',
      status: 'pending',
    })
  })

  it('echoes null package/ecosystem for an advisory-wide job', () => {
    const entry = toBlastRadiusJobEntry({
      analysisId: 'br_01h',
      advisoryId: 'GHSA-jf85-cpcp-j695',
      package: null,
      ecosystem: null,
    })
    expect(entry.package).toBeNull()
    expect(entry.ecosystem).toBeNull()
    expect(entry.status).toBe('pending')
  })
})
