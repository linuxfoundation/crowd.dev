import { describe, expect, it } from 'vitest'

import { blastRadiusJobRequestSchema, toBlastRadiusJobEntry } from './blastRadius'

describe('blastRadiusJobRequestSchema', () => {
  it('accepts a minimal advisory-wide request and defaults force to false', () => {
    const result = blastRadiusJobRequestSchema.parse({
      advisoryId: 'GHSA-jf85-cpcp-j695',
      ecosystem: 'npm',
    })
    expect(result).toEqual({
      advisoryId: 'GHSA-jf85-cpcp-j695',
      ecosystem: 'npm',
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

  it('accepts explicit null for package (advisory-wide, explicit)', () => {
    const result = blastRadiusJobRequestSchema.parse({
      advisoryId: 'GHSA-jf85-cpcp-j695',
      ecosystem: 'npm',
      package: null,
    })
    expect(result.package).toBeNull()
  })

  it('rejects a missing advisoryId', () => {
    const result = blastRadiusJobRequestSchema.safeParse({ ecosystem: 'npm' })
    expect(result.success).toBe(false)
  })

  it('rejects an empty advisoryId', () => {
    const result = blastRadiusJobRequestSchema.safeParse({
      advisoryId: '   ',
      ecosystem: 'npm',
    })
    expect(result.success).toBe(false)
  })

  it('rejects an advisoryId that is not a GHSA or CVE identifier', () => {
    const result = blastRadiusJobRequestSchema.safeParse({
      advisoryId: 'foo',
      ecosystem: 'npm',
    })
    expect(result.success).toBe(false)
  })

  it('accepts a CVE-formatted advisoryId', () => {
    const result = blastRadiusJobRequestSchema.safeParse({
      advisoryId: 'CVE-2024-12345',
      ecosystem: 'npm',
    })
    expect(result.success).toBe(true)
  })

  it('rejects a missing ecosystem', () => {
    const result = blastRadiusJobRequestSchema.safeParse({ advisoryId: 'GHSA-jf85-cpcp-j695' })
    expect(result.success).toBe(false)
  })

  it('rejects null ecosystem', () => {
    const result = blastRadiusJobRequestSchema.safeParse({
      advisoryId: 'GHSA-jf85-cpcp-j695',
      ecosystem: null,
    })
    expect(result.success).toBe(false)
  })

  it('rejects an unsupported ecosystem', () => {
    const result = blastRadiusJobRequestSchema.safeParse({
      advisoryId: 'GHSA-jf85-cpcp-j695',
      ecosystem: 'pypi',
    })
    expect(result.success).toBe(false)
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

  it('echoes a null package for an advisory-wide job', () => {
    const entry = toBlastRadiusJobEntry({
      analysisId: 'br_01h',
      advisoryId: 'GHSA-jf85-cpcp-j695',
      package: null,
      ecosystem: 'npm',
    })
    expect(entry.package).toBeNull()
    expect(entry.ecosystem).toBe('npm')
    expect(entry.status).toBe('pending')
  })
})
