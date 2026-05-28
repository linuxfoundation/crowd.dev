import { describe, expect, it } from 'vitest'

import { parseOsvRecord } from '../parseOsvRecord'
import { OsvRecord } from '../types'

const ALLOW = new Set(['npm', 'maven'])

const baseRecord = (overrides: Partial<OsvRecord>): OsvRecord => ({
  id: 'GHSA-test',
  ...overrides,
})

describe('parseOsvRecord — name splitting', () => {
  it('splits Maven groupId:artifactId', () => {
    const r = baseRecord({
      affected: [
        {
          package: { ecosystem: 'Maven', name: 'org.apache.logging.log4j:log4j-core' },
          ranges: [{ type: 'ECOSYSTEM', events: [{ introduced: '2.0' }, { fixed: '2.15.0' }] }],
        },
      ],
    })
    const out = parseOsvRecord(r, ALLOW)
    expect(out.packages).toHaveLength(1)
    expect(out.packages[0].pkg).toMatchObject({
      ecosystem: 'maven',
      packageName: 'org.apache.logging.log4j:log4j-core',
      namespace: 'org.apache.logging.log4j',
      name: 'log4j-core',
    })
  })

  it('strips the leading @ from npm scoped packages', () => {
    const r = baseRecord({
      affected: [
        {
          package: { ecosystem: 'npm', name: '@types/node' },
          ranges: [{ type: 'SEMVER', events: [{ introduced: '0' }, { fixed: '1.0.0' }] }],
        },
      ],
    })
    const out = parseOsvRecord(r, ALLOW)
    expect(out.packages[0].pkg).toMatchObject({
      packageName: '@types/node',
      namespace: 'types',
      name: 'node',
    })
  })

  it('leaves bare npm packages with namespace=null', () => {
    const r = baseRecord({
      affected: [
        {
          package: { ecosystem: 'npm', name: 'lodash' },
          ranges: [{ type: 'SEMVER', events: [{ introduced: '0' }, { fixed: '4.17.21' }] }],
        },
      ],
    })
    const out = parseOsvRecord(r, ALLOW)
    expect(out.packages[0].pkg).toMatchObject({ namespace: null, name: 'lodash' })
  })

  it('handles a Maven name without colon by falling back to namespace=null', () => {
    // Real-world Maven names always have the colon, but the parser shouldn't crash
    // on malformed input.
    const r = baseRecord({
      affected: [
        {
          package: { ecosystem: 'Maven', name: 'log4j-core' },
          ranges: [{ type: 'ECOSYSTEM', events: [{ introduced: '0' }, { fixed: '2.15.0' }] }],
        },
      ],
    })
    const out = parseOsvRecord(r, ALLOW)
    expect(out.packages[0].pkg).toMatchObject({ namespace: null, name: 'log4j-core' })
  })
})

describe('parseOsvRecord — ecosystem allowlist', () => {
  it('drops affected[] entries for ecosystems outside the allowlist', () => {
    // Cross-ecosystem advisory (e.g. lodash GHSA hits npm + RubyGems): only npm survives.
    const r = baseRecord({
      affected: [
        {
          package: { ecosystem: 'npm', name: 'lodash' },
          ranges: [{ type: 'SEMVER', events: [{ introduced: '0' }, { fixed: '4.17.21' }] }],
        },
        {
          package: { ecosystem: 'RubyGems', name: 'lodash' },
          ranges: [{ type: 'ECOSYSTEM', events: [{ introduced: '0' }, { fixed: '4.17.21' }] }],
        },
      ],
    })
    const out = parseOsvRecord(r, ALLOW)
    expect(out.packages).toHaveLength(1)
    expect(out.packages[0].pkg.ecosystem).toBe('npm')
  })

  it('returns packages: [] when no affected entry survives the filter', () => {
    const r = baseRecord({
      affected: [
        {
          package: { ecosystem: 'PyPI', name: 'requests' },
          ranges: [{ type: 'ECOSYSTEM', events: [{ introduced: '0' }, { fixed: '2.31.0' }] }],
        },
      ],
    })
    const out = parseOsvRecord(r, ALLOW)
    expect(out.packages).toEqual([])
    // Advisory metadata is still populated so callers can choose to log/skip.
    expect(out.advisory.osvId).toBe('GHSA-test')
  })
})

describe('parseOsvRecord — range flattening', () => {
  it('flattens introduced → fixed pairs', () => {
    const r = baseRecord({
      affected: [
        {
          package: { ecosystem: 'npm', name: 'lodash' },
          ranges: [
            {
              type: 'SEMVER',
              events: [
                { introduced: '1.0.0' },
                { fixed: '1.2.0' },
                { introduced: '2.0.0' },
                { fixed: '2.5.0' },
              ],
            },
          ],
        },
      ],
    })
    const out = parseOsvRecord(r, ALLOW)
    expect(out.packages[0].ranges).toEqual([
      { introducedVersion: '1.0.0', fixedVersion: '1.2.0', lastAffected: null },
      { introducedVersion: '2.0.0', fixedVersion: '2.5.0', lastAffected: null },
    ])
  })

  it('flattens introduced → last_affected', () => {
    const r = baseRecord({
      affected: [
        {
          package: { ecosystem: 'npm', name: 'pkg' },
          ranges: [
            {
              type: 'SEMVER',
              events: [{ introduced: '1.0.0' }, { last_affected: '1.4.9' }],
            },
          ],
        },
      ],
    })
    const out = parseOsvRecord(r, ALLOW)
    expect(out.packages[0].ranges).toEqual([
      { introducedVersion: '1.0.0', fixedVersion: null, lastAffected: '1.4.9' },
    ])
  })

  it('handles MAL- style ranges (introduced=0, no fixed) as always-vulnerable rows', () => {
    const r = baseRecord({
      id: 'MAL-2024-1',
      affected: [
        {
          package: { ecosystem: 'npm', name: 'evil-pkg' },
          ranges: [{ type: 'SEMVER', events: [{ introduced: '0' }] }],
        },
      ],
    })
    const out = parseOsvRecord(r, ALLOW)
    expect(out.packages[0].ranges).toEqual([
      { introducedVersion: '0', fixedVersion: null, lastAffected: null },
    ])
  })

  it('skips GIT ranges entirely', () => {
    // Commit-hash ranges aren't useful for version-based matching.
    const r = baseRecord({
      affected: [
        {
          package: { ecosystem: 'npm', name: 'pkg' },
          ranges: [
            { type: 'GIT', events: [{ introduced: 'abc123' }, { fixed: 'def456' }] },
            { type: 'SEMVER', events: [{ introduced: '1.0.0' }, { fixed: '1.1.0' }] },
          ],
        },
      ],
    })
    const out = parseOsvRecord(r, ALLOW)
    expect(out.packages[0].ranges).toEqual([
      { introducedVersion: '1.0.0', fixedVersion: '1.1.0', lastAffected: null },
    ])
  })

  it('skips an affected[] entry with neither ranges nor versions', () => {
    const r = baseRecord({
      affected: [{ package: { ecosystem: 'npm', name: 'pkg' } }],
    })
    const out = parseOsvRecord(r, ALLOW)
    expect(out.packages).toEqual([])
  })

  it('merges multiple affected[] entries for the same package', () => {
    // Some OSV records list the same (ecosystem, name) multiple times to express
    // disjoint range sets. We collapse them under one advisory_package row.
    const r = baseRecord({
      affected: [
        {
          package: { ecosystem: 'npm', name: 'pkg' },
          ranges: [{ type: 'SEMVER', events: [{ introduced: '1.0.0' }, { fixed: '1.2.0' }] }],
        },
        {
          package: { ecosystem: 'npm', name: 'pkg' },
          ranges: [{ type: 'SEMVER', events: [{ introduced: '2.0.0' }, { fixed: '2.5.0' }] }],
        },
      ],
    })
    const out = parseOsvRecord(r, ALLOW)
    expect(out.packages).toHaveLength(1)
    expect(out.packages[0].ranges).toEqual([
      { introducedVersion: '1.0.0', fixedVersion: '1.2.0', lastAffected: null },
      { introducedVersion: '2.0.0', fixedVersion: '2.5.0', lastAffected: null },
    ])
  })
})

describe('parseOsvRecord — advisory metadata', () => {
  it('carries through aliases, summary, details, dates', () => {
    const r = baseRecord({
      id: 'GHSA-abc-def-ghi',
      aliases: ['CVE-2021-99999', 'CVE-2022-00001'],
      summary: 'short summary',
      details: 'long details',
      published: '2021-01-01T00:00:00Z',
      modified: '2021-06-01T00:00:00Z',
      affected: [
        {
          package: { ecosystem: 'npm', name: 'pkg' },
          ranges: [{ type: 'SEMVER', events: [{ introduced: '0' }, { fixed: '1.0.0' }] }],
        },
      ],
    })
    const out = parseOsvRecord(r, ALLOW)
    expect(out.advisory).toMatchObject({
      osvId: 'GHSA-abc-def-ghi',
      aliases: ['CVE-2021-99999', 'CVE-2022-00001'],
      summary: 'short summary',
      details: 'long details',
      publishedAt: '2021-01-01T00:00:00Z',
      modifiedAt: '2021-06-01T00:00:00Z',
    })
  })
})
