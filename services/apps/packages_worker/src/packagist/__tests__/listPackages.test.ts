import { afterEach, describe, expect, it, vi } from 'vitest'

import { fetchPackagistPackageList, parsePackagistPackageList } from '../listPackages'

afterEach(() => {
  vi.unstubAllGlobals()
})

// A1 — list.json parsing: vendor/name split, purl form, invalid skipped+counted, dedup.
describe('parsePackagistPackageList', () => {
  it('maps names to vendor/name entries with pkg:composer purls', () => {
    const { entries, invalid } = parsePackagistPackageList({
      packageNames: ['monolog/monolog', 'symfony/http-kernel'],
    })
    expect(entries).toEqual([
      { vendor: 'monolog', name: 'monolog', purl: 'pkg:composer/monolog/monolog' },
      { vendor: 'symfony', name: 'http-kernel', purl: 'pkg:composer/symfony/http-kernel' },
    ])
    expect(invalid).toBe(0)
  })

  it('accepts dots, underscores and hyphens in segments', () => {
    const { entries } = parsePackagistPackageList({
      packageNames: ['foo/bar.baz', 'a1/b_c', 'x-y/z-1'],
    })
    expect(entries.map((e) => e.purl)).toEqual([
      'pkg:composer/foo/bar.baz',
      'pkg:composer/a1/b_c',
      'pkg:composer/x-y/z-1',
    ])
  })

  it('skips and counts invalid names', () => {
    const { entries, invalid } = parsePackagistPackageList({
      packageNames: [
        'valid/name',
        'noslash',
        'too/many/parts',
        '/leading',
        'trailing/',
        'has space/pkg',
        '',
        42,
      ],
    })
    expect(entries.map((e) => e.purl)).toEqual(['pkg:composer/valid/name'])
    expect(invalid).toBe(7)
  })

  it('lowercases and deduplicates names without counting duplicates as invalid', () => {
    const { entries, invalid } = parsePackagistPackageList({
      packageNames: ['Monolog/Monolog', 'monolog/monolog'],
    })
    expect(entries).toEqual([
      { vendor: 'monolog', name: 'monolog', purl: 'pkg:composer/monolog/monolog' },
    ])
    expect(invalid).toBe(0)
  })

  it('throws on a root shape without a packageNames array', () => {
    expect(() => parsePackagistPackageList({ nope: true })).toThrow()
    expect(() => parsePackagistPackageList({ packageNames: 'not-an-array' })).toThrow()
    expect(() => parsePackagistPackageList(null)).toThrow()
  })
})

describe('fetchPackagistPackageList', () => {
  it('returns the raw JSON on 200 and hits the list endpoint', async () => {
    const body = { packageNames: ['monolog/monolog'] }
    const fetchMock = vi.fn().mockResolvedValue({
      status: 200,
      ok: true,
      json: async () => body,
    } as unknown as Response)
    vi.stubGlobal('fetch', fetchMock)

    expect(await fetchPackagistPackageList()).toEqual(body)
    expect(fetchMock.mock.calls[0][0]).toBe('https://packagist.org/packages/list.json')
  })

  it('maps a 5xx to TRANSIENT', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({ status: 503, ok: false, json: async () => ({}) }),
    )
    expect(await fetchPackagistPackageList()).toMatchObject({ kind: 'TRANSIENT', statusCode: 503 })
  })
})
