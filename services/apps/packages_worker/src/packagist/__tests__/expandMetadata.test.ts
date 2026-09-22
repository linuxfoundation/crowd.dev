import { describe, expect, it } from 'vitest'

import { expandComposerMetadata } from '../expandMetadata'

// C1 — Composer\MetadataMinifier::expand semantics: the first version object is complete,
// each later object carries only changed keys, '__unset' removes a key.
describe('expandComposerMetadata', () => {
  it('expands diffs against the previous version and honors __unset', () => {
    const expanded = expandComposerMetadata([
      {
        version: '3.0.0',
        version_normalized: '3.0.0.0',
        time: '2026-01-01T00:00:00+00:00',
        license: ['MIT'],
        require: { php: '>=8.1', 'psr/log': '^3.0' },
        homepage: 'https://example.org',
      },
      {
        version: '2.0.0',
        version_normalized: '2.0.0.0',
        time: '2024-01-01T00:00:00+00:00',
        require: { php: '>=7.4', 'psr/log': '^2.0' },
        homepage: '__unset',
      },
      {
        version: '1.0.0',
        version_normalized: '1.0.0.0',
        time: '2020-01-01T00:00:00+00:00',
        license: ['BSD-3-Clause'],
      },
    ])

    expect(expanded).toEqual([
      {
        version: '3.0.0',
        version_normalized: '3.0.0.0',
        time: '2026-01-01T00:00:00+00:00',
        license: ['MIT'],
        require: { php: '>=8.1', 'psr/log': '^3.0' },
        homepage: 'https://example.org',
      },
      {
        // license carried over from 3.0.0, homepage removed via __unset
        version: '2.0.0',
        version_normalized: '2.0.0.0',
        time: '2024-01-01T00:00:00+00:00',
        license: ['MIT'],
        require: { php: '>=7.4', 'psr/log': '^2.0' },
      },
      {
        // require carried over from 2.0.0, license overridden
        version: '1.0.0',
        version_normalized: '1.0.0.0',
        time: '2020-01-01T00:00:00+00:00',
        license: ['BSD-3-Clause'],
        require: { php: '>=7.4', 'psr/log': '^2.0' },
      },
    ])
  })

  it('returns independent objects (mutating one expanded version does not leak into others)', () => {
    const expanded = expandComposerMetadata([
      { version: '2.0.0', require: { php: '>=8.0' } },
      { version: '1.0.0' },
    ])
    ;(expanded[0] as Record<string, unknown>).extra = 'mutated'
    expect('extra' in expanded[1]).toBe(false)
  })

  it('returns [] for an empty version list', () => {
    expect(expandComposerMetadata([])).toEqual([])
  })
})
