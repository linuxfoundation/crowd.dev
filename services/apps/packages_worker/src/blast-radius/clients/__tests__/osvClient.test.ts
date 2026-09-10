import { describe, expect, it } from 'vitest'

import { type OsvVuln, affectedEntriesForEcosystem, semverRangeEvents } from '../osvClient'

function vuln(affected: OsvVuln['affected']): OsvVuln {
  return { id: 'GHSA-1', aliases: [], references: [], affected }
}

describe('affectedEntriesForEcosystem', () => {
  it('merges duplicate same-package entries into one, aggregating all ranges', () => {
    const osv = vuln([
      {
        package: { ecosystem: 'npm', name: 'pkg-a' },
        ranges: [{ type: 'SEMVER', events: [{ introduced: '1.0.0' }, { fixed: '1.2.0' }] }],
      },
      {
        package: { ecosystem: 'npm', name: 'pkg-a' },
        ranges: [{ type: 'SEMVER', events: [{ introduced: '2.0.0' }, { fixed: '2.5.0' }] }],
      },
    ])

    const entries = affectedEntriesForEcosystem(osv, 'npm')

    expect(entries).toHaveLength(1)
    expect(semverRangeEvents(entries[0])).toEqual([
      { introduced: '1.0.0', fixed: '1.2.0', lastAffected: null },
      { introduced: '2.0.0', fixed: '2.5.0', lastAffected: null },
    ])
  })

  it('keeps distinct packages separate', () => {
    const osv = vuln([
      { package: { ecosystem: 'npm', name: 'pkg-a' } },
      { package: { ecosystem: 'npm', name: 'pkg-b' } },
    ])

    const entries = affectedEntriesForEcosystem(osv, 'npm')

    expect(entries.map((e) => e.package.name)).toEqual(['pkg-a', 'pkg-b'])
  })

  it('dedups overlapping exact versions across duplicate entries', () => {
    const osv = vuln([
      { package: { ecosystem: 'npm', name: 'pkg-a' }, versions: ['1.0.0', '1.0.1'] },
      { package: { ecosystem: 'npm', name: 'pkg-a' }, versions: ['1.0.1', '1.0.2'] },
    ])

    const entries = affectedEntriesForEcosystem(osv, 'npm')

    expect(entries).toHaveLength(1)
    expect(entries[0].versions).toEqual(['1.0.0', '1.0.1', '1.0.2'])
  })

  it('drops exact-duplicate range tuples instead of storing them twice', () => {
    const range = { type: 'SEMVER', events: [{ introduced: '1.0.0' }, { fixed: '1.2.0' }] }
    const osv = vuln([
      { package: { ecosystem: 'npm', name: 'pkg-a' }, ranges: [range] },
      { package: { ecosystem: 'npm', name: 'pkg-a' }, ranges: [range] },
    ])

    const entries = affectedEntriesForEcosystem(osv, 'npm')

    expect(entries).toHaveLength(1)
    expect(entries[0].ranges).toEqual([range])
  })

  it('keeps same-named packages from different ecosystems separate', () => {
    const osv = vuln([
      { package: { ecosystem: 'npm', name: 'path' } },
      { package: { ecosystem: 'Go', name: 'path' } },
    ])

    expect(affectedEntriesForEcosystem(osv, 'npm')).toHaveLength(1)
    expect(affectedEntriesForEcosystem(osv, 'Go')).toHaveLength(1)
  })
})
