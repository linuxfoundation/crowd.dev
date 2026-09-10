import { describe, expect, it } from 'vitest'

import { selectAdvisoryEntry } from '../selectAdvisoryEntry'

interface FakeEntry {
  package: { name: string }
}

function entry(name: string): FakeEntry {
  return { package: { name } }
}

describe('selectAdvisoryEntry', () => {
  it('returns the single entry when no package was requested', () => {
    const entries = [entry('pkg-a')]
    const result = selectAdvisoryEntry(entries, null, (e) => e.package.name === 'pkg-a', 'GHSA-1')
    expect(result.entry).toBe(entries[0])
    expect(result.relatedAffectedPackages).toEqual([])
  })

  it('returns the matching entry when the requested package is in the advisory', () => {
    const entries = [entry('pkg-a'), entry('pkg-b')]
    const result = selectAdvisoryEntry(
      entries,
      'pkg-b',
      (e) => e.package.name === 'pkg-b',
      'GHSA-1',
    )
    expect(result.entry).toBe(entries[1])
    expect(result.relatedAffectedPackages).toEqual(['pkg-a'])
  })

  it('rejects a requested package that is not in the advisory instead of falling back', () => {
    const entries = [entry('pkg-a'), entry('pkg-b')]
    expect(() =>
      selectAdvisoryEntry(entries, 'pkg-c', (e) => e.package.name === 'pkg-c', 'GHSA-1'),
    ).toThrow(/pkg-c.*not found in advisory GHSA-1.*pkg-a, pkg-b/)
  })

  it('rejects an omitted package against a multi-artifact advisory instead of picking the first entry', () => {
    const entries = [entry('pkg-a'), entry('pkg-b')]
    expect(() => selectAdvisoryEntry(entries, null, () => false, 'GHSA-1')).toThrow(
      /GHSA-1 affects 2 packages \(pkg-a, pkg-b\)/,
    )
  })

  it('treats an empty-string request as explicit, not as "no request"', () => {
    const entries = [entry('pkg-a'), entry('pkg-b')]
    expect(() => selectAdvisoryEntry(entries, '', () => false, 'GHSA-1')).toThrow(
      /Requested package {2}not found in advisory GHSA-1.*pkg-a, pkg-b/,
    )
  })
})
