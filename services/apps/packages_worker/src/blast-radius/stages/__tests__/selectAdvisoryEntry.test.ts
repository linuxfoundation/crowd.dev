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
    expect(selectAdvisoryEntry(entries, null, (e) => e.package.name === 'pkg-a', 'GHSA-1')).toBe(
      entries[0],
    )
  })

  it('returns the matching entry when the requested package is in the advisory', () => {
    const entries = [entry('pkg-a'), entry('pkg-b')]
    const match = selectAdvisoryEntry(entries, 'pkg-b', (e) => e.package.name === 'pkg-b', 'GHSA-1')
    expect(match).toBe(entries[1])
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
})
