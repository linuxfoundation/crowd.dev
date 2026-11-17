import { describe, expect, test } from 'vitest'

import { buildCdpSubprojectIndex, findExistingProjectCandidates } from './existingProjects'
import type { CdpSubproject } from './existingProjects'

function subproject(overrides: Partial<CdpSubproject> & { id: string }): CdpSubproject {
  return { slug: overrides.id, name: overrides.id, sourceId: null, isLF: true, ...overrides }
}

describe('findExistingProjectCandidates', () => {
  test('finds a CDP project with the same slug and name', () => {
    const chapel = subproject({ id: 'seg-1', slug: 'chapel', name: 'Chapel' })
    const index = buildCdpSubprojectIndex([chapel])

    expect(findExistingProjectCandidates(index, 'chapel', 'Chapel')).toEqual([
      { ...chapel, matchedOn: ['slug', 'name'] },
    ])
  })

  test('finds a non-LF project by the slug behind its prefix', () => {
    const git = subproject({ id: 'seg-1', slug: 'nonlf_git', name: 'git', isLF: false })
    const index = buildCdpSubprojectIndex([git])

    expect(findExistingProjectCandidates(index, 'git', 'Git SCM')).toEqual([
      { ...git, matchedOn: ['slug'] },
    ])
  })

  test('matches names regardless of case and surrounding spaces', () => {
    const wireshark = subproject({ id: 'seg-1', slug: 'nonlf_wireshark-app', name: ' Wireshark ' })
    const index = buildCdpSubprojectIndex([wireshark])

    expect(findExistingProjectCandidates(index, 'wireshark', 'WireShark')).toEqual([
      { ...wireshark, matchedOn: ['name'] },
    ])
  })

  test('reports a slug taken by a project linked to another PCC ID', () => {
    const tink = subproject({ id: 'seg-1', slug: 'tink', name: 'Tink', sourceId: 'other-pcc-id' })
    const index = buildCdpSubprojectIndex([tink])

    const [candidate] = findExistingProjectCandidates(index, 'tink', 'Tink')

    expect(candidate.sourceId).toBe('other-pcc-id')
  })

  test('returns every project that matches', () => {
    const lf = subproject({ id: 'seg-1', slug: 'ini', name: 'Inclusive Naming' })
    const nonLf = subproject({ id: 'seg-2', slug: 'nonlf_ini', name: 'ini', isLF: false })
    const index = buildCdpSubprojectIndex([lf, nonLf])

    const candidates = findExistingProjectCandidates(index, 'ini', 'INI Fund')

    expect(candidates.map((candidate) => candidate.id)).toEqual(['seg-1', 'seg-2'])
  })

  test('does not match a similar name', () => {
    const index = buildCdpSubprojectIndex([
      subproject({ id: 'seg-1', slug: 'kokkos-project', name: 'Kokkos Project' }),
    ])

    expect(findExistingProjectCandidates(index, 'kokkos-fund', 'Kokkos Fund')).toEqual([])
  })

  test('indexes a CDP project without a name by its slug only', () => {
    const unnamed = subproject({ id: 'seg-1', slug: 'hpx', name: null })
    const index = buildCdpSubprojectIndex([unnamed])

    expect(findExistingProjectCandidates(index, 'hpx', 'HPX')).toEqual([
      { ...unnamed, matchedOn: ['slug'] },
    ])
  })

  test('matches by name when the PCC project has no slug', () => {
    const hpx = subproject({ id: 'seg-1', slug: 'hpx', name: 'HPX' })
    const index = buildCdpSubprojectIndex([hpx])

    expect(findExistingProjectCandidates(index, null, 'HPX')).toEqual([
      { ...hpx, matchedOn: ['name'] },
    ])
  })
})
