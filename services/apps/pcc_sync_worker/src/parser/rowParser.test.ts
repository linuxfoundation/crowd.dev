import { describe, expect, it } from 'vitest'

import { SegmentStatus } from '@crowd/types'

import { parsePccRow } from './rowParser'

interface Ancestor {
  name: string
  id?: string
  model?: string | null
}

const LINUX_FOUNDATION: Ancestor = {
  name: 'The Linux Foundation',
  id: 'a0941000002wBz9AAE',
  model: 'Membership',
}

function spineRowsWithModelsFromTop(path: Ancestor[]): Record<string, unknown>[] {
  const leafName = path[path.length - 1].name
  return path.map((ancestor, index) => ({
    PROJECT_ID: 'pcc-leaf',
    NAME: leafName,
    PROJECT_STATUS: 'Active',
    MAPPED_PROJECT_ID: ancestor.id ?? `pcc-${index}`,
    MAPPED_PROJECT_NAME: ancestor.name,
    MAPPED_PROJECT_SLUG: ancestor.name.toLowerCase().replaceAll(' ', '-'),
    MAPPED_PROJECT_MODEL: ancestor.model,
    HIERARCHY_LEVEL: path.length - index,
  }))
}

function spineRowsFromTop(path: string[]): Record<string, unknown>[] {
  return spineRowsWithModelsFromTop(path.map((name) => ({ name })))
}

describe('parsePccRow', () => {
  it('maps a standalone project to all three CDP levels', () => {
    const result = parsePccRow(spineRowsFromTop(['Zephyr Project']))

    expect(result).toMatchObject({
      ok: true,
      project: {
        cdpTarget: {
          group: 'Zephyr Project',
          project: 'Zephyr Project',
          subproject: 'Zephyr Project',
        },
      },
    })
  })

  it('keeps the foundation as the group of a project directly under it', () => {
    const result = parsePccRow(spineRowsFromTop(['LF AI & Data', 'AIRSEAI']))

    expect(result).toMatchObject({
      ok: true,
      project: {
        cdpTarget: { group: 'LF AI & Data', project: 'AIRSEAI', subproject: 'AIRSEAI' },
      },
    })
  })

  it('puts a Community Project under The Linux Foundation group', () => {
    const result = parsePccRow(spineRowsFromTop(['The Linux Foundation', 'Open Quantum Safe']))

    expect(result).toMatchObject({
      ok: true,
      project: {
        cdpTarget: {
          group: 'The Linux Foundation',
          project: 'Open Quantum Safe',
          subproject: 'Open Quantum Safe',
        },
      },
    })
  })

  it('maps three levels to group, project and subproject', () => {
    const result = parsePccRow(spineRowsFromTop(['LF Networking (LFN)', 'Anuket', 'RC1']))

    expect(result).toMatchObject({
      ok: true,
      project: {
        cdpTarget: { group: 'LF Networking (LFN)', project: 'Anuket', subproject: 'RC1' },
      },
    })
  })

  it('keeps the top two levels and the leaf when the tree is deeper than three levels', () => {
    const result = parsePccRow(
      spineRowsFromTop(['Foundation', 'Umbrella', 'Working Group', 'Task Force', 'Plugin']),
    )

    expect(result).toMatchObject({
      ok: true,
      project: {
        cdpTarget: { group: 'Foundation', project: 'Umbrella', subproject: 'Plugin' },
      },
    })
  })

  it('reads the hierarchy from the level, not from the order of the rows', () => {
    const rows = spineRowsFromTop(['LF Networking (LFN)', 'Anuket', 'RC1'])

    const result = parsePccRow([rows[2], rows[0], rows[1]])

    expect(result).toMatchObject({
      ok: true,
      project: {
        pccSlug: 'rc1',
        cdpTarget: { group: 'LF Networking (LFN)', project: 'Anuket', subproject: 'RC1' },
      },
    })
  })

  it('reads a hierarchy level that Parquet serialized as a big-endian buffer', () => {
    const [top, leaf] = spineRowsFromTop(['OpenJS Foundation', 'PEP'])

    const result = parsePccRow([
      { ...top, HIERARCHY_LEVEL: Buffer.from([0, 2]) },
      { ...leaf, HIERARCHY_LEVEL: Buffer.from([0, 1]) },
    ])

    expect(result).toMatchObject({
      ok: true,
      project: { cdpTarget: { group: 'OpenJS Foundation', project: 'PEP', subproject: 'PEP' } },
    })
  })

  it('carries the trimmed leaf metadata and the mapped status', () => {
    const result = parsePccRow([
      {
        PROJECT_ID: ' pcc-1 ',
        NAME: ' Kubescape ',
        DESCRIPTION: ' Kubernetes security platform ',
        PROJECT_LOGO: 'https://example.org/kubescape.svg',
        PROJECT_STATUS: 'Formation - Engaged',
        PROJECT_ADMIN_CATEGORY: 'Incubating',
        MAPPED_PROJECT_NAME: 'Kubescape',
        MAPPED_PROJECT_SLUG: 'kubescape',
        HIERARCHY_LEVEL: 1,
        SEGMENT_ID: 'segment-1',
      },
      {
        PROJECT_ID: ' pcc-1 ',
        NAME: ' Kubescape ',
        MAPPED_PROJECT_NAME: 'Cloud Native Computing Foundation (CNCF)',
        MAPPED_PROJECT_SLUG: 'cncf',
        HIERARCHY_LEVEL: 2,
      },
    ])

    expect(result).toEqual({
      ok: true,
      project: {
        pccProjectId: 'pcc-1',
        pccSlug: 'kubescape',
        name: 'Kubescape',
        status: SegmentStatus.FORMATION,
        maturity: 'Incubating',
        description: 'Kubernetes security platform',
        logoUrl: 'https://example.org/kubescape.svg',
        segmentIdsFromSnowflake: ['segment-1'],
        cdpTarget: {
          group: 'Cloud Native Computing Foundation (CNCF)',
          project: 'Kubescape',
          subproject: 'Kubescape',
        },
        creationTarget: null,
      },
    })
  })

  it('keeps one segment id when every hierarchy row repeats it', () => {
    const rows = spineRowsFromTop(['LF AI & Data', 'AIRSEAI']).map((row) => ({
      ...row,
      SEGMENT_ID: 'segment-1',
    }))

    const result = parsePccRow(rows)

    expect(result).toMatchObject({ ok: true, project: { segmentIdsFromSnowflake: ['segment-1'] } })
  })

  it('keeps every segment id when two CDP sub-projects share the PCC project', () => {
    const rows = spineRowsFromTop(['FINOS', 'Rune DSL'])

    const result = parsePccRow([
      ...rows.map((row) => ({ ...row, SEGMENT_ID: 'segment-rosetta' })),
      ...rows.map((row) => ({ ...row, SEGMENT_ID: 'segment-rune' })),
    ])

    expect(result).toMatchObject({
      ok: true,
      project: {
        segmentIdsFromSnowflake: ['segment-rosetta', 'segment-rune'],
        cdpTarget: { group: 'FINOS', project: 'Rune DSL', subproject: 'Rune DSL' },
      },
    })
  })

  it('has no segment id for a project that is not in CDP', () => {
    const result = parsePccRow(spineRowsFromTop(['Zephyr Project']))

    expect(result).toMatchObject({ ok: true, project: { segmentIdsFromSnowflake: [] } })
  })

  it('reports a leaf that has no hierarchy level as a schema mismatch', () => {
    const result = parsePccRow([
      {
        PROJECT_ID: 'pcc-orphan',
        NAME: 'Orphan Project',
        MAPPED_PROJECT_NAME: null,
        MAPPED_PROJECT_SLUG: null,
        HIERARCHY_LEVEL: null,
      },
    ])

    expect(result).toEqual({
      ok: false,
      errorType: 'SCHEMA_MISMATCH',
      pccProjectId: 'pcc-orphan',
      pccSlug: null,
      details: {
        reason: 'no rows with valid HIERARCHY_LEVEL',
        projectId: 'pcc-orphan',
        name: 'Orphan Project',
      },
    })
  })

  it('reports a missing ancestor name instead of guessing the group', () => {
    const [top, leaf] = spineRowsFromTop(['LF Edge', 'InstantX'])

    const result = parsePccRow([{ ...top, MAPPED_PROJECT_NAME: '  ' }, leaf])

    expect(result).toEqual({
      ok: false,
      errorType: 'SCHEMA_MISMATCH',
      pccProjectId: 'pcc-leaf',
      pccSlug: 'instantx',
      details: {
        reason: 'missing MAPPED_PROJECT_NAME at hierarchy_level=2',
        maxLevel: 2,
        projectId: 'pcc-leaf',
        name: 'InstantX',
      },
    })
  })

  it('reports rows without a project id or name as a schema mismatch', () => {
    const result = parsePccRow([{ PROJECT_ID: 'pcc-1', NAME: null, HIERARCHY_LEVEL: 1 }])

    expect(result).toEqual({
      ok: false,
      errorType: 'SCHEMA_MISMATCH',
      details: { reason: 'missing required fields', missingFields: ['NAME'] },
    })
  })
})

describe('parsePccRow under The Linux Foundation', () => {
  it('gives a Membership project its own group', () => {
    const result = parsePccRow(
      spineRowsWithModelsFromTop([
        LINUX_FOUNDATION,
        { name: 'Spack Fund', model: 'Membership' },
        { name: 'Spack' },
      ]),
    )

    expect(result).toMatchObject({
      ok: true,
      project: { cdpTarget: { group: 'Spack Fund', project: 'Spack', subproject: 'Spack' } },
    })
  })

  it('maps a Membership project without children to all three CDP levels', () => {
    const result = parsePccRow(
      spineRowsWithModelsFromTop([
        LINUX_FOUNDATION,
        { name: 'The FAIR Web Foundation', model: 'Membership' },
      ]),
    )

    expect(result).toMatchObject({
      ok: true,
      project: {
        cdpTarget: {
          group: 'The FAIR Web Foundation',
          project: 'The FAIR Web Foundation',
          subproject: 'The FAIR Web Foundation',
        },
      },
    })
  })

  it('keeps the level below a Membership project as the CDP project', () => {
    const result = parsePccRow(
      spineRowsWithModelsFromTop([
        LINUX_FOUNDATION,
        { name: 'Kokkos Fund', model: 'Membership' },
        { name: 'Kokkos' },
        { name: 'Kokkos Kernels' },
      ]),
    )

    expect(result).toMatchObject({
      ok: true,
      project: {
        cdpTarget: { group: 'Kokkos Fund', project: 'Kokkos', subproject: 'Kokkos Kernels' },
      },
    })
  })

  it('treats a model that combines Membership with another one as Membership', () => {
    const result = parsePccRow(
      spineRowsWithModelsFromTop([
        LINUX_FOUNDATION,
        { name: 'AI Infrastructure Alliance', model: 'Membership;Crowdfunding' },
        { name: 'AIIA Landscape' },
      ]),
    )

    expect(result).toMatchObject({
      ok: true,
      project: { cdpTarget: { group: 'AI Infrastructure Alliance' } },
    })
  })

  it.each(['Alternate Funding', 'Crowdfunding', null, undefined])(
    'keeps a project with model %s under The Linux Foundation group',
    (model) => {
      const result = parsePccRow(
        spineRowsWithModelsFromTop([LINUX_FOUNDATION, { name: 'Open Quantum Safe', model }]),
      )

      expect(result).toMatchObject({
        ok: true,
        project: {
          cdpTarget: {
            group: 'The Linux Foundation',
            project: 'Open Quantum Safe',
            subproject: 'Open Quantum Safe',
          },
        },
      })
    },
  )

  it('keeps the top foundation as the group of a foundation nested in it', () => {
    const result = parsePccRow(
      spineRowsWithModelsFromTop([
        LINUX_FOUNDATION,
        { name: 'LF Decentralized Trust', model: 'Membership' },
        { name: 'Trust Over IP Project (ToIP)', model: 'Membership' },
        { name: 'Concepts & Terminology Working Group' },
      ]),
    )

    expect(result).toMatchObject({
      ok: true,
      project: {
        cdpTarget: {
          group: 'LF Decentralized Trust',
          project: 'Trust Over IP Project (ToIP)',
          subproject: 'Concepts & Terminology Working Group',
        },
      },
    })
  })

  it('leaves a Membership project under another foundation where it is', () => {
    const result = parsePccRow(
      spineRowsWithModelsFromTop([
        { name: 'LF Networking (LFN)', model: 'Membership' },
        { name: 'Anuket', model: 'Membership' },
        { name: 'RC1' },
      ]),
    )

    expect(result).toMatchObject({
      ok: true,
      project: {
        cdpTarget: { group: 'LF Networking (LFN)', project: 'Anuket', subproject: 'RC1' },
      },
    })
  })
})

describe('parsePccRow creation target', () => {
  it('uses one PCC project for the three CDP levels of a standalone project', () => {
    const result = parsePccRow(spineRowsFromTop(['Zephyr Project']))

    const zephyr = { pccProjectId: 'pcc-0', name: 'Zephyr Project', slug: 'zephyr-project' }
    expect(result).toMatchObject({
      ok: true,
      project: { creationTarget: { group: zephyr, project: zephyr, subprojects: [zephyr] } },
    })
  })

  it('gives a project under a foundation its own sub-project', () => {
    const result = parsePccRow(spineRowsFromTop(['LF AI & Data', 'AIRSEAI']))

    const airseai = { pccProjectId: 'pcc-1', name: 'AIRSEAI', slug: 'airseai' }
    expect(result).toMatchObject({
      ok: true,
      project: {
        creationTarget: {
          group: { pccProjectId: 'pcc-0', name: 'LF AI & Data', slug: 'lf-ai-&-data' },
          project: airseai,
          subprojects: [airseai],
        },
      },
    })
  })

  it('lists the project sub-project before the leaf of a three-level tree', () => {
    const result = parsePccRow(spineRowsFromTop(['LF Networking (LFN)', 'Anuket', 'RC1']))

    const anuket = { pccProjectId: 'pcc-1', name: 'Anuket', slug: 'anuket' }
    expect(result).toMatchObject({
      ok: true,
      project: {
        creationTarget: {
          project: anuket,
          subprojects: [anuket, { pccProjectId: 'pcc-2', name: 'RC1', slug: 'rc1' }],
        },
      },
    })
  })

  it('turns the levels between the project and the leaf into sub-projects', () => {
    const result = parsePccRow(
      spineRowsFromTop(['Foundation', 'Umbrella', 'Working Group', 'Task Force', 'Plugin']),
    )

    expect(result).toMatchObject({
      ok: true,
      project: {
        creationTarget: {
          group: { name: 'Foundation' },
          project: { name: 'Umbrella' },
          subprojects: [
            { pccProjectId: 'pcc-1', name: 'Umbrella' },
            { pccProjectId: 'pcc-2', name: 'Working Group' },
            { pccProjectId: 'pcc-3', name: 'Task Force' },
            { pccProjectId: 'pcc-4', name: 'Plugin' },
          ],
        },
      },
    })
  })

  it('keeps a Community Project under The Linux Foundation group', () => {
    const result = parsePccRow(
      spineRowsWithModelsFromTop([LINUX_FOUNDATION, { name: 'Open Quantum Safe' }]),
    )

    expect(result).toMatchObject({
      ok: true,
      project: {
        creationTarget: {
          group: { pccProjectId: LINUX_FOUNDATION.id, name: 'The Linux Foundation' },
          project: { name: 'Open Quantum Safe' },
          subprojects: [{ name: 'Open Quantum Safe' }],
        },
      },
    })
  })

  it('leaves The Linux Foundation out when a Membership project is the group', () => {
    const result = parsePccRow(
      spineRowsWithModelsFromTop([
        LINUX_FOUNDATION,
        { name: 'The FAIR Web Foundation', model: 'Membership' },
        { name: 'FAIR Package Manager' },
      ]),
    )

    expect(result).toMatchObject({
      ok: true,
      project: {
        creationTarget: {
          group: { pccProjectId: 'pcc-1', name: 'The FAIR Web Foundation' },
          project: { pccProjectId: 'pcc-2', name: 'FAIR Package Manager' },
          subprojects: [{ pccProjectId: 'pcc-2', name: 'FAIR Package Manager' }],
        },
      },
    })
  })

  it('has no creation target when a level it needs has no slug', () => {
    const [top, middle, leaf] = spineRowsFromTop(['LF Networking (LFN)', 'Anuket', 'RC1'])

    const result = parsePccRow([top, { ...middle, MAPPED_PROJECT_SLUG: ' ' }, leaf])

    expect(result).toMatchObject({
      ok: true,
      project: {
        cdpTarget: { group: 'LF Networking (LFN)', project: 'Anuket', subproject: 'RC1' },
        creationTarget: null,
      },
    })
  })

  it('has no creation target when a level between the project and the leaf has no PCC id', () => {
    const rows = spineRowsFromTop(['Foundation', 'Umbrella', 'Working Group', 'Plugin'])

    const result = parsePccRow([rows[0], rows[1], { ...rows[2], MAPPED_PROJECT_ID: null }, rows[3]])

    expect(result).toMatchObject({ ok: true, project: { creationTarget: null } })
  })
})
