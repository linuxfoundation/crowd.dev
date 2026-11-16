import { describe, expect, it } from 'vitest'

import { SegmentStatus } from '@crowd/types'

import { parsePccRow } from './rowParser'

function spineRowsFromTop(path: string[]): Record<string, unknown>[] {
  const leafName = path[path.length - 1]
  return path.map((ancestorName, index) => ({
    PROJECT_ID: 'pcc-leaf',
    NAME: leafName,
    PROJECT_STATUS: 'Active',
    MAPPED_PROJECT_NAME: ancestorName,
    MAPPED_PROJECT_SLUG: ancestorName.toLowerCase().replaceAll(' ', '-'),
    HIERARCHY_LEVEL: path.length - index,
  }))
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
        segmentIdFromSnowflake: 'segment-1',
        cdpTarget: {
          group: 'Cloud Native Computing Foundation (CNCF)',
          project: 'Kubescape',
          subproject: 'Kubescape',
        },
      },
    })
  })

  it('reports a leaf that has no PROJECT_SPINE rows as a schema mismatch', () => {
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
