import { test as base, describe, expect } from 'vitest'

import type { QueryExecutor } from '@crowd/database'
import { withQx } from '@crowd/test-kit/db'
import { createSegments } from '@crowd/test-kit/factories'
import { SegmentStatus } from '@crowd/types'

import {
  applyParentMetadata,
  loadInsightsProjectsOfSegments,
  loadParentSegments,
} from './parentMetadata'

const test = withQx(base)

async function createCncf(qx: QueryExecutor) {
  const { projectGroups } = await createSegments(qx, [
    {
      name: 'CNCF',
      slug: 'cncf',
      sourceId: 'pcc-cncf',
      projects: [
        {
          name: 'Kubernetes',
          slug: 'k8s',
          sourceId: 'pcc-kubernetes',
          subprojects: [{ name: 'Kubernetes', slug: 'k8s', sourceId: 'pcc-kubernetes' }],
        },
      ],
    },
  ])
  const [group] = projectGroups
  return { group: group.row, subproject: group.projects[0].subprojects[0] }
}

async function insertInsightsProject(qx: QueryExecutor, segmentId: string): Promise<string> {
  const { id }: { id: string } = await qx.selectOne(
    `
    INSERT INTO "insightsProjects" (name, slug, "segmentId", "isLF")
    VALUES ('Kubernetes', 'k8s', $(segmentId), TRUE)
    RETURNING id
    `,
    { segmentId },
  )
  return id
}

describe('loadParentSegments', () => {
  test('returns the segments that carry one of the PCC ids', async ({ qx }) => {
    const { group } = await createCncf(qx)

    const segments = await loadParentSegments(qx, ['pcc-cncf'])

    expect(segments).toEqual([
      {
        id: group.id,
        type: 'projectGroup',
        name: 'CNCF',
        sourceId: 'pcc-cncf',
        status: SegmentStatus.ACTIVE,
        maturity: null,
        description: null,
      },
    ])
  })

  test('returns nothing when no PCC id is given', async ({ qx }) => {
    await createCncf(qx)

    expect(await loadParentSegments(qx, [])).toEqual([])
  })
})

describe('applyParentMetadata', () => {
  test('writes the planned fields and leaves the others as they are', async ({ qx }) => {
    const { group, subproject } = await createCncf(qx)
    const insightsProjectId = await insertInsightsProject(qx, subproject.id)

    await applyParentMetadata(qx, {
      segments: [
        {
          segmentId: group.id,
          segmentType: 'projectGroup',
          segmentName: 'CNCF',
          pccProjectId: 'pcc-cncf',
          description: { from: null, to: 'Cloud native computing' },
        },
      ],
      insightsProjects: [
        {
          insightsProjectId,
          segmentId: subproject.id,
          pccProjectId: 'pcc-kubernetes',
          logoUrl: { from: null, to: 'https://logos.test/k8s.svg' },
        },
      ],
    })

    expect(await loadParentSegments(qx, ['pcc-cncf'])).toMatchObject([
      { status: SegmentStatus.ACTIVE, maturity: null, description: 'Cloud native computing' },
    ])
    expect(await loadInsightsProjectsOfSegments(qx, [subproject.id])).toEqual([
      {
        id: insightsProjectId,
        segmentId: subproject.id,
        description: null,
        logoUrl: 'https://logos.test/k8s.svg',
      },
    ])
  })

  test('writes a new status and maturity on the segment', async ({ qx }) => {
    const { group } = await createCncf(qx)

    await applyParentMetadata(qx, {
      segments: [
        {
          segmentId: group.id,
          segmentType: 'projectGroup',
          segmentName: 'CNCF',
          pccProjectId: 'pcc-cncf',
          status: { from: SegmentStatus.ACTIVE, to: SegmentStatus.ARCHIVED },
          maturity: { from: null, to: 'Graduated' },
        },
      ],
      insightsProjects: [],
    })

    expect(await loadParentSegments(qx, ['pcc-cncf'])).toMatchObject([
      { status: SegmentStatus.ARCHIVED, maturity: 'Graduated', description: null },
    ])
  })
})
