import { test as base, describe, expect } from 'vitest'

import { generateUUIDv1 } from '@crowd/common'
import type { QueryExecutor } from '@crowd/database'
import { withQx } from '@crowd/test-kit/db'
import { createOrganizations, createSegments } from '@crowd/test-kit/factories'

import {
  approvePendingOrganizationProjectMatches,
  findOrganizationProjectMatches,
  insertOrganizationProjectMatches,
  rejectOrganizationProjectMatch,
} from './projectMatches'

const test = withQx(base)

async function createChaossProject(qx: QueryExecutor) {
  const { projectGroups } = await createSegments(qx, [
    {
      name: 'CHAOSS',
      slug: 'chaoss',
      sourceId: 'pcc-chaoss',
      projects: [
        {
          name: 'CHAOSS',
          slug: 'chaoss',
          sourceId: 'pcc-chaoss',
          subprojects: [{ name: 'CHAOSS', slug: 'chaoss', sourceId: 'pcc-chaoss' }],
        },
      ],
    },
  ])
  const [group] = projectGroups
  const [project] = group.projects
  return { group: group.row, project: project.row, subproject: project.subprojects[0] }
}

async function createOrganization(
  qx: QueryExecutor,
  displayName: string,
  isAffiliationBlocked = false,
): Promise<string> {
  const [organization] = await createOrganizations(qx, [{ displayName, isAffiliationBlocked }])
  return organization.id
}

function pairsOf(organizationIds: string[], segmentIds: string[]) {
  return organizationIds.flatMap((organizationId) =>
    segmentIds.map((segmentId) => ({ organizationId, segmentId })),
  )
}

async function createReviewer(qx: QueryExecutor): Promise<string> {
  const id = generateUUIDv1()
  await qx.result(
    `
    INSERT INTO users (id, email, "createdAt", "updatedAt")
    VALUES ($(id), $(email), NOW(), NOW())
    `,
    { id, email: `${id}@reviewers.test` },
  )
  return id
}

async function pendingMatches(qx: QueryExecutor) {
  const { rows } = await findOrganizationProjectMatches(qx, { limit: 10, offset: 0 })
  return rows
}

describe('insertOrganizationProjectMatches', () => {
  test('records one match for a project whose three levels share the PCC id', async ({ qx }) => {
    const { group, project, subproject } = await createChaossProject(qx)
    const organizationId = await createOrganization(qx, 'CHAOSS')

    const inserted = await insertOrganizationProjectMatches(qx, {
      pairs: pairsOf([organizationId], [group.id, project.id, subproject.id]),
      detectedBy: 'organization-created',
    })

    expect(inserted).toBe(1)
    expect(await pendingMatches(qx)).toMatchObject([
      {
        organizationId,
        segmentId: group.id,
        segmentType: 'projectGroup',
        status: 'pending',
        detectedBy: 'organization-created',
        matchedOrganizationName: 'CHAOSS',
        matchedSegmentName: 'CHAOSS',
        isAffiliationBlocked: false,
      },
    ])
  })

  test('keeps the match on the project when its group is another project', async ({ qx }) => {
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
    const [project] = projectGroups[0].projects
    const organizationId = await createOrganization(qx, 'Kubernetes')

    await insertOrganizationProjectMatches(qx, {
      pairs: pairsOf([organizationId], [project.subprojects[0].id]),
      detectedBy: 'segment-created',
    })

    expect(await pendingMatches(qx)).toMatchObject([
      { segmentId: project.row.id, segmentType: 'project' },
    ])
  })

  test('groups the levels by name when the segments have no PCC id', async ({ qx }) => {
    const { projectGroups } = await createSegments(qx, [
      {
        name: 'Zephyr',
        slug: 'zephyr',
        projects: [{ name: 'zephyr ', slug: 'zephyr', subprojects: [] }],
      },
    ])
    const [group] = projectGroups
    const organizationId = await createOrganization(qx, 'Zephyr')

    await insertOrganizationProjectMatches(qx, {
      pairs: pairsOf([organizationId], [group.projects[0].row.id]),
      detectedBy: 'segment-created',
    })

    expect(await pendingMatches(qx)).toMatchObject([{ segmentId: group.row.id }])
  })

  test('does not record the same pair twice', async ({ qx }) => {
    const { group } = await createChaossProject(qx)
    const organizationId = await createOrganization(qx, 'CHAOSS')
    const pair = {
      pairs: pairsOf([organizationId], [group.id]),
      detectedBy: 'segment-created' as const,
    }

    await insertOrganizationProjectMatches(qx, pair)
    const insertedAgain = await insertOrganizationProjectMatches(qx, pair)

    expect(insertedAgain).toBe(0)
    expect(await pendingMatches(qx)).toHaveLength(1)
  })

  test('does not raise a rejected pair again', async ({ qx }) => {
    const { group } = await createChaossProject(qx)
    const organizationId = await createOrganization(qx, 'CHAOSS')
    const pair = {
      pairs: pairsOf([organizationId], [group.id]),
      detectedBy: 'segment-created' as const,
    }
    await insertOrganizationProjectMatches(qx, pair)
    const [match] = await pendingMatches(qx)
    await rejectOrganizationProjectMatch(qx, match.id, null)

    await insertOrganizationProjectMatches(qx, pair)

    expect(await pendingMatches(qx)).toEqual([])
  })

  test('skips an organization that is already blocked', async ({ qx }) => {
    const { group } = await createChaossProject(qx)
    const blockedOrganizationId = await createOrganization(qx, 'CHAOSS', true)

    const inserted = await insertOrganizationProjectMatches(qx, {
      pairs: pairsOf([blockedOrganizationId], [group.id]),
      detectedBy: 'segment-created',
    })

    expect(inserted).toBe(0)
  })

  test('does not climb to a group that has the name when the project in between is another one', async ({
    qx,
  }) => {
    const { projectGroups } = await createSegments(qx, [
      {
        name: 'Zephyr',
        slug: 'zephyr',
        sourceId: 'pcc-zephyr',
        projects: [
          {
            name: 'Tools',
            slug: 'tools',
            sourceId: 'pcc-tools',
            subprojects: [{ name: 'zephyr', slug: 'zephyr-cli', sourceId: 'pcc-zephyr-cli' }],
          },
        ],
      },
    ])
    const [subproject] = projectGroups[0].projects[0].subprojects
    const organizationId = await createOrganization(qx, 'Zephyr')

    await insertOrganizationProjectMatches(qx, {
      pairs: pairsOf([organizationId], [subproject.id]),
      detectedBy: 'segment-created',
    })

    expect(await pendingMatches(qx)).toMatchObject([
      { segmentId: subproject.id, segmentType: 'subproject' },
    ])
  })

  test('records only the pairs it is given', async ({ qx }) => {
    const { group: chaoss } = await createChaossProject(qx)
    const { projectGroups } = await createSegments(qx, [{ name: 'Zephyr', slug: 'zephyr' }])
    const zephyr = projectGroups[0].row
    const chaossOrganizationId = await createOrganization(qx, 'CHAOSS')
    const zephyrOrganizationId = await createOrganization(qx, 'Zephyr')

    const inserted = await insertOrganizationProjectMatches(qx, {
      pairs: [
        { organizationId: chaossOrganizationId, segmentId: chaoss.id },
        { organizationId: zephyrOrganizationId, segmentId: zephyr.id },
      ],
      detectedBy: 'backfill',
    })

    expect(inserted).toBe(2)
  })

  test('skips a segment that is not an LF one', async ({ qx }) => {
    const { projectGroups } = await createSegments(qx, [
      { name: 'Zephyr', slug: 'nonlf_zephyr', isLF: false },
    ])
    const organizationId = await createOrganization(qx, 'Zephyr')

    const inserted = await insertOrganizationProjectMatches(qx, {
      pairs: pairsOf([organizationId], [projectGroups[0].row.id]),
      detectedBy: 'segment-created',
    })

    expect(inserted).toBe(0)
  })

  test('records nothing when no pair is given', async ({ qx }) => {
    expect(
      await insertOrganizationProjectMatches(qx, { pairs: [], detectedBy: 'segment-created' }),
    ).toBe(0)
  })
})

describe('rejectOrganizationProjectMatch', () => {
  test('closes a pending match and records who decided', async ({ qx }) => {
    const { group } = await createChaossProject(qx)
    const organizationId = await createOrganization(qx, 'CHAOSS')
    const reviewerId = await createReviewer(qx)
    await insertOrganizationProjectMatches(qx, {
      pairs: pairsOf([organizationId], [group.id]),
      detectedBy: 'segment-created',
    })
    const [match] = await pendingMatches(qx)

    const rejected = await rejectOrganizationProjectMatch(qx, match.id, reviewerId)

    expect(rejected).toBe(true)
    const decided = await qx.selectOne(
      `SELECT status, "decidedById", "decidedAt" IS NOT NULL AS "hasDecisionTime"
       FROM "organizationProjectMatches" WHERE id = $(id)`,
      { id: match.id },
    )
    expect(decided).toEqual({
      status: 'rejected',
      decidedById: reviewerId,
      hasDecisionTime: true,
    })
  })

  test('leaves a match that was already decided', async ({ qx }) => {
    const { group } = await createChaossProject(qx)
    const organizationId = await createOrganization(qx, 'CHAOSS')
    await insertOrganizationProjectMatches(qx, {
      pairs: pairsOf([organizationId], [group.id]),
      detectedBy: 'segment-created',
    })
    const [match] = await pendingMatches(qx)
    await approvePendingOrganizationProjectMatches(qx, organizationId, null)

    const rejected = await rejectOrganizationProjectMatch(qx, match.id, null)

    expect(rejected).toBe(false)
    const { rows } = await findOrganizationProjectMatches(qx, {
      status: 'approved',
      limit: 10,
      offset: 0,
    })
    expect(rows).toHaveLength(1)
  })
})

describe('approvePendingOrganizationProjectMatches', () => {
  test('closes every pending match of the organization and leaves the others', async ({ qx }) => {
    const { group } = await createChaossProject(qx)
    const approvedOrganizationId = await createOrganization(qx, 'CHAOSS')
    const otherOrganizationId = await createOrganization(qx, 'Chaoss Project')
    await insertOrganizationProjectMatches(qx, {
      pairs: pairsOf([approvedOrganizationId, otherOrganizationId], [group.id]),
      detectedBy: 'segment-created',
    })

    const approved = await approvePendingOrganizationProjectMatches(
      qx,
      approvedOrganizationId,
      null,
    )

    expect(approved).toBe(1)
    expect(await pendingMatches(qx)).toMatchObject([{ organizationId: otherOrganizationId }])
  })
})

describe('findOrganizationProjectMatches', () => {
  test('pages through the pending matches and counts them', async ({ qx }) => {
    const { group } = await createChaossProject(qx)
    const organizationIds = [
      await createOrganization(qx, 'CHAOSS'),
      await createOrganization(qx, 'Chaoss Project'),
      await createOrganization(qx, 'chaoss'),
    ]
    await insertOrganizationProjectMatches(qx, {
      pairs: pairsOf(organizationIds, [group.id]),
      detectedBy: 'segment-created',
    })

    const firstPage = await findOrganizationProjectMatches(qx, { limit: 2, offset: 0 })
    const secondPage = await findOrganizationProjectMatches(qx, { limit: 2, offset: 2 })

    expect(firstPage).toMatchObject({ count: 3, limit: 2, offset: 0, hasMore: true })
    expect(firstPage.rows).toHaveLength(2)
    expect(secondPage).toMatchObject({ count: 3, hasMore: false })
    expect(secondPage.rows).toHaveLength(1)
  })
})
