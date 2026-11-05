import { test as base, describe, expect } from 'vitest'

import { DEFAULT_TENANT_ID, generateUUIDv1 } from '@crowd/common'
import type { QueryExecutor } from '@crowd/database'
import { withQx } from '@crowd/test-kit/db'

import {
  deactivateOctolensKeywordMappingsNotSeenAcross,
  findOrCreateOctolensAnchorIntegration,
  findOrCreateOctolensSubprojectIntegration,
  listActiveOctolensKeywordMappings,
  reconcileOctolensKeywordMappings,
  softDeleteOctolensSubprojectIntegrationsWithoutActiveMappings,
  upsertOctolensKeywordMapping,
} from './index'

const test = withQx(base)

async function createIntegration(qx: QueryExecutor): Promise<string> {
  const id = generateUUIDv1()
  await qx.result(
    `
    INSERT INTO public.integrations (id, platform, status, "tenantId", "createdAt", "updatedAt", "deletedAt")
    VALUES ($(id), 'octolens', 'done', $(tenantId), NOW(), NOW(), NULL)
    `,
    { id, tenantId: DEFAULT_TENANT_ID },
  )
  return id
}

async function createSegment(qx: QueryExecutor): Promise<string> {
  const id = generateUUIDv1()
  const slug = `segment-${id}`
  await qx.result(
    `
    INSERT INTO public.segments (id, slug, name, "tenantId", "createdAt", "updatedAt")
    VALUES ($(id), $(slug), $(slug), $(tenantId), NOW(), NOW())
    `,
    { id, slug, tenantId: DEFAULT_TENANT_ID },
  )
  return id
}

async function readRowIncludingDeactivated(
  qx: QueryExecutor,
  integrationId: string,
  keywordId: number,
): Promise<{
  segmentId: string
  keyword: string
  deactivatedAt: Date | null
  updatedAt: Date
  lastSeenAt: Date
}> {
  return qx.selectOne(
    `
    SELECT "segmentId", keyword, "deactivatedAt", "updatedAt", "lastSeenAt"
    FROM integration."octolensKeywordMappings"
    WHERE "integrationId" = $(integrationId) AND "keywordId" = $(keywordId)
    `,
    { integrationId, keywordId },
  )
}

async function findActiveMapping(
  qx: QueryExecutor,
  integrationId: string,
  keywordId: number,
): Promise<{
  id: string
  integrationId: string
  segmentId: string
  keywordId: number
  keyword: string
} | null> {
  return qx.selectOneOrNone(
    `
    SELECT id, "integrationId", "segmentId", "keywordId", keyword
    FROM integration."octolensKeywordMappings"
    WHERE "integrationId" = $(integrationId) AND "keywordId" = $(keywordId)
      AND "deactivatedAt" IS NULL
    `,
    { integrationId, keywordId },
  )
}

async function deactivateMapping(
  qx: QueryExecutor,
  integrationId: string,
  keywordId: number,
): Promise<void> {
  await qx.result(
    `
    UPDATE integration."octolensKeywordMappings"
    SET "deactivatedAt" = now()
    WHERE "integrationId" = $(integrationId) AND "keywordId" = $(keywordId)
    `,
    { integrationId, keywordId },
  )
}

describe('octolens keyword mappings', () => {
  test('upsertOctolensKeywordMapping inserts a new mapping', async ({ qx }) => {
    const integrationId = await createIntegration(qx)
    const segmentId = await createSegment(qx)

    await upsertOctolensKeywordMapping(qx, {
      integrationId,
      segmentId,
      keywordId: 101,
      keyword: 'crowd.dev',
    })

    const mapping = await findActiveMapping(qx, integrationId, 101)

    expect(mapping).toMatchObject({
      integrationId,
      segmentId,
      keywordId: 101,
      keyword: 'crowd.dev',
    })
  })

  test('upsertOctolensKeywordMapping updates segment and keyword of an existing mapping', async ({
    qx,
  }) => {
    const integrationId = await createIntegration(qx)
    const segmentId = await createSegment(qx)
    const otherSegmentId = await createSegment(qx)

    await upsertOctolensKeywordMapping(qx, {
      integrationId,
      segmentId,
      keywordId: 5,
      keyword: 'old',
    })
    const before = await findActiveMapping(qx, integrationId, 5)
    await qx.result(
      `
      UPDATE integration."octolensKeywordMappings" SET "updatedAt" = '2020-01-01T00:00:00Z'
      WHERE "integrationId" = $(integrationId)
      `,
      { integrationId },
    )

    await upsertOctolensKeywordMapping(qx, {
      integrationId,
      segmentId: otherSegmentId,
      keywordId: 5,
      keyword: 'new',
    })

    const after = await findActiveMapping(qx, integrationId, 5)
    expect(after).toMatchObject({ id: before?.id, segmentId: otherSegmentId, keyword: 'new' })
    const row = await readRowIncludingDeactivated(qx, integrationId, 5)
    expect(new Date(row.updatedAt).getTime()).toBeGreaterThan(Date.parse('2020-01-02'))
  })

  test('upsertOctolensKeywordMapping on an unchanged mapping only moves lastSeenAt', async ({
    qx,
  }) => {
    const integrationId = await createIntegration(qx)
    const segmentId = await createSegment(qx)
    const data = { integrationId, segmentId, keywordId: 6, keyword: 'same' }

    await upsertOctolensKeywordMapping(qx, data)
    await qx.result(
      `
      UPDATE integration."octolensKeywordMappings"
      SET "updatedAt" = '2020-01-01T00:00:00Z', "lastSeenAt" = '2020-01-01T00:00:00Z'
      WHERE "integrationId" = $(integrationId)
      `,
      { integrationId },
    )

    await upsertOctolensKeywordMapping(qx, data)

    const row = await readRowIncludingDeactivated(qx, integrationId, 6)
    expect(new Date(row.updatedAt).toISOString()).toBe('2020-01-01T00:00:00.000Z')
    expect(new Date(row.lastSeenAt).getTime()).toBeGreaterThan(Date.parse('2020-01-02'))
  })

  test('upsertOctolensKeywordMapping reactivates a deactivated mapping', async ({ qx }) => {
    const integrationId = await createIntegration(qx)
    const segmentId = await createSegment(qx)
    const data = { integrationId, segmentId, keywordId: 7, keyword: 'back' }

    await upsertOctolensKeywordMapping(qx, data)
    await upsertOctolensKeywordMapping(qx, { ...data, keywordId: 8, keyword: 'other' })
    await deactivateMapping(qx, integrationId, 7)
    expect(await findActiveMapping(qx, integrationId, 7)).toBeNull()

    await upsertOctolensKeywordMapping(qx, data)

    expect(await findActiveMapping(qx, integrationId, 7)).toMatchObject({
      keyword: 'back',
    })
    expect((await readRowIncludingDeactivated(qx, integrationId, 7)).deactivatedAt).toBeNull()
  })
})

async function createPlatformIntegration(
  qx: QueryExecutor,
  options: {
    segmentId: string | null
    platform?: string
    deletedAt?: string | null
    updatedAt?: string
  },
): Promise<string> {
  const id = generateUUIDv1()
  await qx.result(
    `
    INSERT INTO public.integrations
      (id, platform, status, "segmentId", "tenantId", "createdAt", "updatedAt", "deletedAt")
    VALUES
      ($(id), $(platform), 'done', $(segmentId), $(tenantId), NOW(),
       COALESCE($(updatedAt)::timestamptz, NOW()), $(deletedAt)::timestamptz)
    `,
    {
      id,
      platform: options.platform ?? 'octolens',
      segmentId: options.segmentId,
      tenantId: DEFAULT_TENANT_ID,
      deletedAt: options.deletedAt ?? null,
      updatedAt: options.updatedAt ?? null,
    },
  )
  return id
}

async function readIntegration(
  qx: QueryExecutor,
  id: string,
): Promise<{ platform: string; status: string; segmentId: string | null; deletedAt: Date | null }> {
  return qx.selectOne(
    `SELECT platform, status, "segmentId", "deletedAt" FROM public.integrations WHERE id = $(id)`,
    { id },
  )
}

async function countIntegrationsForSegment(qx: QueryExecutor, segmentId: string): Promise<number> {
  const row: { count: number } = await qx.selectOne(
    `SELECT count(*)::int AS count FROM public.integrations WHERE "segmentId" = $(segmentId)`,
    { segmentId },
  )
  return row.count
}

describe('octolens subproject integrations', () => {
  test('findOrCreateOctolensSubprojectIntegration creates once and then finds the same row', async ({
    qx,
  }) => {
    const segmentId = await createSegment(qx)

    const first = await findOrCreateOctolensSubprojectIntegration(qx, segmentId)
    const second = await findOrCreateOctolensSubprojectIntegration(qx, segmentId)

    expect(first).toMatchObject({ created: true, restored: false })
    expect(second).toEqual({ id: first.id, created: false, restored: false })
    expect(await countIntegrationsForSegment(qx, segmentId)).toBe(1)
    expect(await readIntegration(qx, first.id)).toMatchObject({
      platform: 'octolens',
      status: 'done',
      segmentId,
      deletedAt: null,
    })
  })

  test('findOrCreateOctolensSubprojectIntegration ignores other platforms on the segment', async ({
    qx,
  }) => {
    const segmentId = await createSegment(qx)
    const githubId = await createPlatformIntegration(qx, { segmentId, platform: 'github' })

    const result = await findOrCreateOctolensSubprojectIntegration(qx, segmentId)

    expect(result.created).toBe(true)
    expect(result.id).not.toBe(githubId)
  })

  test('findOrCreateOctolensSubprojectIntegration throws on two live rows for one segment', async ({
    qx,
  }) => {
    const segmentId = await createSegment(qx)
    await createPlatformIntegration(qx, { segmentId })
    await createPlatformIntegration(qx, { segmentId })

    await expect(findOrCreateOctolensSubprojectIntegration(qx, segmentId)).rejects.toThrow()
  })

  test('findOrCreateOctolensSubprojectIntegration restores the most recently updated soft-deleted row', async ({
    qx,
  }) => {
    const segmentId = await createSegment(qx)
    const older = await createPlatformIntegration(qx, {
      segmentId,
      deletedAt: '2026-01-01T00:00:00Z',
      updatedAt: '2026-01-01T00:00:00Z',
    })
    const newer = await createPlatformIntegration(qx, {
      segmentId,
      deletedAt: '2026-02-01T00:00:00Z',
      updatedAt: '2026-02-01T00:00:00Z',
    })

    const result = await findOrCreateOctolensSubprojectIntegration(qx, segmentId)

    expect(result).toEqual({ id: newer, created: false, restored: true })
    expect((await readIntegration(qx, newer)).deletedAt).toBeNull()
    expect((await readIntegration(qx, older)).deletedAt).not.toBeNull()
    expect(await countIntegrationsForSegment(qx, segmentId)).toBe(2)
  })

  test('findOrCreateOctolensAnchorIntegration creates one segment-less row and ignores subprojects', async ({
    qx,
  }) => {
    const segmentId = await createSegment(qx)
    const subproject = await findOrCreateOctolensSubprojectIntegration(qx, segmentId)
    const github = await createPlatformIntegration(qx, { segmentId: null, platform: 'github' })

    const first = await findOrCreateOctolensAnchorIntegration(qx)
    const second = await findOrCreateOctolensAnchorIntegration(qx)

    expect(first).toMatchObject({ created: true, restored: false })
    expect(second).toEqual({ id: first.id, created: false, restored: false })
    expect([subproject.id, github]).not.toContain(first.id)
    expect(await readIntegration(qx, first.id)).toMatchObject({
      platform: 'octolens',
      segmentId: null,
      deletedAt: null,
    })
  })

  test('findOrCreateOctolensAnchorIntegration restores the most recently updated soft-deleted anchor', async ({
    qx,
  }) => {
    const segmentId = await createSegment(qx)
    await createPlatformIntegration(qx, { segmentId, deletedAt: '2026-03-01T00:00:00Z' })
    const older = await createPlatformIntegration(qx, {
      segmentId: null,
      deletedAt: '2026-01-01T00:00:00Z',
      updatedAt: '2026-01-01T00:00:00Z',
    })
    const newer = await createPlatformIntegration(qx, {
      segmentId: null,
      deletedAt: '2026-02-01T00:00:00Z',
      updatedAt: '2026-02-01T00:00:00Z',
    })

    const result = await findOrCreateOctolensAnchorIntegration(qx)

    expect(result).toEqual({ id: newer, created: false, restored: true })
    expect((await readIntegration(qx, newer)).deletedAt).toBeNull()
    expect((await readIntegration(qx, older)).deletedAt).not.toBeNull()
  })

  test('findOrCreateOctolensAnchorIntegration throws on two live anchors', async ({ qx }) => {
    await createPlatformIntegration(qx, { segmentId: null })
    await createPlatformIntegration(qx, { segmentId: null })

    await expect(findOrCreateOctolensAnchorIntegration(qx)).rejects.toThrow()
  })

  test('listActiveOctolensKeywordMappings returns active mappings of live subproject integrations only', async ({
    qx,
  }) => {
    const segmentA = await createSegment(qx)
    const segmentB = await createSegment(qx)
    const integrationA = await createPlatformIntegration(qx, { segmentId: segmentA })
    const integrationB = await createPlatformIntegration(qx, { segmentId: segmentB })
    const deletedIntegration = await createPlatformIntegration(qx, {
      segmentId: segmentA,
      deletedAt: '2026-01-01T00:00:00Z',
    })
    const githubIntegration = await createPlatformIntegration(qx, {
      segmentId: segmentA,
      platform: 'github',
    })
    const anchor = await createPlatformIntegration(qx, { segmentId: null })

    const upsert = (integrationId: string, segmentId: string, keywordId: number) =>
      upsertOctolensKeywordMapping(qx, {
        integrationId,
        segmentId,
        keywordId,
        keyword: `kw-${keywordId}`,
      })
    await upsert(integrationA, segmentA, 1)
    await upsert(integrationA, segmentA, 2)
    await upsert(integrationB, segmentB, 1)
    await upsert(integrationB, segmentB, 3)
    await upsert(deletedIntegration, segmentA, 4)
    await upsert(githubIntegration, segmentA, 5)
    await upsert(anchor, segmentA, 6)
    await deactivateMapping(qx, integrationB, 3)

    const ours = new Set([
      integrationA,
      integrationB,
      deletedIntegration,
      githubIntegration,
      anchor,
    ])
    const result = (await listActiveOctolensKeywordMappings(qx)).filter((m) =>
      ours.has(m.integrationId),
    )

    expect(result).toHaveLength(3)
    expect(result).toEqual(
      expect.arrayContaining([
        { keywordId: 1, keyword: 'kw-1', integrationId: integrationA, segmentId: segmentA },
        { keywordId: 1, keyword: 'kw-1', integrationId: integrationB, segmentId: segmentB },
        { keywordId: 2, keyword: 'kw-2', integrationId: integrationA, segmentId: segmentA },
      ]),
    )
  })

  test('deactivateOctolensKeywordMappingsNotSeenAcross keeps seen pairs and other platforms', async ({
    qx,
  }) => {
    const segmentA = await createSegment(qx)
    const segmentB = await createSegment(qx)
    const integrationA = await createPlatformIntegration(qx, { segmentId: segmentA })
    const integrationB = await createPlatformIntegration(qx, { segmentId: segmentB })
    const githubIntegration = await createPlatformIntegration(qx, {
      segmentId: segmentA,
      platform: 'github',
    })
    const anchor = await createPlatformIntegration(qx, { segmentId: null })

    for (const [integrationId, segmentId, keywordId] of [
      [integrationA, segmentA, 1],
      [integrationA, segmentA, 2],
      [integrationB, segmentB, 1],
      [githubIntegration, segmentA, 9],
      [anchor, segmentA, 7],
    ] as const) {
      await upsertOctolensKeywordMapping(qx, {
        integrationId,
        segmentId,
        keywordId,
        keyword: `kw-${keywordId}`,
      })
    }

    const deactivated = await deactivateOctolensKeywordMappingsNotSeenAcross(qx, [
      { integrationId: integrationA, keywordId: 1 },
      { integrationId: integrationB, keywordId: 2 },
    ])

    expect(deactivated).toBe(2)
    expect(await findActiveMapping(qx, integrationA, 1)).not.toBeNull()
    expect(await findActiveMapping(qx, integrationA, 2)).toBeNull()
    expect(await findActiveMapping(qx, integrationB, 1)).toBeNull()
    expect(await findActiveMapping(qx, githubIntegration, 9)).not.toBeNull()
    expect(await findActiveMapping(qx, anchor, 7)).not.toBeNull()
  })

  test('deactivateOctolensKeywordMappingsNotSeenAcross with nothing seen deactivates nothing', async ({
    qx,
  }) => {
    const segmentId = await createSegment(qx)
    const integrationId = await createPlatformIntegration(qx, { segmentId })
    await upsertOctolensKeywordMapping(qx, { integrationId, segmentId, keywordId: 1, keyword: 'k' })

    expect(await deactivateOctolensKeywordMappingsNotSeenAcross(qx, [])).toBe(0)
    expect(await findActiveMapping(qx, integrationId, 1)).not.toBeNull()
  })

  test('softDeleteOctolensSubprojectIntegrationsWithoutActiveMappings deletes only empty subproject integrations', async ({
    qx,
  }) => {
    const segmentA = await createSegment(qx)
    const segmentB = await createSegment(qx)
    const withMappings = await createPlatformIntegration(qx, { segmentId: segmentA })
    const empty = await createPlatformIntegration(qx, { segmentId: segmentB })
    const anchor = await createPlatformIntegration(qx, { segmentId: null })
    const github = await createPlatformIntegration(qx, { segmentId: segmentB, platform: 'github' })
    await upsertOctolensKeywordMapping(qx, {
      integrationId: withMappings,
      segmentId: segmentA,
      keywordId: 1,
      keyword: 'live',
    })
    await upsertOctolensKeywordMapping(qx, {
      integrationId: empty,
      segmentId: segmentB,
      keywordId: 2,
      keyword: 'gone',
    })
    await deactivateOctolensKeywordMappingsNotSeenAcross(qx, [
      { integrationId: withMappings, keywordId: 1 },
    ])

    const deleted = await softDeleteOctolensSubprojectIntegrationsWithoutActiveMappings(qx)

    expect(deleted).toContain(empty)
    expect(deleted).not.toContain(withMappings)
    expect(deleted).not.toContain(anchor)
    expect(deleted).not.toContain(github)
    expect((await readIntegration(qx, empty)).deletedAt).not.toBeNull()
    expect((await readIntegration(qx, withMappings)).deletedAt).toBeNull()
    expect((await readIntegration(qx, anchor)).deletedAt).toBeNull()
    expect((await readIntegration(qx, github)).deletedAt).toBeNull()

    const restored = await findOrCreateOctolensSubprojectIntegration(qx, segmentB)
    expect(restored).toEqual({ id: empty, created: false, restored: true })
    expect((await readIntegration(qx, empty)).deletedAt).toBeNull()
  })

  test('integrations owning an active sync unit are never treated as subproject integrations', async ({
    qx,
  }) => {
    const createActiveSyncUnit = (integrationId: string) =>
      qx.result(
        `
        INSERT INTO integration.sync_units
          ("integrationId", platform, "channelId", "channelName", "syncName", status)
        VALUES ($(integrationId), 'octolens', 'octolens', 'octolens', 'mentions', 'active')
        `,
        { integrationId },
      )
    const segmentId = await createSegment(qx)
    const owner = await createPlatformIntegration(qx, { segmentId })
    await createActiveSyncUnit(owner)

    const found = await findOrCreateOctolensSubprojectIntegration(qx, segmentId)
    expect(found.created).toBe(true)
    expect(found.id).not.toBe(owner)

    const deletedSegmentId = await createSegment(qx)
    const deletedOwner = await createPlatformIntegration(qx, {
      segmentId: deletedSegmentId,
      deletedAt: '2026-01-01T00:00:00Z',
    })
    await createActiveSyncUnit(deletedOwner)
    const notRestored = await findOrCreateOctolensSubprojectIntegration(qx, deletedSegmentId)
    expect(notRestored.created).toBe(true)
    expect(notRestored.id).not.toBe(deletedOwner)
    expect((await readIntegration(qx, deletedOwner)).deletedAt).not.toBeNull()

    const anchor = await createPlatformIntegration(qx, { segmentId: null })
    await createActiveSyncUnit(anchor)
    expect(await findOrCreateOctolensAnchorIntegration(qx)).toEqual({
      id: anchor,
      created: false,
      restored: false,
    })

    for (const keywordId of [1, 2]) {
      await upsertOctolensKeywordMapping(qx, {
        integrationId: owner,
        segmentId,
        keywordId,
        keyword: `kw-${keywordId}`,
      })
    }
    await deactivateMapping(qx, owner, 1)
    const other = await createPlatformIntegration(qx, { segmentId: await createSegment(qx) })

    const listed = await listActiveOctolensKeywordMappings(qx)
    await deactivateOctolensKeywordMappingsNotSeenAcross(qx, [
      { integrationId: other, keywordId: 1 },
    ])

    expect(listed.map((m) => m.integrationId)).not.toContain(owner)
    expect(await findActiveMapping(qx, owner, 2)).not.toBeNull()

    await deactivateMapping(qx, owner, 2)
    const deleted = await softDeleteOctolensSubprojectIntegrationsWithoutActiveMappings(qx)

    expect(deleted).not.toContain(owner)
    expect((await readIntegration(qx, owner)).deletedAt).toBeNull()
  })
})

describe('reconcileOctolensKeywordMappings', () => {
  async function liveIntegrationFor(qx: QueryExecutor, segmentId: string): Promise<string | null> {
    const row: { id: string } | null = await qx.selectOneOrNone(
      `SELECT id FROM public.integrations
       WHERE platform = 'octolens' AND "segmentId" = $(segmentId) AND "deletedAt" IS NULL`,
      { segmentId },
    )
    return row?.id ?? null
  }

  test('creates one integration per subproject and maps each keyword under it', async ({ qx }) => {
    const ebpf = await createSegment(qx)
    const valkey = await createSegment(qx)

    const outcome = await reconcileOctolensKeywordMappings(qx, [
      { keywordId: 1, keyword: 'ebpf', segmentId: ebpf },
      { keywordId: 2, keyword: 'cilium', segmentId: ebpf },
      { keywordId: 3, keyword: 'valkey', segmentId: valkey },
    ])

    const ebpfIntegration = await liveIntegrationFor(qx, ebpf)
    const valkeyIntegration = await liveIntegrationFor(qx, valkey)
    expect(outcome.created.sort()).toEqual([ebpfIntegration, valkeyIntegration].sort())
    expect(outcome).toMatchObject({ deactivated: 0, restored: [], softDeleted: [] })
    expect(await findActiveMapping(qx, ebpfIntegration as string, 2)).toMatchObject({
      segmentId: ebpf,
      keyword: 'cilium',
    })
    expect(await findActiveMapping(qx, valkeyIntegration as string, 3)).not.toBeNull()
  })

  test('moves a keyword to its new subproject and soft-deletes the emptied integration', async ({
    qx,
  }) => {
    const from = await createSegment(qx)
    const to = await createSegment(qx)
    await reconcileOctolensKeywordMappings(qx, [{ keywordId: 1, keyword: 'ebpf', segmentId: from }])
    const fromIntegration = (await liveIntegrationFor(qx, from)) as string

    const outcome = await reconcileOctolensKeywordMappings(qx, [
      { keywordId: 1, keyword: 'ebpf', segmentId: to },
    ])

    const toIntegration = (await liveIntegrationFor(qx, to)) as string
    expect(outcome).toEqual({
      deactivated: 1,
      created: [toIntegration],
      restored: [],
      softDeleted: [fromIntegration],
    })
    expect(await findActiveMapping(qx, fromIntegration, 1)).toBeNull()
    expect(await findActiveMapping(qx, toIntegration, 1)).not.toBeNull()
    expect(await listActiveOctolensKeywordMappings(qx)).toContainEqual(
      expect.objectContaining({ keywordId: 1, integrationId: toIntegration, segmentId: to }),
    )
  })

  test('restores the same integration when a soft-deleted subproject gets its keywords back', async ({
    qx,
  }) => {
    const segment = await createSegment(qx)
    const other = await createSegment(qx)
    await reconcileOctolensKeywordMappings(qx, [
      { keywordId: 1, keyword: 'ebpf', segmentId: segment },
    ])
    const original = (await liveIntegrationFor(qx, segment)) as string
    await reconcileOctolensKeywordMappings(qx, [
      { keywordId: 2, keyword: 'other', segmentId: other },
    ])
    expect(await liveIntegrationFor(qx, segment)).toBeNull()

    const outcome = await reconcileOctolensKeywordMappings(qx, [
      { keywordId: 1, keyword: 'ebpf', segmentId: segment },
      { keywordId: 2, keyword: 'other', segmentId: other },
    ])

    expect(outcome.restored).toEqual([original])
    expect(await liveIntegrationFor(qx, segment)).toBe(original)
    expect(await findActiveMapping(qx, original, 1)).not.toBeNull()
  })

  test('rolls back every write when a mapping fails partway through', async ({ qx }) => {
    const kept = await createSegment(qx)
    await reconcileOctolensKeywordMappings(qx, [{ keywordId: 1, keyword: 'keep', segmentId: kept }])
    const keptIntegration = (await liveIntegrationFor(qx, kept)) as string
    const added = await createSegment(qx)

    await expect(
      reconcileOctolensKeywordMappings(qx, [
        { keywordId: 2, keyword: 'new', segmentId: added },
        { keywordId: 3, keyword: null as unknown as string, segmentId: added },
      ]),
    ).rejects.toThrow()

    expect(await liveIntegrationFor(qx, added)).toBeNull()
    expect(await findActiveMapping(qx, keptIntegration, 1)).not.toBeNull()
    expect(await liveIntegrationFor(qx, kept)).toBe(keptIntegration)
  })
})
