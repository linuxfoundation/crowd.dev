import { test as base, describe, expect } from 'vitest'

import { DEFAULT_TENANT_ID, generateUUIDv1 } from '@crowd/common'
import type { QueryExecutor } from '@crowd/database'
import { withQx } from '@crowd/test-kit/db'

import {
  getShadowRecordsInWindow,
  pruneMatchingShadowRecords,
  recordShadowRecords,
} from './shadowRecords'

const test = withQx(base)

async function createIntegration(qx: QueryExecutor): Promise<string> {
  const id = generateUUIDv1()
  await qx.result(
    `
    INSERT INTO public.integrations (id, platform, status, "tenantId", "createdAt", "updatedAt")
    VALUES ($(id), 'github', 'done', $(tenantId), NOW(), NOW())
    `,
    { id, tenantId: DEFAULT_TENANT_ID },
  )
  return id
}

async function createSyncUnit(qx: QueryExecutor, integrationId: string): Promise<string> {
  const id = generateUUIDv1()
  const channelId = generateUUIDv1()
  await qx.result(
    `
    INSERT INTO integration.sync_units
      (id, "integrationId", platform, "channelId", "channelName", "syncName")
    VALUES
      ($(id), $(integrationId), 'github', $(channelId), $(channelId), 'issues')
    `,
    { id, integrationId, channelId },
  )
  return id
}

describe('getShadowRecordsInWindow', () => {
  test('returns only records whose occurredAt falls within the window', async ({ qx }) => {
    const integrationId = await createIntegration(qx)
    const unitId = await createSyncUnit(qx, integrationId)

    await recordShadowRecords(qx, unitId, [
      {
        type: 'issue',
        sourceId: 'before-window',
        occurredAt: '2026-09-01T00:00:00.000Z',
        data: { title: 'before' },
      },
      {
        type: 'issue',
        sourceId: 'in-window',
        occurredAt: '2026-09-10T00:00:00.000Z',
        data: { title: 'in' },
      },
      {
        type: 'issue',
        sourceId: 'after-window',
        occurredAt: '2026-09-20T00:00:00.000Z',
        data: { title: 'after' },
      },
    ])

    const result = await getShadowRecordsInWindow(
      qx,
      unitId,
      new Date('2026-09-05T00:00:00.000Z'),
      new Date('2026-09-15T00:00:00.000Z'),
    )

    expect(result).toHaveLength(1)
    expect(result[0]).toMatchObject({
      type: 'issue',
      sourceId: 'in-window',
      data: { title: 'in' },
    })
  })

  test('scopes results to the given unitId', async ({ qx }) => {
    const integrationId = await createIntegration(qx)
    const unitId = await createSyncUnit(qx, integrationId)
    const otherUnitId = await createSyncUnit(qx, integrationId)

    await recordShadowRecords(qx, unitId, [
      {
        type: 'issue',
        sourceId: 'mine',
        occurredAt: '2026-09-10T00:00:00.000Z',
        data: { title: 'mine' },
      },
    ])
    await recordShadowRecords(qx, otherUnitId, [
      {
        type: 'issue',
        sourceId: 'not-mine',
        occurredAt: '2026-09-10T00:00:00.000Z',
        data: { title: 'not-mine' },
      },
    ])

    const result = await getShadowRecordsInWindow(
      qx,
      unitId,
      new Date('2026-09-05T00:00:00.000Z'),
      new Date('2026-09-15T00:00:00.000Z'),
    )

    expect(result).toHaveLength(1)
    expect(result[0].sourceId).toBe('mine')
  })
})

describe('pruneMatchingShadowRecords', () => {
  test('deletes only the records that match the given delete list', async ({ qx }) => {
    const integrationId = await createIntegration(qx)
    const unitId = await createSyncUnit(qx, integrationId)

    await recordShadowRecords(qx, unitId, [
      { type: 'issue', sourceId: 'matched', occurredAt: '2026-09-10T00:00:00.000Z', data: {} },
      { type: 'issue', sourceId: 'mismatched', occurredAt: '2026-09-10T00:00:00.000Z', data: {} },
    ])

    const deleted = await pruneMatchingShadowRecords(
      qx,
      unitId,
      new Date('2026-09-05T00:00:00.000Z'),
      new Date('2026-09-15T00:00:00.000Z'),
      [{ type: 'issue', sourceId: 'matched' }],
    )

    expect(deleted).toBe(1)
    const remaining = await getShadowRecordsInWindow(
      qx,
      unitId,
      new Date('2026-09-05T00:00:00.000Z'),
      new Date('2026-09-15T00:00:00.000Z'),
    )
    expect(remaining).toHaveLength(1)
    expect(remaining[0].sourceId).toBe('mismatched')
  })

  test('deletes nothing when the delete list is empty', async ({ qx }) => {
    const integrationId = await createIntegration(qx)
    const unitId = await createSyncUnit(qx, integrationId)

    await recordShadowRecords(qx, unitId, [
      { type: 'issue', sourceId: 'a', occurredAt: '2026-09-10T00:00:00.000Z', data: {} },
      { type: 'issue', sourceId: 'b', occurredAt: '2026-09-10T00:00:00.000Z', data: {} },
    ])

    const deleted = await pruneMatchingShadowRecords(
      qx,
      unitId,
      new Date('2026-09-05T00:00:00.000Z'),
      new Date('2026-09-15T00:00:00.000Z'),
      [],
    )

    expect(deleted).toBe(0)
    const remaining = await getShadowRecordsInWindow(
      qx,
      unitId,
      new Date('2026-09-05T00:00:00.000Z'),
      new Date('2026-09-15T00:00:00.000Z'),
    )
    expect(remaining).toHaveLength(2)
  })

  test('does not delete a matching key that was inserted after the snapshot but outside the window', async ({
    qx,
  }) => {
    const integrationId = await createIntegration(qx)
    const unitId = await createSyncUnit(qx, integrationId)

    await recordShadowRecords(qx, unitId, [
      { type: 'issue', sourceId: 'outside', occurredAt: '2026-09-20T00:00:00.000Z', data: {} },
    ])

    const deleted = await pruneMatchingShadowRecords(
      qx,
      unitId,
      new Date('2026-09-05T00:00:00.000Z'),
      new Date('2026-09-15T00:00:00.000Z'),
      [{ type: 'issue', sourceId: 'outside' }],
    )

    expect(deleted).toBe(0)
  })

  test('does not delete records belonging to other units even if the key matches', async ({
    qx,
  }) => {
    const integrationId = await createIntegration(qx)
    const unitId = await createSyncUnit(qx, integrationId)
    const otherUnitId = await createSyncUnit(qx, integrationId)

    await recordShadowRecords(qx, otherUnitId, [
      { type: 'issue', sourceId: 'other-unit', occurredAt: '2026-09-10T00:00:00.000Z', data: {} },
    ])

    const deleted = await pruneMatchingShadowRecords(
      qx,
      unitId,
      new Date('2026-09-05T00:00:00.000Z'),
      new Date('2026-09-15T00:00:00.000Z'),
      [{ type: 'issue', sourceId: 'other-unit' }],
    )

    expect(deleted).toBe(0)
  })

  test('does not delete a record inserted concurrently with a matching key that was never in the delete list', async ({
    qx,
  }) => {
    const integrationId = await createIntegration(qx)
    const unitId = await createSyncUnit(qx, integrationId)

    await recordShadowRecords(qx, unitId, [
      { type: 'issue', sourceId: 'not-examined', occurredAt: '2026-09-10T00:00:00.000Z', data: {} },
    ])

    const deleted = await pruneMatchingShadowRecords(
      qx,
      unitId,
      new Date('2026-09-05T00:00:00.000Z'),
      new Date('2026-09-15T00:00:00.000Z'),
      [],
    )

    expect(deleted).toBe(0)
    const remaining = await getShadowRecordsInWindow(
      qx,
      unitId,
      new Date('2026-09-05T00:00:00.000Z'),
      new Date('2026-09-15T00:00:00.000Z'),
    )
    expect(remaining).toHaveLength(1)
  })
})
