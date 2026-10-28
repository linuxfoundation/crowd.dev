import { test as base, describe, expect } from 'vitest'

import { DEFAULT_TENANT_ID, generateUUIDv1 } from '@crowd/common'
import type { QueryExecutor } from '@crowd/database'
import { withQx } from '@crowd/test-kit/db'

import {
  IValidationResultUpsert,
  listValidationUnitsPage,
  setUnitsValidation,
  upsertValidationResult,
} from './completenessValidation'

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

interface SyncUnitFixture {
  integrationId: string
  channelName?: string
  status?: string
  validationEnabled?: boolean
}

async function createSyncUnit(qx: QueryExecutor, fixture: SyncUnitFixture): Promise<string> {
  const id = generateUUIDv1()
  const channelId = generateUUIDv1()
  await qx.result(
    `
    INSERT INTO integration.sync_units
      (id, "integrationId", platform, "channelId", "channelName", "syncName", status, "validationEnabled")
    VALUES
      ($(id), $(integrationId), 'github', $(channelId), $(channelName), 'issues', $(status), $(validationEnabled))
    `,
    {
      id,
      channelId,
      integrationId: fixture.integrationId,
      channelName: fixture.channelName ?? channelId,
      status: fixture.status ?? 'active',
      validationEnabled: fixture.validationEnabled ?? false,
    },
  )
  return id
}

async function getResults(qx: QueryExecutor, unitId: string) {
  return qx.select(
    `SELECT "activityTypes", "windowStart"::text AS "windowStart", "windowEnd"::text AS "windowEnd",
            "sourceCount"::int AS "sourceCount", "tinybirdCount"::int AS "tinybirdCount", gap::int AS gap
     FROM integration.sync_validation_results
     WHERE "unitId" = $(unitId)
     ORDER BY "windowStart"`,
    { unitId },
  )
}

function baseResult(overrides: Partial<IValidationResultUpsert>): IValidationResultUpsert {
  return {
    unitId: '',
    counterKey: 'issues',
    activityTypes: ['issues-opened'],
    window: { kind: 'range', from: '2026-10-01T00:00:00Z', to: '2026-10-02T00:00:00Z' },
    sourceCount: 10,
    tinybirdCount: 10,
    ...overrides,
  }
}

describe('upsertValidationResult', () => {
  test('second upsert on the same key and window replaces counts and derives gap', async ({
    qx,
  }) => {
    const unitId = await createSyncUnit(qx, { integrationId: await createIntegration(qx) })

    await upsertValidationResult(qx, baseResult({ unitId, sourceCount: 5, tinybirdCount: 5 }))
    await upsertValidationResult(
      qx,
      baseResult({
        unitId,
        activityTypes: ['issues-opened', 'issues-closed'],
        sourceCount: 10,
        tinybirdCount: 12,
      }),
    )

    const rows = await getResults(qx, unitId)
    expect(rows).toHaveLength(1)
    expect(rows[0]).toMatchObject({
      activityTypes: ['issues-opened', 'issues-closed'],
      sourceCount: 10,
      tinybirdCount: 12,
      gap: -2,
    })
  })

  test('all-time stores infinite bounds and stays separate from a range row', async ({ qx }) => {
    const unitId = await createSyncUnit(qx, { integrationId: await createIntegration(qx) })

    await upsertValidationResult(qx, baseResult({ unitId, window: { kind: 'all-time' } }))
    await upsertValidationResult(qx, baseResult({ unitId }))

    const rows = await getResults(qx, unitId)
    expect(rows).toHaveLength(2)
    expect(rows[0]).toMatchObject({ windowStart: '-infinity', windowEnd: 'infinity' })
    expect(rows[1].windowStart).not.toBe('-infinity')
  })
})

describe('listValidationUnitsPage', () => {
  test('enabled returns only enabled active units ordered by id, honoring afterId and limit', async ({
    qx,
  }) => {
    const integrationId = await createIntegration(qx)
    const enabled = await Promise.all(
      [1, 2, 3].map(() => createSyncUnit(qx, { integrationId, validationEnabled: true })),
    )
    await createSyncUnit(qx, { integrationId })
    await createSyncUnit(qx, { integrationId, validationEnabled: true, status: 'paused' })
    const deletedIntegrationId = await createIntegration(qx)
    await createSyncUnit(qx, { integrationId: deletedIntegrationId, validationEnabled: true })
    await qx.result(`UPDATE public.integrations SET "deletedAt" = NOW() WHERE id = $(id)`, {
      id: deletedIntegrationId,
    })
    const sorted = [...enabled].sort()

    const firstPage = await listValidationUnitsPage(qx, { kind: 'enabled' }, { limit: 2 })
    const secondPage = await listValidationUnitsPage(
      qx,
      { kind: 'enabled' },
      { afterId: sorted[1], limit: 2 },
    )

    expect(firstPage.map((unit) => unit.id)).toEqual(sorted.slice(0, 2))
    expect(secondPage.map((unit) => unit.id)).toEqual(sorted.slice(2))
  })

  test('channel matches case-insensitively', async ({ qx }) => {
    const integrationId = await createIntegration(qx)
    const unitId = await createSyncUnit(qx, {
      integrationId,
      channelName: 'https://github.com/foo/bar',
    })

    const units = await listValidationUnitsPage(
      qx,
      { kind: 'channel', channelName: 'https://github.com/Foo/Bar' },
      { limit: 10 },
    )

    expect(units.map((unit) => unit.id)).toEqual([unitId])
  })

  test('units includes a paused unit and excludes a decommissioned one', async ({ qx }) => {
    const integrationId = await createIntegration(qx)
    const paused = await createSyncUnit(qx, { integrationId, status: 'paused' })
    const decommissioned = await createSyncUnit(qx, { integrationId, status: 'decommissioned' })

    const units = await listValidationUnitsPage(
      qx,
      { kind: 'units', unitIds: [paused, decommissioned] },
      { limit: 10 },
    )

    expect(units.map((unit) => unit.id)).toEqual([paused])
  })

  test('units with an empty id list returns nothing', async ({ qx }) => {
    const units = await listValidationUnitsPage(qx, { kind: 'units', unitIds: [] }, { limit: 10 })

    expect(units).toEqual([])
  })
})

describe('setUnitsValidation', () => {
  test('sets both columns and leaves nextRunAt unchanged', async ({ qx }) => {
    const unitId = await createSyncUnit(qx, { integrationId: await createIntegration(qx) })
    const before = await qx.selectOne(
      `SELECT "nextRunAt"::text AS "nextRunAt" FROM integration.sync_units WHERE id = $(unitId)`,
      { unitId },
    )

    const updated = await setUnitsValidation(qx, [unitId], true, ['issues'])

    const after = await qx.selectOne(
      `SELECT "nextRunAt"::text AS "nextRunAt", "validationEnabled", "validationCounterKeys"
       FROM integration.sync_units WHERE id = $(unitId)`,
      { unitId },
    )
    expect(updated).toBe(1)
    expect(after).toMatchObject({
      nextRunAt: before.nextRunAt,
      validationEnabled: true,
      validationCounterKeys: ['issues'],
    })
  })
})
