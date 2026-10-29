import { test as base, describe, expect } from 'vitest'

import { DEFAULT_TENANT_ID, generateUUIDv1 } from '@crowd/common'
import type { QueryExecutor } from '@crowd/database'
import { withQx } from '@crowd/test-kit/db'

import {
  IValidationResultUpsert,
  listValidationChannelsPage,
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
  channelId?: string
  channelName?: string
  syncName?: string
  status?: string
}

async function createSyncUnit(qx: QueryExecutor, fixture: SyncUnitFixture): Promise<string> {
  const id = generateUUIDv1()
  const channelId = fixture.channelId ?? generateUUIDv1()
  await qx.result(
    `
    INSERT INTO integration.sync_units
      (id, "integrationId", platform, "channelId", "channelName", "syncName", status)
    VALUES
      ($(id), $(integrationId), 'github', $(channelId), $(channelName), $(syncName), $(status))
    `,
    {
      id,
      channelId,
      integrationId: fixture.integrationId,
      channelName: fixture.channelName ?? channelId,
      syncName: fixture.syncName ?? 'issues',
      status: fixture.status ?? 'active',
    },
  )
  return id
}

async function getResults(qx: QueryExecutor, unitId: string) {
  return qx.select(
    `SELECT "windowStart"::text AS "windowStart", "windowEnd"::text AS "windowEnd",
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
    await upsertValidationResult(qx, baseResult({ unitId, sourceCount: 10, tinybirdCount: 12 }))

    const rows = await getResults(qx, unitId)
    expect(rows).toHaveLength(1)
    expect(rows[0]).toMatchObject({ sourceCount: 10, tinybirdCount: 12, gap: -2 })
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

  test('rejects an empty window', async ({ qx }) => {
    const unitId = await createSyncUnit(qx, { integrationId: await createIntegration(qx) })
    const instant = '2026-10-01T00:00:00Z'

    await expect(
      upsertValidationResult(
        qx,
        baseResult({ unitId, window: { kind: 'range', from: instant, to: instant } }),
      ),
    ).rejects.toThrow()
  })
})

describe('listValidationUnitsPage', () => {
  test('integration pages by id, lists units without a config row and excludes paused, disabled and soft-deleted-integration units', async ({
    qx,
  }) => {
    const integrationId = await createIntegration(qx)
    const active = await Promise.all([1, 2, 3].map(() => createSyncUnit(qx, { integrationId })))
    const paused = await createSyncUnit(qx, { integrationId, status: 'paused' })
    const disabled = await createSyncUnit(qx, { integrationId })
    await setUnitsValidation(qx, [disabled], false, null)
    const sorted = [...active].sort()
    const target = { kind: 'integration', integrationId } as const

    const firstPage = await listValidationUnitsPage(qx, target, { limit: 2 })
    const secondPage = await listValidationUnitsPage(qx, target, { afterId: sorted[1], limit: 2 })
    const withDisabled = await listValidationUnitsPage(qx, target, {
      limit: 10,
      includeDisabled: true,
    })

    expect(firstPage.map((unit) => unit.id)).toEqual(sorted.slice(0, 2))
    expect(secondPage.map((unit) => unit.id)).toEqual(sorted.slice(2))
    expect(withDisabled.map((unit) => unit.id).sort()).toEqual([...active, disabled].sort())
    expect(withDisabled.map((unit) => unit.id)).not.toContain(paused)

    await qx.result(`UPDATE public.integrations SET "deletedAt" = NOW() WHERE id = $(id)`, {
      id: integrationId,
    })
    expect(await listValidationUnitsPage(qx, target, { limit: 10 })).toEqual([])
  })

  test('channel matches case-insensitively and can be scoped to an integration', async ({ qx }) => {
    const integrationId = await createIntegration(qx)
    const otherIntegrationId = await createIntegration(qx)
    const channelName = 'https://github.com/foo/bar'
    const unitId = await createSyncUnit(qx, { integrationId, channelName })
    const otherUnitId = await createSyncUnit(qx, { integrationId: otherIntegrationId, channelName })
    const requested = 'https://github.com/Foo/Bar'

    const all = await listValidationUnitsPage(
      qx,
      { kind: 'channel', channelName: requested },
      { limit: 10 },
    )
    const scoped = await listValidationUnitsPage(
      qx,
      { kind: 'channel', channelName: requested, integrationId },
      { limit: 10 },
    )

    expect(all.map((unit) => unit.id).sort()).toEqual([unitId, otherUnitId].sort())
    expect(scoped.map((unit) => unit.id)).toEqual([unitId])
    await expect(
      listValidationUnitsPage(
        qx,
        { kind: 'channel', channelName: requested, integrationId: '' },
        { limit: 10 },
      ),
    ).rejects.toThrow()
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

  test('units includes a disabled unit', async ({ qx }) => {
    const unitId = await createSyncUnit(qx, { integrationId: await createIntegration(qx) })
    await setUnitsValidation(qx, [unitId], false, null)

    const units = await listValidationUnitsPage(
      qx,
      { kind: 'units', unitIds: [unitId] },
      { limit: 10 },
    )

    expect(units.map((unit) => unit.id)).toEqual([unitId])
  })

  test('channel and platform exclude a disabled unit', async ({ qx }) => {
    const integrationId = await createIntegration(qx)
    const channelName = `https://github.com/${generateUUIDv1()}`
    const unitId = await createSyncUnit(qx, { integrationId, channelName })
    await setUnitsValidation(qx, [unitId], false, null)

    const byChannel = await listValidationUnitsPage(
      qx,
      { kind: 'channel', channelName },
      { limit: 10 },
    )
    const byChannelIncluded = await listValidationUnitsPage(
      qx,
      { kind: 'channel', channelName },
      { limit: 10, includeDisabled: true },
    )

    expect(byChannel).toEqual([])
    expect(byChannelIncluded.map((unit) => unit.id)).toEqual([unitId])
  })

  test('units with an empty id list returns nothing', async ({ qx }) => {
    const units = await listValidationUnitsPage(qx, { kind: 'units', unitIds: [] }, { limit: 10 })

    expect(units).toEqual([])
  })
})

describe('listValidationChannelsPage', () => {
  test('returns one row per repo, skips fully disabled repos and pages by key', async ({ qx }) => {
    const integrationId = await createIntegration(qx)
    const channels = [1, 2, 3, 4].map(() => ({
      integrationId,
      channelId: generateUUIDv1(),
      channelName: `https://github.com/${generateUUIDv1()}`,
    }))
    const [first, second, third, fourth] = channels
    await createSyncUnit(qx, { ...first, syncName: 'issues' })
    await createSyncUnit(qx, { ...first, syncName: 'pulls' })
    await createSyncUnit(qx, { ...second, status: 'paused' })
    const disabledOnly = await createSyncUnit(qx, third)
    const partlyDisabled = await createSyncUnit(qx, { ...fourth, syncName: 'issues' })
    await createSyncUnit(qx, { ...fourth, syncName: 'pulls' })
    await setUnitsValidation(qx, [disabledOnly, partlyDisabled], false, null)
    const expected = [first, fourth].sort((a, b) => a.channelId.localeCompare(b.channelId))
    const ours = (rows: { integrationId: string }[]) =>
      rows.filter((row) => row.integrationId === integrationId)

    const all = ours(await listValidationChannelsPage(qx, { limit: 1000 }))
    const firstPage = await listValidationChannelsPage(qx, {
      after: { integrationId, channelId: '00000000-0000-0000-0000-000000000000' },
      limit: 1,
    })
    const nextPage = await listValidationChannelsPage(qx, {
      after: { integrationId, channelId: expected[0].channelId },
      limit: 1,
    })

    expect(all).toEqual(expected)
    expect(firstPage).toEqual([expected[0]])
    expect(nextPage).toEqual([expected[1]])
  })

  test('skips a repo under a soft-deleted integration', async ({ qx }) => {
    const integrationId = await createIntegration(qx)
    await createSyncUnit(qx, { integrationId })
    await qx.result(`UPDATE public.integrations SET "deletedAt" = NOW() WHERE id = $(id)`, {
      id: integrationId,
    })

    const rows = await listValidationChannelsPage(qx, { limit: 1000 })

    expect(rows.filter((row) => row.integrationId === integrationId)).toEqual([])
  })
})

describe('setUnitsValidation', () => {
  test('enables with overrides, clears them on null, and keeps results on disable', async ({
    qx,
  }) => {
    const integrationId = await createIntegration(qx)
    const unitId = await createSyncUnit(qx, { integrationId })
    const target = { kind: 'integration', integrationId } as const
    const list = () => listValidationUnitsPage(qx, target, { limit: 10 })

    const enabledCount = await setUnitsValidation(qx, [unitId, unitId], true, [
      'prs',
      'issues',
      'prs',
    ])
    expect(enabledCount).toBe(1)
    expect((await list())[0]).toMatchObject({
      id: unitId,
      validationCounterKeys: ['issues', 'prs'],
    })

    await setUnitsValidation(qx, [unitId], true, null)
    expect((await list())[0]).toMatchObject({ id: unitId, validationCounterKeys: null })

    await upsertValidationResult(qx, baseResult({ unitId }))
    const disabledCount = await setUnitsValidation(qx, [unitId], false, null)

    expect(disabledCount).toBe(1)
    expect(await list()).toEqual([])
    expect(await getResults(qx, unitId)).toHaveLength(1)
  })

  test('disable clears overrides and a later enable with null keys leaves none', async ({ qx }) => {
    const unitId = await createSyncUnit(qx, { integrationId: await createIntegration(qx) })
    const readState = () =>
      qx.selectOne(
        `SELECT v.enabled,
                (SELECT count(*)::int FROM integration.sync_unit_validation_counters c
                 WHERE c."unitId" = v."unitId") AS overrides
         FROM integration.sync_unit_validations v
         WHERE v."unitId" = $(unitId)`,
        { unitId },
      )

    await setUnitsValidation(qx, [unitId], true, ['issues'])
    await setUnitsValidation(qx, [unitId], false, null)
    expect(await readState()).toEqual({ enabled: false, overrides: 0 })

    await setUnitsValidation(qx, [unitId], true, null)
    expect(await readState()).toEqual({ enabled: true, overrides: 0 })
  })
})
