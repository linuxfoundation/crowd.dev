import { test as base, describe, expect } from 'vitest'

import { DEFAULT_TENANT_ID, generateUUIDv1 } from '@crowd/common'
import type { QueryExecutor } from '@crowd/database'
import { withQx } from '@crowd/test-kit/db'

import {
  ISyncDiffSummaryUpsert,
  getUnitIdsWithSummary,
  upsertSyncDiffSummary,
} from './shadowDiffSummary'

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

async function getSummary(qx: QueryExecutor, unitId: string, day: string) {
  return qx.selectOneOrNone(
    `SELECT * FROM integration.sync_diff_summary WHERE "unitId" = $(unitId) AND day = $(day)`,
    { unitId, day },
  )
}

function baseSummary(overrides: Partial<ISyncDiffSummaryUpsert>): ISyncDiffSummaryUpsert {
  return {
    unitId: '',
    day: '2026-09-17',
    integrationId: '',
    channelName: 'https://github.com/kubernetes/kubernetes',
    missingInNangoCount: 0,
    missingInShadowCount: 0,
    fieldMismatchCount: 0,
    unsupportedSyncCount: 0,
    highSeverityCount: 0,
    ...overrides,
  }
}

describe('upsertSyncDiffSummary', () => {
  test('inserts a row and derives hasMismatch from the counts', async ({ qx }) => {
    const integrationId = await createIntegration(qx)
    const unitId = await createSyncUnit(qx, integrationId)

    await upsertSyncDiffSummary(
      qx,
      baseSummary({ unitId, integrationId, fieldMismatchCount: 3, highSeverityCount: 2 }),
    )

    const row = await getSummary(qx, unitId, '2026-09-17')
    expect(row).toMatchObject({
      fieldMismatchCount: 3,
      highSeverityCount: 2,
      hasMismatch: true,
    })
  })

  test('hasMismatch is false when every count is zero', async ({ qx }) => {
    const integrationId = await createIntegration(qx)
    const unitId = await createSyncUnit(qx, integrationId)

    await upsertSyncDiffSummary(qx, baseSummary({ unitId, integrationId }))

    const row = await getSummary(qx, unitId, '2026-09-17')
    expect(row).toMatchObject({ hasMismatch: false })
  })

  test('overwrites the existing row for the same unit and day on conflict', async ({ qx }) => {
    const integrationId = await createIntegration(qx)
    const unitId = await createSyncUnit(qx, integrationId)

    await upsertSyncDiffSummary(qx, baseSummary({ unitId, integrationId, fieldMismatchCount: 5 }))
    await upsertSyncDiffSummary(qx, baseSummary({ unitId, integrationId, fieldMismatchCount: 0 }))

    const rows = await qx.select(
      `SELECT * FROM integration.sync_diff_summary WHERE "unitId" = $(unitId)`,
      { unitId },
    )
    expect(rows).toHaveLength(1)
    expect(rows[0]).toMatchObject({ fieldMismatchCount: 0, hasMismatch: false })
  })

  test('keeps separate rows for the same unit on different days', async ({ qx }) => {
    const integrationId = await createIntegration(qx)
    const unitId = await createSyncUnit(qx, integrationId)

    await upsertSyncDiffSummary(qx, baseSummary({ unitId, integrationId, day: '2026-09-16' }))
    await upsertSyncDiffSummary(qx, baseSummary({ unitId, integrationId, day: '2026-09-17' }))

    const rows = await qx.select(
      `SELECT * FROM integration.sync_diff_summary WHERE "unitId" = $(unitId) ORDER BY day`,
      { unitId },
    )
    expect(rows).toHaveLength(2)
  })
})

describe('getUnitIdsWithSummary', () => {
  test('returns only the unit ids that already have a summary for the given day', async ({
    qx,
  }) => {
    const integrationId = await createIntegration(qx)
    const summarizedUnitId = await createSyncUnit(qx, integrationId)
    const pendingUnitId = await createSyncUnit(qx, integrationId)

    await upsertSyncDiffSummary(
      qx,
      baseSummary({ unitId: summarizedUnitId, integrationId, day: '2026-09-17' }),
    )

    const result = await getUnitIdsWithSummary(qx, [summarizedUnitId, pendingUnitId], '2026-09-17')

    expect(result).toEqual(new Set([summarizedUnitId]))
  })

  test('ignores summaries from other days', async ({ qx }) => {
    const integrationId = await createIntegration(qx)
    const unitId = await createSyncUnit(qx, integrationId)

    await upsertSyncDiffSummary(qx, baseSummary({ unitId, integrationId, day: '2026-09-16' }))

    const result = await getUnitIdsWithSummary(qx, [unitId], '2026-09-17')

    expect(result).toEqual(new Set())
  })

  test('returns an empty set when no unit ids are given', async ({ qx }) => {
    const result = await getUnitIdsWithSummary(qx, [], '2026-09-17')

    expect(result).toEqual(new Set())
  })
})
