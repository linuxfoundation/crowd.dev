import { test as base, describe, expect } from 'vitest'

import { DEFAULT_TENANT_ID, generateUUIDv1 } from '@crowd/common'
import type { QueryExecutor } from '@crowd/database'
import { withQx } from '@crowd/test-kit/db'

import { listShadowDiffUnits } from './syncUnits'

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

async function createSyncUnit(
  qx: QueryExecutor,
  integrationId: string,
  overrides: {
    channelName: string
    syncName: string
    status?: string
    emitEnabled?: boolean
    watermark?: Record<string, unknown> | null
  },
): Promise<string> {
  const id = generateUUIDv1()
  await qx.result(
    `
    INSERT INTO integration.sync_units
      (id, "integrationId", platform, "channelId", "channelName", "syncName", status, "emitEnabled", watermark)
    VALUES
      ($(id), $(integrationId), 'github', $(channelName), $(channelName), $(syncName), $(status), $(emitEnabled), $(watermark)::jsonb)
    `,
    {
      id,
      integrationId,
      channelName: overrides.channelName,
      syncName: overrides.syncName,
      status: overrides.status ?? 'active',
      emitEnabled: overrides.emitEnabled ?? false,
      watermark: overrides.watermark === undefined ? null : JSON.stringify(overrides.watermark),
    },
  )
  return id
}

describe('listShadowDiffUnits', () => {
  test('returns only active, shadow-mode units in the incremental phase', async ({ qx }) => {
    const integrationId = await createIntegration(qx)

    const eligibleId = await createSyncUnit(qx, integrationId, {
      channelName: 'https://github.com/kubernetes/kubernetes',
      syncName: 'issues',
      watermark: { phase: 'incremental' },
    })

    await createSyncUnit(qx, integrationId, {
      channelName: 'https://github.com/kubernetes/kubernetes',
      syncName: 'forks',
      emitEnabled: true,
      watermark: { phase: 'incremental' },
    })

    await createSyncUnit(qx, integrationId, {
      channelName: 'https://github.com/torvalds/linux',
      syncName: 'issues',
      status: 'paused',
      watermark: { phase: 'incremental' },
    })

    await createSyncUnit(qx, integrationId, {
      channelName: 'https://github.com/rust-lang/rust',
      syncName: 'issues',
      watermark: { phase: 'backfill' },
    })

    await createSyncUnit(qx, integrationId, {
      channelName: 'https://github.com/nodejs/node',
      syncName: 'issues',
      watermark: null,
    })

    const result = await listShadowDiffUnits(qx)

    expect(result).toHaveLength(1)
    expect(result[0]).toMatchObject({
      id: eligibleId,
      integrationId,
      channelName: 'https://github.com/kubernetes/kubernetes',
      syncName: 'issues',
    })
  })
})
