import { test as base, describe, expect } from 'vitest'

import { DEFAULT_TENANT_ID, generateUUIDv1 } from '@crowd/common'
import type { QueryExecutor } from '@crowd/database'
import { withQx } from '@crowd/test-kit/db'

import { recordShadowRecords } from './shadowRecords'
import {
  claimDueUnits,
  countShadowRepos,
  countUndrainedDecommissionedSyncUnits,
  decommissionAllSyncUnits,
  decommissionSyncUnitsForChannels,
  deleteDrainedSyncUnits,
  hasActiveSyncUnitsForChannelElsewhere,
  hasSyncUnitsForChannel,
  listShadowDiffUnits,
  listShadowDiffUnitsPage,
  listDecommissionedSyncUnitChannels,
  listSyncUnitChannelsForIntegration,
  alignSyncUnitChannelName,
  lockSyncUnitChannel,
  mergeSyncUnitWatermark,
  reassertUnitClaim,
  recordRunFailure,
  recordRunTimeout,
  renewRunningUnitLease,
  reviveDeadLetteredSyncUnits,
  takeOverSyncUnit,
  upsertSyncUnits,
} from './syncUnits'

const test = withQx(base)

async function createIntegration(
  qx: QueryExecutor,
  overrides: { deletedAt?: string; platform?: string } = {},
): Promise<string> {
  const id = generateUUIDv1()
  await qx.result(
    `
    INSERT INTO public.integrations (id, platform, status, "tenantId", "createdAt", "updatedAt", "deletedAt")
    VALUES ($(id), $(platform), 'done', $(tenantId), NOW(), NOW(), $(deletedAt)::timestamptz)
    `,
    {
      id,
      tenantId: DEFAULT_TENANT_ID,
      deletedAt: overrides.deletedAt ?? null,
      platform: overrides.platform ?? 'github',
    },
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
    platform?: string
    lockedAt?: string
  },
): Promise<string> {
  const id = generateUUIDv1()
  await qx.result(
    `
    INSERT INTO integration.sync_units
      (id, "integrationId", platform, "channelId", "channelName", "syncName", status, "emitEnabled", watermark, "lockedAt")
    VALUES
      ($(id), $(integrationId), $(platform), $(channelName), $(channelName), $(syncName), $(status), $(emitEnabled), $(watermark)::jsonb, $(lockedAt)::timestamptz)
    `,
    {
      id,
      integrationId,
      platform: overrides.platform ?? 'github',
      channelName: overrides.channelName,
      syncName: overrides.syncName,
      status: overrides.status ?? 'active',
      emitEnabled: overrides.emitEnabled ?? false,
      watermark: overrides.watermark === undefined ? null : JSON.stringify(overrides.watermark),
      lockedAt: overrides.lockedAt ?? null,
    },
  )
  return id
}

async function readUnitById(
  qx: QueryExecutor,
  id: string,
): Promise<{ watermark: Record<string, unknown> | null }> {
  return qx.selectOne(`SELECT watermark FROM integration.sync_units WHERE id = $(id)`, { id })
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

    await createSyncUnit(qx, integrationId, {
      channelName: 'dummy-channel',
      syncName: 'issues',
      watermark: { phase: 'incremental' },
      platform: 'dummy',
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

  test('excludes units whose parent integration was soft-deleted', async ({ qx }) => {
    const deletedIntegrationId = await createIntegration(qx, {
      deletedAt: '2026-09-01T00:00:00.000Z',
    })

    await createSyncUnit(qx, deletedIntegrationId, {
      channelName: 'https://github.com/kubernetes/kubernetes',
      syncName: 'issues',
      watermark: { phase: 'incremental' },
    })

    const result = await listShadowDiffUnits(qx)

    expect(result).toHaveLength(0)
  })
})

describe('listShadowDiffUnitsPage', () => {
  test('walks eligible channels in pages so a large fleet never comes back in one result', async ({
    qx,
  }) => {
    const integrationId = await createIntegration(qx)
    const channels = ['a/one', 'b/two', 'c/three'].map((repo) => `https://github.com/${repo}`)
    for (const channelName of channels) {
      for (const syncName of ['issues', 'forks']) {
        await createSyncUnit(qx, integrationId, {
          channelName,
          syncName,
          watermark: { phase: 'incremental' },
        })
      }
    }
    await createSyncUnit(qx, integrationId, {
      channelName: channels[0],
      syncName: 'pull-requests',
      emitEnabled: true,
      watermark: { phase: 'incremental' },
    })

    const first = await listShadowDiffUnitsPage(qx, { channelLimit: 2 })
    expect(first.map((unit) => [unit.channelName, unit.syncName])).toEqual([
      [channels[0], 'forks'],
      [channels[0], 'issues'],
      [channels[1], 'forks'],
      [channels[1], 'issues'],
    ])

    const last = first[first.length - 1]
    const second = await listShadowDiffUnitsPage(qx, {
      channelLimit: 2,
      after: { channelName: last.channelName, integrationId: last.integrationId },
    })
    expect(second.map((unit) => [unit.channelName, unit.syncName])).toEqual([
      [channels[2], 'forks'],
      [channels[2], 'issues'],
    ])

    const third = await listShadowDiffUnitsPage(qx, {
      channelLimit: 2,
      after: { channelName: channels[2], integrationId },
    })
    expect(third).toEqual([])
  })
})

async function readUnit(
  qx: QueryExecutor,
  integrationId: string,
  syncName: string,
): Promise<{
  emitEnabled: boolean
  nextRunAt: Date
  watermark: Record<string, unknown> | null
  consecutiveFailures: number
  channelName: string
  delaySeconds: number
}> {
  return qx.selectOne(
    `
    SELECT "emitEnabled", "nextRunAt", watermark, "consecutiveFailures", "channelName",
           EXTRACT(EPOCH FROM ("nextRunAt" - now()))::float8 AS "delaySeconds"
    FROM integration.sync_units
    WHERE "integrationId" = $(integrationId) AND "syncName" = $(syncName)
    `,
    { integrationId, syncName },
  )
}

describe('upsertSyncUnits', () => {
  const base = { platform: 'github', channelId: 'repo-1', channelName: 'org/repo' }

  test('defaults to shadow mode and a 10-900s random start when new fields are omitted', async ({
    qx,
  }) => {
    const integrationId = await createIntegration(qx)

    await upsertSyncUnits(qx, [{ ...base, integrationId, syncName: 'issues' }])

    const unit = await readUnit(qx, integrationId, 'issues')
    expect(unit.emitEnabled).toBe(false)
    expect(unit.delaySeconds).toBeGreaterThanOrEqual(9)
    expect(unit.delaySeconds).toBeLessThanOrEqual(900)
  })

  test('applies emitEnabled and initialDelaySeconds on insert', async ({ qx }) => {
    const integrationId = await createIntegration(qx)

    await upsertSyncUnits(qx, [
      { ...base, integrationId, syncName: 'issues', emitEnabled: true, initialDelaySeconds: 0 },
      { ...base, integrationId, syncName: 'forks' },
    ])

    const issues = await readUnit(qx, integrationId, 'issues')
    expect(issues.emitEnabled).toBe(true)
    expect(Math.abs(issues.delaySeconds)).toBeLessThan(5)
    const forks = await readUnit(qx, integrationId, 'forks')
    expect(forks.emitEnabled).toBe(false)
    expect(forks.delaySeconds).toBeGreaterThanOrEqual(9)
  })

  test('re-running without a watermark leaves emitEnabled, nextRunAt, watermark and failures untouched', async ({
    qx,
  }) => {
    const integrationId = await createIntegration(qx)
    await upsertSyncUnits(qx, [{ ...base, integrationId, syncName: 'issues' }])
    await qx.result(
      `
      UPDATE integration.sync_units
      SET "emitEnabled" = true, "nextRunAt" = '2030-01-01T00:00:00Z',
          watermark = '{"phase":"incremental"}'::jsonb, "consecutiveFailures" = 3
      WHERE "integrationId" = $(integrationId)
      `,
      { integrationId },
    )

    await upsertSyncUnits(qx, [
      {
        ...base,
        integrationId,
        syncName: 'issues',
        channelName: 'org/renamed',
        emitEnabled: false,
        initialDelaySeconds: 0,
      },
    ])

    const unit = await readUnit(qx, integrationId, 'issues')
    expect(unit).toMatchObject({
      emitEnabled: true,
      watermark: { phase: 'incremental' },
      consecutiveFailures: 3,
      channelName: 'org/renamed',
    })
    expect(new Date(unit.nextRunAt).toISOString()).toBe('2030-01-01T00:00:00.000Z')
  })

  test('returns only the rows actually inserted', async ({ qx }) => {
    const integrationId = await createIntegration(qx)
    const units = [
      { ...base, integrationId, syncName: 'issues' },
      { ...base, integrationId, syncName: 'forks' },
    ]

    expect(await upsertSyncUnits(qx, units)).toBe(2)
    expect(await upsertSyncUnits(qx, units)).toBe(0)
    expect(await upsertSyncUnits(qx, [...units, { ...base, integrationId, syncName: 'x' }])).toBe(1)
  })

  test('re-running with a watermark replaces the stored watermark', async ({ qx }) => {
    const integrationId = await createIntegration(qx)
    await upsertSyncUnits(qx, [
      { ...base, integrationId, syncName: 'issues', watermark: { phase: 'backfill' } },
    ])

    await upsertSyncUnits(qx, [
      { ...base, integrationId, syncName: 'issues', watermark: { phase: 'incremental' } },
    ])

    expect((await readUnit(qx, integrationId, 'issues')).watermark).toEqual({
      phase: 'incremental',
    })
  })
})

describe('mergeSyncUnitWatermark', () => {
  test('adds the patch next to the existing keys', async ({ qx }) => {
    const integrationId = await createIntegration(qx)
    const id = await createSyncUnit(qx, integrationId, {
      channelName: 'org',
      syncName: 'badgePoll',
      watermark: { since: '2026-10-27T00:00:00.000Z' },
    })

    expect(await mergeSyncUnitWatermark(qx, id, { backfill: { floor: '2019-03-01' } })).toBe(1)
    expect((await readUnitById(qx, id)).watermark).toEqual({
      since: '2026-10-27T00:00:00.000Z',
      backfill: { floor: '2019-03-01' },
    })
  })

  test('starts from an empty watermark', async ({ qx }) => {
    const integrationId = await createIntegration(qx)
    const id = await createSyncUnit(qx, integrationId, {
      channelName: 'org',
      syncName: 'badgePoll',
    })

    expect(await mergeSyncUnitWatermark(qx, id, { backfill: { floor: '2019-03-01' } })).toBe(1)
    expect((await readUnitById(qx, id)).watermark).toEqual({ backfill: { floor: '2019-03-01' } })
  })

  test('skips a unit that already has the key or is running', async ({ qx }) => {
    const integrationId = await createIntegration(qx)
    const armed = await createSyncUnit(qx, integrationId, {
      channelName: 'armed',
      syncName: 'badgePoll',
      watermark: { backfill: { floor: '2020-01-01' } },
    })
    const running = await createSyncUnit(qx, integrationId, {
      channelName: 'running',
      syncName: 'badgePoll',
      lockedAt: new Date().toISOString(),
    })

    expect(await mergeSyncUnitWatermark(qx, armed, { backfill: { floor: '2019-03-01' } })).toBe(0)
    expect(await mergeSyncUnitWatermark(qx, running, { backfill: { floor: '2019-03-01' } })).toBe(0)
    expect((await readUnitById(qx, armed)).watermark).toEqual({ backfill: { floor: '2020-01-01' } })
  })
})

describe('countShadowRepos and hasSyncUnitsForChannel', () => {
  test('counts repos with a live shadow unit, and finds units by channel id', async ({ qx }) => {
    const integrationId = await createIntegration(qx)

    await createSyncUnit(qx, integrationId, {
      channelName: 'org/emitting',
      syncName: 'issues',
      emitEnabled: true,
    })
    await createSyncUnit(qx, integrationId, {
      channelName: 'org/emitting',
      syncName: 'forks',
      emitEnabled: true,
    })
    await createSyncUnit(qx, integrationId, {
      channelName: 'org/mixed',
      syncName: 'issues',
      emitEnabled: true,
    })
    await createSyncUnit(qx, integrationId, {
      channelName: 'org/retired',
      syncName: 'issues',
      status: 'decommissioned',
    })
    await createSyncUnit(qx, integrationId, { channelName: 'org/mixed', syncName: 'forks' })
    await createSyncUnit(qx, integrationId, { channelName: 'org/shadow', syncName: 'issues' })
    await createSyncUnit(qx, integrationId, { channelName: 'org/shadow', syncName: 'forks' })
    const deletedIntegrationId = await createIntegration(qx, {
      deletedAt: '2026-09-01T00:00:00.000Z',
    })
    await createSyncUnit(qx, deletedIntegrationId, {
      channelName: 'org/orphan',
      syncName: 'issues',
    })

    expect(await countShadowRepos(qx)).toBe(2)
    expect(await hasSyncUnitsForChannel(qx, integrationId, 'org/shadow')).toBe(true)
    expect(await hasSyncUnitsForChannel(qx, integrationId, 'org/unknown')).toBe(false)
  })
})

describe('hasActiveSyncUnitsForChannelElsewhere', () => {
  const staleLockBefore = new Date(Date.now() - 65 * 60_000)
  const freshLock = new Date(Date.now() - 10 * 60_000).toISOString()
  const staleLock = new Date(Date.now() - 120 * 60_000).toISOString()

  test('sees live or still-running units of another github-nango integration only', async ({
    qx,
  }) => {
    const owner = await createIntegration(qx, { platform: 'github-nango' })
    const other = await createIntegration(qx, { platform: 'github-nango' })
    const unitId = await createSyncUnit(qx, owner, { channelName: 'R_1', syncName: 'issues' })
    const elsewhere = (integrationId: string, channelId: string) =>
      hasActiveSyncUnitsForChannelElsewhere(qx, integrationId, channelId, staleLockBefore)

    expect(await elsewhere(other, 'R_1')).toBe(true)
    expect(await elsewhere(owner, 'R_1')).toBe(false)

    await qx.result(
      `UPDATE integration.sync_units SET status = 'decommissioned' WHERE id = $(unitId)`,
      { unitId },
    )
    expect(await elsewhere(other, 'R_1')).toBe(false)

    await qx.result(
      `UPDATE integration.sync_units SET "lockedAt" = $(freshLock) WHERE id = $(unitId)`,
      {
        unitId,
        freshLock,
      },
    )
    expect(await elsewhere(other, 'R_1')).toBe(true)

    const deletedOwner = await createIntegration(qx, {
      platform: 'github-nango',
      deletedAt: '2026-09-01T00:00:00.000Z',
    })
    await createSyncUnit(qx, deletedOwner, { channelName: 'R_2', syncName: 'issues' })
    expect(await elsewhere(other, 'R_2')).toBe(false)

    await createSyncUnit(qx, deletedOwner, {
      channelName: 'R_3',
      syncName: 'issues',
      lockedAt: freshLock,
    })
    expect(await elsewhere(other, 'R_3')).toBe(true)

    await createSyncUnit(qx, deletedOwner, {
      channelName: 'R_4',
      syncName: 'issues',
      lockedAt: staleLock,
    })
    expect(await elsewhere(other, 'R_4')).toBe(false)
  })
})

describe('reviveDeadLetteredSyncUnits', () => {
  async function readUnit(qx: QueryExecutor, id: string) {
    return qx.selectOne(
      `SELECT status, "consecutiveFailures", "lastErrorClass", watermark FROM integration.sync_units WHERE id = $(id)`,
      { id },
    )
  }

  test('reactivates only the dead-lettered unit of that integration and sync, keeping its watermark', async ({
    qx,
  }) => {
    const integrationId = await createIntegration(qx)
    const otherIntegrationId = await createIntegration(qx)
    const dead = await createSyncUnit(qx, integrationId, {
      channelName: 'octolens-global',
      syncName: 'mentionPoll',
      status: 'dead_letter',
      watermark: { startDate: '2026-10-01T00:00:00.000Z' },
    })
    const otherSync = await createSyncUnit(qx, integrationId, {
      channelName: 'octolens-global',
      syncName: 'otherSync',
      status: 'dead_letter',
    })
    const otherIntegration = await createSyncUnit(qx, otherIntegrationId, {
      channelName: 'octolens-global',
      syncName: 'mentionPoll',
      status: 'dead_letter',
    })
    await qx.result(
      `UPDATE integration.sync_units SET "consecutiveFailures" = 5, "lastErrorClass" = 'provider.auth' WHERE id = $(dead)`,
      { dead },
    )

    expect(await reviveDeadLetteredSyncUnits(qx, integrationId, 'mentionPoll')).toBe(1)

    expect(await readUnit(qx, dead)).toEqual({
      status: 'active',
      consecutiveFailures: 0,
      lastErrorClass: null,
      watermark: { startDate: '2026-10-01T00:00:00.000Z' },
    })
    expect((await readUnit(qx, otherSync)).status).toBe('dead_letter')
    expect((await readUnit(qx, otherIntegration)).status).toBe('dead_letter')
  })

  test('leaves an active unit untouched', async ({ qx }) => {
    const integrationId = await createIntegration(qx)
    await createSyncUnit(qx, integrationId, {
      channelName: 'octolens-global',
      syncName: 'mentionPoll',
    })

    expect(await reviveDeadLetteredSyncUnits(qx, integrationId, 'mentionPoll')).toBe(0)
  })

  test('does not revive a unit stopped because the provider refused access', async ({ qx }) => {
    const integrationId = await createIntegration(qx)
    const refused = await createSyncUnit(qx, integrationId, {
      channelName: 'octolens-global',
      syncName: 'mentionPoll',
      status: 'dead_letter',
    })
    await qx.result(
      `UPDATE integration.sync_units SET "lastErrorClass" = 'provider.access_denied' WHERE id = $(refused)`,
      { refused },
    )

    expect(await reviveDeadLetteredSyncUnits(qx, integrationId, 'mentionPoll')).toBe(0)
    expect((await readUnit(qx, refused)).status).toBe('dead_letter')
  })
})

async function setClaimState(
  qx: QueryExecutor,
  id: string,
  state: { lockedAgo: string | null; nextRunAgo: string },
): Promise<void> {
  await qx.result(
    `UPDATE integration.sync_units
     SET "lockedAt" = now() - $(lockedAgo)::interval,
         "nextRunAt" = now() - $(nextRunAgo)::interval
     WHERE id = $(id)`,
    { id, ...state },
  )
}

async function readClaimState(
  qx: QueryExecutor,
  id: string,
): Promise<{ lockedAgoSeconds: number | null; nextRunAt: string }> {
  return qx.selectOne(
    `SELECT EXTRACT(EPOCH FROM (now() - "lockedAt"))::float8 AS "lockedAgoSeconds", "nextRunAt"
     FROM integration.sync_units WHERE id = $(id)`,
    { id },
  )
}

describe('renewRunningUnitLease', () => {
  test('renews the claim and moves the next run to the end of the new lease', async ({ qx }) => {
    const integrationId = await createIntegration(qx)
    const id = await createSyncUnit(qx, integrationId, { channelName: 'repo', syncName: 'issues' })
    await setClaimState(qx, id, { lockedAgo: '6 minutes', nextRunAgo: '10 minutes' })

    expect(await renewRunningUnitLease(qx, id)).toBe(1)

    const lease: { lockedAgoSeconds: number; leaseSeconds: number } = await qx.selectOne(
      `SELECT EXTRACT(EPOCH FROM (now() - "lockedAt"))::float8 AS "lockedAgoSeconds",
              EXTRACT(EPOCH FROM ("nextRunAt" - "lockedAt"))::float8 AS "leaseSeconds"
       FROM integration.sync_units WHERE id = $(id)`,
      { id },
    )
    expect(lease.lockedAgoSeconds).toBeLessThan(5)
    expect(lease.leaseSeconds).toBe(300)
  })

  test('leaves a unit alone once its run has recorded its end', async ({ qx }) => {
    const integrationId = await createIntegration(qx)
    const id = await createSyncUnit(qx, integrationId, { channelName: 'repo', syncName: 'issues' })
    await setClaimState(qx, id, { lockedAgo: null, nextRunAgo: '-5 minutes' })
    const before = await readClaimState(qx, id)

    expect(await renewRunningUnitLease(qx, id)).toBe(0)

    expect(await readClaimState(qx, id)).toEqual(before)
  })
})

describe('claimDueUnits', () => {
  test('reports whether the claim took over a lapsed lease', async ({ qx }) => {
    const integrationId = await createIntegration(qx)
    const freeId = await createSyncUnit(qx, integrationId, { channelName: 'a', syncName: 'issues' })
    const lapsedId = await createSyncUnit(qx, integrationId, {
      channelName: 'b',
      syncName: 'issues',
    })
    await setClaimState(qx, freeId, { lockedAgo: null, nextRunAgo: '1 minute' })
    await setClaimState(qx, lapsedId, { lockedAgo: '6 minutes', nextRunAgo: '1 minute' })

    const claimed = await claimDueUnits(qx, 100)

    expect(claimed.find((u) => u.id === freeId)?.leaseLapsed).toBe(false)
    expect(claimed.find((u) => u.id === lapsedId)?.leaseLapsed).toBe(true)
    expect((await readClaimState(qx, lapsedId)).lockedAgoSeconds).toBeLessThan(5)
  })

  test('skips a due unit whose running lease was renewed, and reclaims it once the lease lapses', async ({
    qx,
  }) => {
    const integrationId = await createIntegration(qx)
    const id = await createSyncUnit(qx, integrationId, { channelName: 'repo', syncName: 'issues' })
    await setClaimState(qx, id, { lockedAgo: '6 minutes', nextRunAgo: '1 minute' })
    await renewRunningUnitLease(qx, id)
    await setClaimState(qx, id, { lockedAgo: '0 minutes', nextRunAgo: '1 minute' })

    expect((await claimDueUnits(qx, 100)).map((u) => u.id)).not.toContain(id)

    await setClaimState(qx, id, { lockedAgo: '6 minutes', nextRunAgo: '1 minute' })
    expect((await claimDueUnits(qx, 100)).map((u) => u.id)).toContain(id)
  })
})

describe('reassertUnitClaim', () => {
  test('locks a unit whose lock a previous run cleared', async ({ qx }) => {
    const integrationId = await createIntegration(qx)
    const id = await createSyncUnit(qx, integrationId, { channelName: 'repo', syncName: 'issues' })
    await setClaimState(qx, id, { lockedAgo: null, nextRunAgo: '1 minute' })

    expect(await reassertUnitClaim(qx, id)).toBe(true)

    expect((await readClaimState(qx, id)).lockedAgoSeconds).toBeLessThan(5)
    expect((await claimDueUnits(qx, 100)).map((u) => u.id)).not.toContain(id)
  })

  test('rejects a unit that is no longer active and releases the claim that got it started', async ({
    qx,
  }) => {
    const integrationId = await createIntegration(qx)
    const id = await createSyncUnit(qx, integrationId, { channelName: 'repo', syncName: 'issues' })
    await setClaimState(qx, id, { lockedAgo: '10 seconds', nextRunAgo: '1 minute' })
    await qx.result(`UPDATE integration.sync_units SET status = 'paused' WHERE id = $(id)`, { id })

    expect(await reassertUnitClaim(qx, id)).toBe(false)
    expect((await readClaimState(qx, id)).lockedAgoSeconds).toBeNull()
  })
})

describe('takeOverSyncUnit', () => {
  test('flips the unit to emit, rewinds a since walk, keeps a cursor walk, and drops every shadow row', async ({
    qx,
  }) => {
    const integrationId = await createIntegration(qx)
    const sinceWalk = await createSyncUnit(qx, integrationId, {
      channelName: 'repo',
      syncName: 'issues',
      watermark: {
        phase: 'incremental',
        since: '2026-10-19T09:00:00Z',
        cursor: 'c1',
        coveredWindows: [{ confirmedThrough: 'a', coveredUntil: 'b' }],
      },
    })
    const cursorWalk = await createSyncUnit(qx, integrationId, {
      channelName: 'repo',
      syncName: 'forks',
      watermark: { phase: 'incremental', since: '2026-10-19T09:00:00Z', cursor: 'c2' },
    })
    await recordShadowRecords(qx, sinceWalk, [
      {
        type: 'issues-opened',
        sourceId: 's1',
        occurredAt: '2026-10-19T09:30:00Z',
        data: {},
        segmentId: generateUUIDv1(),
        integrationId,
      },
    ])

    expect(await takeOverSyncUnit(qx, sinceWalk, '2026-10-19T08:00:00.000Z')).toBe(1)
    expect(await takeOverSyncUnit(qx, cursorWalk, null)).toBe(1)
    expect(await takeOverSyncUnit(qx, sinceWalk, '2026-10-19T07:00:00.000Z')).toBe(0)

    const units = await qx.select(
      `SELECT id, "emitEnabled", watermark
       FROM integration.sync_units
       WHERE id IN ($(sinceWalk), $(cursorWalk))
       ORDER BY "syncName"`,
      { sinceWalk, cursorWalk },
    )
    expect(units).toEqual([
      {
        id: cursorWalk,
        emitEnabled: true,
        watermark: { phase: 'incremental', since: '2026-10-19T09:00:00Z', cursor: 'c2' },
      },
      {
        id: sinceWalk,
        emitEnabled: true,
        watermark: {
          phase: 'incremental',
          since: '2026-10-19T08:00:00.000Z',
          cursor: null,
          coveredWindows: [],
        },
      },
    ])
    expect(
      await qx.selectOne(
        `SELECT count(*)::int AS n FROM integration.sync_shadow_records WHERE "unitId" = $(id)`,
        { id: sinceWalk },
      ),
    ).toEqual({ n: 0 })
  })
})

describe('sync unit lifecycle', () => {
  test('decommissions units per channel or whole integration, counts fresh locks as undrained, deletes drained units and cascades shadow records and diff summaries', async ({
    qx,
  }) => {
    const integrationId = await createIntegration(qx)
    const active = await createSyncUnit(qx, integrationId, {
      channelName: 'https://github.com/acme/a',
      syncName: 'issues',
    })
    const freshLock = await createSyncUnit(qx, integrationId, {
      channelName: 'https://github.com/acme/a',
      syncName: 'forks',
      emitEnabled: true,
      lockedAt: new Date().toISOString(),
    })
    const staleLock = await createSyncUnit(qx, integrationId, {
      channelName: 'https://github.com/Acme/A',
      syncName: 'discussions',
      lockedAt: new Date(Date.now() - 2 * 3_600_000).toISOString(),
    })
    const deadLetter = await createSyncUnit(qx, integrationId, {
      channelName: 'https://github.com/acme/a',
      syncName: 'pull-requests',
      status: 'dead_letter',
    })
    const switchPaused = await createSyncUnit(qx, integrationId, {
      channelName: 'https://github.com/acme/p',
      syncName: 'releases',
      status: 'paused',
    })
    const untouched = await createSyncUnit(qx, integrationId, {
      channelName: 'https://github.com/acme/b',
      syncName: 'issues',
      emitEnabled: true,
    })
    await recordShadowRecords(qx, active, [
      {
        type: 'issues-opened',
        sourceId: 's1',
        occurredAt: '2026-10-19T09:30:00Z',
        data: {},
        segmentId: generateUUIDv1(),
        integrationId,
      },
    ])
    await qx.result(
      `INSERT INTO integration.sync_diff_summary ("unitId", day, "integrationId", "channelName")
       VALUES ($(id), '2026-10-20', $(integrationId), 'https://github.com/acme/a')`,
      { id: active, integrationId },
    )
    const staleLockBefore = new Date(Date.now() - 3_600_000)
    const channelA = 'https://github.com/acme/a'
    const stranger = await createIntegration(qx)

    const countChildren = async (table: string): Promise<{ n: number }> =>
      qx.selectOne(`SELECT count(*)::int AS n FROM integration.${table} WHERE "unitId" = $(id)`, {
        id: active,
      })
    const statusOf = async (id: string): Promise<string> =>
      (await qx.selectOne(`SELECT status FROM integration.sync_units WHERE id = $(id)`, { id }))
        .status
    expect(await countChildren('sync_shadow_records')).toEqual({ n: 1 })
    expect(await countChildren('sync_diff_summary')).toEqual({ n: 1 })

    expect(await deleteDrainedSyncUnits(qx, integrationId, staleLockBefore)).toBe(0)
    expect(await decommissionSyncUnitsForChannels(qx, integrationId, [])).toBe(0)
    expect(await decommissionSyncUnitsForChannels(qx, stranger, [channelA])).toBe(0)
    expect(await decommissionSyncUnitsForChannels(qx, integrationId, [channelA])).toBe(4)
    expect(await decommissionSyncUnitsForChannels(qx, integrationId, [channelA])).toBe(0)
    expect(await statusOf(switchPaused)).toBe('paused')
    expect(await statusOf(untouched)).toBe('active')

    expect(await countUndrainedDecommissionedSyncUnits(qx, integrationId, staleLockBefore)).toBe(1)
    expect(await countUndrainedDecommissionedSyncUnits(qx, stranger, staleLockBefore)).toBe(0)
    expect(await deleteDrainedSyncUnits(qx, stranger, staleLockBefore)).toBe(0)
    expect(await deleteDrainedSyncUnits(qx, integrationId, staleLockBefore)).toBe(3)
    expect(await listDecommissionedSyncUnitChannels(qx, integrationId)).toEqual([channelA])
    expect(await listDecommissionedSyncUnitChannels(qx, stranger)).toEqual([])

    const channels = await listSyncUnitChannelsForIntegration(qx, integrationId)
    expect(channels.map((c) => ({ ...c, channelName: c.channelName.toLowerCase() }))).toEqual([
      { channelName: channelA, hasDecommissioned: true, hasEmitting: false },
      { channelName: 'https://github.com/acme/b', hasDecommissioned: false, hasEmitting: true },
      { channelName: 'https://github.com/acme/p', hasDecommissioned: false, hasEmitting: false },
    ])
    expect(await statusOf(freshLock)).toBe('decommissioned')
    for (const id of [active, staleLock, deadLetter]) {
      expect(
        await qx.selectOne(
          `SELECT count(*)::int AS n FROM integration.sync_units WHERE id = $(id)`,
          { id },
        ),
      ).toEqual({ n: 0 })
    }
    expect(await countChildren('sync_shadow_records')).toEqual({ n: 0 })
    expect(await countChildren('sync_diff_summary')).toEqual({ n: 0 })

    expect(await decommissionAllSyncUnits(qx, integrationId)).toBe(2)
    expect(await decommissionAllSyncUnits(qx, stranger)).toBe(0)
    expect(await statusOf(switchPaused)).toBe('decommissioned')
    expect(await statusOf(untouched)).toBe('decommissioned')

    await qx.result(
      `UPDATE integration.sync_units SET "consecutiveFailures" = 4, "lockedAt" = now() WHERE id = $(id)`,
      { id: untouched },
    )
    await recordRunFailure(qx, untouched, 'provider.auth', null, 5, new Date())
    expect(
      await qx.selectOne(`SELECT status, "lockedAt" FROM integration.sync_units WHERE id = $(id)`, {
        id: untouched,
      }),
    ).toEqual({ status: 'decommissioned', lockedAt: null })
  })
})

describe('lockSyncUnitChannel', () => {
  test('takes a transaction-scoped advisory lock keyed by the channel', async ({ qx }) => {
    const advisoryLocks = async (inner: QueryExecutor): Promise<number> =>
      (
        await inner.selectOne(
          `SELECT count(*)::int AS n FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid()`,
        )
      ).n
    const before = await advisoryLocks(qx)
    await qx.tx(async (tx) => {
      await lockSyncUnitChannel(tx, 'R_lock')
      expect(await advisoryLocks(tx)).toBe(before + 1)
    })
  })
})

describe('alignSyncUnitChannelName', () => {
  test('rewrites only the units of the channel whose spelling differs', async ({ qx }) => {
    const integrationId = await createIntegration(qx)
    const other = await createIntegration(qx)
    const stale = await createSyncUnit(qx, integrationId, {
      channelName: 'https://github.com/org/tag-security',
      syncName: 'issues',
    })
    await createSyncUnit(qx, integrationId, {
      channelName: 'https://github.com/org/TAG-security',
      syncName: 'forks',
    })
    const elsewhere = await createSyncUnit(qx, other, {
      channelName: 'https://github.com/org/tag-security',
      syncName: 'issues',
    })
    const nameOf = async (id: string): Promise<string> =>
      (
        await qx.selectOne(`SELECT "channelName" FROM integration.sync_units WHERE id = $(id)`, {
          id,
        })
      ).channelName

    await qx.result(`UPDATE integration.sync_units SET "lockedAt" = now() WHERE id = $(id)`, {
      id: stale,
    })
    await expect(
      alignSyncUnitChannelName(
        qx,
        integrationId,
        'https://github.com/org/tag-security',
        'https://github.com/org/TAG-security',
      ),
    ).resolves.toBe(0)
    expect(await nameOf(stale)).toBe('https://github.com/org/tag-security')
    await qx.result(`UPDATE integration.sync_units SET "lockedAt" = NULL WHERE id = $(id)`, {
      id: stale,
    })
    await expect(
      alignSyncUnitChannelName(
        qx,
        integrationId,
        'https://github.com/org/tag-security',
        'https://github.com/org/TAG-security',
      ),
    ).resolves.toBe(1)
    expect(await nameOf(stale)).toBe('https://github.com/org/TAG-security')
    expect(await nameOf(elsewhere)).toBe('https://github.com/org/tag-security')
    await expect(
      alignSyncUnitChannelName(
        qx,
        integrationId,
        'https://github.com/org/tag-security',
        'https://github.com/org/TAG-security',
      ),
    ).resolves.toBe(0)
  })
})

describe('recordRunTimeout', () => {
  async function setRunState(
    qx: QueryExecutor,
    id: string,
    state: { lastRunAt: string; consecutiveFailures: number; nextRunAt: string },
  ) {
    await qx.result(
      `UPDATE integration.sync_units
       SET "lastRunAt" = $(lastRunAt)::timestamptz,
           "consecutiveFailures" = $(consecutiveFailures),
           "nextRunAt" = $(nextRunAt)::timestamptz
       WHERE id = $(id)`,
      { id, ...state },
    )
  }

  async function readRun(qx: QueryExecutor, id: string) {
    return qx.selectOne(
      `SELECT status, "lockedAt", "nextRunAt", "consecutiveFailures", "lastErrorClass",
              "lastErrorMessage", "lastRunComplete", "lastRunAt"
       FROM integration.sync_units WHERE id = $(id)`,
      { id },
    )
  }

  const NEXT_RUN_AT = '2026-10-26T12:00:00.000Z'
  const LOCKED_AT = '2026-10-26T10:00:00.000Z'
  const minutesAgo = (minutes: number) => new Date(Date.now() - minutes * 60_000)

  test('records the timeout as a failure without releasing the claim', async ({ qx }) => {
    const integrationId = await createIntegration(qx)
    const id = await createSyncUnit(qx, integrationId, {
      channelName: 'https://github.com/o/r',
      syncName: 'issues',
      lockedAt: LOCKED_AT,
    })
    await setRunState(qx, id, {
      lastRunAt: minutesAgo(60).toISOString(),
      consecutiveFailures: 2,
      nextRunAt: NEXT_RUN_AT,
    })

    expect(await recordRunTimeout(qx, id, 'sync run timed out (HEARTBEAT)', minutesAgo(10))).toBe(1)

    const run = await readRun(qx, id)
    expect(new Date(run.lockedAt).toISOString()).toBe(LOCKED_AT)
    expect(new Date(run.nextRunAt).toISOString()).toBe(NEXT_RUN_AT)
    expect(run).toMatchObject({
      status: 'active',
      consecutiveFailures: 3,
      lastErrorClass: 'worker.timeout',
      lastErrorMessage: 'sync run timed out (HEARTBEAT)',
      lastRunComplete: false,
    })
  })

  test('leaves a unit whose attempt already recorded its own outcome', async ({ qx }) => {
    const integrationId = await createIntegration(qx)
    const id = await createSyncUnit(qx, integrationId, {
      channelName: 'https://github.com/o/r',
      syncName: 'issues',
    })
    await setRunState(qx, id, {
      lastRunAt: new Date().toISOString(),
      consecutiveFailures: 0,
      nextRunAt: NEXT_RUN_AT,
    })
    const before = await readRun(qx, id)

    expect(await recordRunTimeout(qx, id, 'sync run timed out (HEARTBEAT)', minutesAgo(10))).toBe(0)

    expect(await readRun(qx, id)).toEqual(before)
  })

  test('never dead-letters a unit', async ({ qx }) => {
    const integrationId = await createIntegration(qx)
    const id = await createSyncUnit(qx, integrationId, {
      channelName: 'https://github.com/o/r',
      syncName: 'issues',
    })
    await setRunState(qx, id, {
      lastRunAt: minutesAgo(60).toISOString(),
      consecutiveFailures: 50,
      nextRunAt: NEXT_RUN_AT,
    })

    await recordRunTimeout(qx, id, 'sync run timed out (START_TO_CLOSE)', minutesAgo(10))

    expect((await readRun(qx, id)).status).toBe('active')
  })
})
