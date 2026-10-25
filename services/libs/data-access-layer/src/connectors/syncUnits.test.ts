import { test as base, describe, expect } from 'vitest'

import { DEFAULT_TENANT_ID, generateUUIDv1 } from '@crowd/common'
import type { QueryExecutor } from '@crowd/database'
import { withQx } from '@crowd/test-kit/db'

import { recordShadowRecords } from './shadowRecords'
import {
  claimDueUnits,
  countShadowRepos,
  hasSyncUnitsForChannel,
  listShadowDiffUnits,
  listShadowDiffUnitsPage,
  reassertUnitClaim,
  renewRunningUnitLease,
  reviveDeadLetteredSyncUnits,
  takeOverSyncUnit,
  upsertSyncUnits,
} from './syncUnits'

const test = withQx(base)

async function createIntegration(
  qx: QueryExecutor,
  overrides: { deletedAt?: string } = {},
): Promise<string> {
  const id = generateUUIDv1()
  await qx.result(
    `
    INSERT INTO public.integrations (id, platform, status, "tenantId", "createdAt", "updatedAt", "deletedAt")
    VALUES ($(id), 'github', 'done', $(tenantId), NOW(), NOW(), $(deletedAt)::timestamptz)
    `,
    { id, tenantId: DEFAULT_TENANT_ID, deletedAt: overrides.deletedAt ?? null },
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
  },
): Promise<string> {
  const id = generateUUIDv1()
  await qx.result(
    `
    INSERT INTO integration.sync_units
      (id, "integrationId", platform, "channelId", "channelName", "syncName", status, "emitEnabled", watermark)
    VALUES
      ($(id), $(integrationId), $(platform), $(channelName), $(channelName), $(syncName), $(status), $(emitEnabled), $(watermark)::jsonb)
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
