import { test as base, describe, expect } from 'vitest'

import { DEFAULT_TENANT_ID, generateUUIDv1 } from '@crowd/common'
import type { QueryExecutor } from '@crowd/database'
import { withQx } from '@crowd/test-kit/db'

import {
  listOctolensMentionPollUnits,
  queueOctolensKeywordBackfills,
  rewindOctolensMentionPollStartDate,
} from './index'

const test = withQx(base)

async function createAnchorIntegration(
  qx: QueryExecutor,
  overrides: { deletedAt?: string } = {},
): Promise<string> {
  const id = generateUUIDv1()
  await qx.result(
    `
    INSERT INTO public.integrations (id, platform, status, "tenantId", "createdAt", "updatedAt", "deletedAt")
    VALUES ($(id), 'octolens', 'done', $(tenantId), NOW(), NOW(), $(deletedAt)::timestamptz)
    `,
    { id, tenantId: DEFAULT_TENANT_ID, deletedAt: overrides.deletedAt ?? null },
  )
  return id
}

async function createMentionPollUnit(
  qx: QueryExecutor,
  integrationId: string,
  overrides: {
    status?: string
    locked?: boolean
    watermark?: Record<string, unknown> | null
  } = {},
): Promise<string> {
  const id = generateUUIDv1()
  await qx.result(
    `
    INSERT INTO integration.sync_units
      (id, "integrationId", platform, "channelId", "channelName", "syncName", status, "emitEnabled", watermark, "lockedAt")
    VALUES
      ($(id), $(integrationId), 'octolens', 'octolens-mentions', 'octolens-mentions', 'mentionPoll',
       $(status), true, $(watermark)::jsonb, CASE WHEN $(locked) THEN now() ELSE NULL END)
    `,
    {
      id,
      integrationId,
      status: overrides.status ?? 'active',
      locked: overrides.locked ?? false,
      watermark: overrides.watermark == null ? null : JSON.stringify(overrides.watermark),
    },
  )
  return id
}

async function readWatermark(qx: QueryExecutor, unitId: string): Promise<unknown> {
  const row: { watermark: unknown } = await qx.selectOne(
    `SELECT watermark FROM integration.sync_units WHERE id = $(unitId)`,
    { unitId },
  )
  return row.watermark
}

const MID_CYCLE = {
  startDate: '2026-10-16T10:00:00.000Z',
  cursor: 'c-2',
  pendingMaxTimestamp: '2026-10-16T10:04:00.000Z',
  backfill: [{ keywordId: 7, cursor: 'k7-page3' }],
}

describe('listOctolensMentionPollUnits', () => {
  test('returns only mention-poll units of live segment-less octolens integrations', async ({
    qx,
  }) => {
    const anchorId = await createAnchorIntegration(qx)
    const unitId = await createMentionPollUnit(qx, anchorId, { status: 'dead_letter' })
    const deletedAnchorId = await createAnchorIntegration(qx, { deletedAt: '2026-01-01' })
    await createMentionPollUnit(qx, deletedAnchorId)

    const units = await listOctolensMentionPollUnits(qx)
    expect(units.map((u) => [u.id, u.integrationId, u.status])).toEqual([
      [unitId, anchorId, 'dead_letter'],
    ])
  })
})

describe('queueOctolensKeywordBackfills', () => {
  test('appends new keywords after the existing queue and skips already-queued ones', async ({
    qx,
  }) => {
    const unitId = await createMentionPollUnit(qx, await createAnchorIntegration(qx), {
      watermark: MID_CYCLE,
    })

    const result = await queueOctolensKeywordBackfills(qx, unitId, [9, 7, 8, 9])

    expect(result.outcome).toBe('applied')
    expect(await readWatermark(qx, unitId)).toEqual({
      ...MID_CYCLE,
      backfill: [
        { keywordId: 7, cursor: 'k7-page3' },
        { keywordId: 9, cursor: null },
        { keywordId: 8, cursor: null },
      ],
    })
  })

  test('queues a keyword whose only entry is one the poll would drop', async ({ qx }) => {
    const malformed = [{ keywordId: 7 }, { keywordId: 8, cursor: 42 }]
    const unitId = await createMentionPollUnit(qx, await createAnchorIntegration(qx), {
      watermark: { ...MID_CYCLE, backfill: malformed },
    })

    const result = await queueOctolensKeywordBackfills(qx, unitId, [7, 8])

    expect(result.outcome).toBe('applied')
    expect(await readWatermark(qx, unitId)).toMatchObject({
      backfill: [...malformed, { keywordId: 7, cursor: null }, { keywordId: 8, cursor: null }],
    })
  })

  test('creates the queue on a unit that has never run', async ({ qx }) => {
    const unitId = await createMentionPollUnit(qx, await createAnchorIntegration(qx))

    await queueOctolensKeywordBackfills(qx, unitId, [5])

    expect(await readWatermark(qx, unitId)).toEqual({ backfill: [{ keywordId: 5, cursor: null }] })
  })

  test('adds the queue to a stored watermark without a backfill key', async ({ qx }) => {
    const poll = {
      startDate: MID_CYCLE.startDate,
      cursor: MID_CYCLE.cursor,
      pendingMaxTimestamp: MID_CYCLE.pendingMaxTimestamp,
    }
    const unitId = await createMentionPollUnit(qx, await createAnchorIntegration(qx), {
      watermark: poll,
    })

    await queueOctolensKeywordBackfills(qx, unitId, [5])

    expect(await readWatermark(qx, unitId)).toEqual({
      ...poll,
      backfill: [{ keywordId: 5, cursor: null }],
    })
  })

  test('replaces a backfill value that is not an array', async ({ qx }) => {
    const unitId = await createMentionPollUnit(qx, await createAnchorIntegration(qx), {
      watermark: { ...MID_CYCLE, backfill: { keywordId: 7 } },
    })

    await queueOctolensKeywordBackfills(qx, unitId, [5])

    expect(await readWatermark(qx, unitId)).toEqual({
      ...MID_CYCLE,
      backfill: [{ keywordId: 5, cursor: null }],
    })
  })

  test('refuses and writes nothing while the unit is claimed', async ({ qx }) => {
    const unitId = await createMentionPollUnit(qx, await createAnchorIntegration(qx), {
      locked: true,
      watermark: MID_CYCLE,
    })

    const result = await queueOctolensKeywordBackfills(qx, unitId, [9])

    expect(result.outcome).toBe('locked')
    expect(await readWatermark(qx, unitId)).toEqual(MID_CYCLE)
  })

  test('refuses a unit that is not active', async ({ qx }) => {
    const unitId = await createMentionPollUnit(qx, await createAnchorIntegration(qx), {
      status: 'dead_letter',
      watermark: MID_CYCLE,
    })

    const result = await queueOctolensKeywordBackfills(qx, unitId, [9])

    expect(result.outcome).toBe('inactive')
    expect(await readWatermark(qx, unitId)).toEqual(MID_CYCLE)
  })

  test('reports a unit id that is not a mention-poll unit as missing', async ({ qx }) => {
    const result = await queueOctolensKeywordBackfills(qx, generateUUIDv1(), [9])

    expect(result.outcome).toBe('missing')
  })
})

describe('rewindOctolensMentionPollStartDate', () => {
  test('sets the start date, clears the cycle progress and keeps the backfill queue', async ({
    qx,
  }) => {
    const unitId = await createMentionPollUnit(qx, await createAnchorIntegration(qx), {
      watermark: MID_CYCLE,
    })

    const result = await rewindOctolensMentionPollStartDate(
      qx,
      unitId,
      '2026-09-01T00:00:00.000Z',
      MID_CYCLE.startDate,
    )

    expect(result.outcome).toBe('applied')
    expect(await readWatermark(qx, unitId)).toEqual({
      startDate: '2026-09-01T00:00:00.000Z',
      cursor: null,
      pendingMaxTimestamp: null,
      backfill: MID_CYCLE.backfill,
    })
  })

  test('refuses when the start date changed since the rewind was planned', async ({ qx }) => {
    const unitId = await createMentionPollUnit(qx, await createAnchorIntegration(qx), {
      watermark: MID_CYCLE,
    })
    const first = await rewindOctolensMentionPollStartDate(
      qx,
      unitId,
      '2026-08-01T00:00:00.000Z',
      MID_CYCLE.startDate,
    )

    const second = await rewindOctolensMentionPollStartDate(
      qx,
      unitId,
      '2026-09-01T00:00:00.000Z',
      MID_CYCLE.startDate,
    )

    expect(first.outcome).toBe('applied')
    expect(second.outcome).toBe('changed')
    expect(await readWatermark(qx, unitId)).toMatchObject({ startDate: '2026-08-01T00:00:00.000Z' })
  })

  test('rewinds a unit that has never run when it still has no start date', async ({ qx }) => {
    const unitId = await createMentionPollUnit(qx, await createAnchorIntegration(qx))

    const result = await rewindOctolensMentionPollStartDate(
      qx,
      unitId,
      '2026-09-01T00:00:00.000Z',
      null,
    )

    expect(result.outcome).toBe('applied')
    expect(await readWatermark(qx, unitId)).toMatchObject({ startDate: '2026-09-01T00:00:00.000Z' })
  })

  test('refuses and writes nothing while the unit is claimed', async ({ qx }) => {
    const unitId = await createMentionPollUnit(qx, await createAnchorIntegration(qx), {
      locked: true,
      watermark: MID_CYCLE,
    })

    const result = await rewindOctolensMentionPollStartDate(
      qx,
      unitId,
      '2026-09-01T00:00:00.000Z',
      MID_CYCLE.startDate,
    )

    expect(result.outcome).toBe('locked')
    expect(await readWatermark(qx, unitId)).toEqual(MID_CYCLE)
  })

  test('refuses a unit that is not active', async ({ qx }) => {
    const unitId = await createMentionPollUnit(qx, await createAnchorIntegration(qx), {
      status: 'paused',
      watermark: MID_CYCLE,
    })

    const result = await rewindOctolensMentionPollStartDate(
      qx,
      unitId,
      '2026-09-01T00:00:00.000Z',
      MID_CYCLE.startDate,
    )

    expect(result.outcome).toBe('inactive')
    expect(await readWatermark(qx, unitId)).toEqual(MID_CYCLE)
  })
})
