import { DEFAULT_TENANT_ID, generateUUIDv4 } from '@crowd/common'
import { IntegrationState, PlatformType } from '@crowd/types'

import { QueryExecutor } from '../queryExecutor'

export interface IUpsertOctolensKeywordMapping {
  integrationId: string
  segmentId: string
  keywordId: number
  keyword: string
}

// lastSeenAt moves on every call so gaps in discovery show up; updatedAt only on a real change.
export async function upsertOctolensKeywordMapping(
  qx: QueryExecutor,
  data: IUpsertOctolensKeywordMapping,
): Promise<void> {
  await qx.result(
    `
      INSERT INTO integration."octolensKeywordMappings" AS m
        ("integrationId", "segmentId", "keywordId", keyword, "lastSeenAt")
      VALUES ($(integrationId), $(segmentId), $(keywordId), $(keyword), now())
      ON CONFLICT ("integrationId", "keywordId") DO UPDATE
      SET "segmentId" = EXCLUDED."segmentId",
          keyword = EXCLUDED.keyword,
          "deactivatedAt" = NULL,
          "lastSeenAt" = now(),
          "updatedAt" = CASE
            WHEN m."segmentId" IS DISTINCT FROM EXCLUDED."segmentId"
              OR m.keyword IS DISTINCT FROM EXCLUDED.keyword
              OR m."deactivatedAt" IS NOT NULL
            THEN now()
            ELSE m."updatedAt"
          END
    `,
    data,
  )
}

export interface IFoundOctolensIntegration {
  id: string
  created: boolean
  restored: boolean
}

export interface IActiveOctolensKeywordMapping {
  keywordId: number
  keyword: string
  integrationId: string
  segmentId: string
}

export interface IOctolensKeywordMappingRef {
  integrationId: string
  keywordId: number
}

// An integration owning active sync units (the anchor, or a legacy one) is never a subproject integration.
export const OWNS_NO_ACTIVE_SYNC_UNITS = `NOT EXISTS (
  SELECT 1 FROM integration.sync_units su WHERE su."integrationId" = i.id AND su.status = 'active'
)`

// integrations has no unique key on (platform, segmentId), so every writer of
// Octolens integration rows serializes on this lock to avoid duplicates.
async function lockOctolensIntegrations(tx: QueryExecutor): Promise<void> {
  await tx.select(`SELECT pg_advisory_xact_lock(hashtextextended('octolensIntegration', 0))`)
}

async function insertOctolensIntegration(
  tx: QueryExecutor,
  segmentId: string | null,
): Promise<string> {
  const id = generateUUIDv4()
  await tx.result(
    `
      INSERT INTO public.integrations
        (id, platform, status, "segmentId", "tenantId", "createdAt", "updatedAt")
      VALUES ($(id), $(platform), $(status), $(segmentId), $(tenantId), now(), now())
    `,
    {
      id,
      platform: PlatformType.OCTOLENS,
      status: IntegrationState.DONE,
      segmentId,
      tenantId: DEFAULT_TENANT_ID,
    },
  )
  return id
}

async function findOrCreateOctolensIntegration(
  qx: QueryExecutor,
  segmentId: string | null,
): Promise<IFoundOctolensIntegration> {
  return qx.tx(async (tx) => {
    await lockOctolensIntegrations(tx)

    const rows: { id: string; deletedAt: Date | null }[] = await tx.select(
      `
        SELECT i.id, i."deletedAt"
        FROM public.integrations i
        WHERE i.platform = $(platform)
          AND i."segmentId" IS NOT DISTINCT FROM $(segmentId)::uuid
          AND ($(segmentId)::uuid IS NULL OR ${OWNS_NO_ACTIVE_SYNC_UNITS})
        ORDER BY i."updatedAt" DESC, i."createdAt" DESC, i.id
      `,
      { platform: PlatformType.OCTOLENS, segmentId },
    )

    const live = rows.filter((r) => r.deletedAt === null)
    if (live.length > 1) {
      throw new Error(
        `Found ${live.length} live Octolens integrations for segment ${segmentId ?? 'NULL'}!`,
      )
    }
    if (live.length === 1) {
      return { id: live[0].id, created: false, restored: false }
    }

    if (rows.length > 0) {
      const { id } = rows[0]
      await tx.result(
        `
          UPDATE public.integrations
          SET "deletedAt" = NULL, "updatedAt" = now()
          WHERE id = $(id)
        `,
        { id },
      )
      return { id, created: false, restored: true }
    }

    const id = await insertOctolensIntegration(tx, segmentId)
    return { id, created: true, restored: false }
  })
}

export async function findOrCreateOctolensSubprojectIntegration(
  qx: QueryExecutor,
  segmentId: string,
): Promise<IFoundOctolensIntegration> {
  return findOrCreateOctolensIntegration(qx, segmentId)
}

export async function findOrCreateOctolensAnchorIntegration(
  qx: QueryExecutor,
): Promise<IFoundOctolensIntegration> {
  return findOrCreateOctolensIntegration(qx, null)
}

export async function listActiveOctolensKeywordMappings(
  qx: QueryExecutor,
): Promise<IActiveOctolensKeywordMapping[]> {
  return qx.select(
    `
      SELECT m."keywordId", m.keyword, m."integrationId", m."segmentId"
      FROM integration."octolensKeywordMappings" m
      JOIN public.integrations i ON i.id = m."integrationId"
      WHERE m."deactivatedAt" IS NULL
        AND i.platform = $(platform)
        AND i."deletedAt" IS NULL
        AND i."segmentId" IS NOT NULL
        AND ${OWNS_NO_ACTIVE_SYNC_UNITS}
      ORDER BY m."keywordId", m."integrationId"
    `,
    { platform: PlatformType.OCTOLENS },
  )
}

// An empty set deactivates nothing: a run that saw no mappings must never wipe them all.
export async function deactivateOctolensKeywordMappingsNotSeenAcross(
  qx: QueryExecutor,
  seen: IOctolensKeywordMappingRef[],
): Promise<number> {
  if (seen.length === 0) {
    return 0
  }

  return qx.result(
    `
      UPDATE integration."octolensKeywordMappings" m
      SET "deactivatedAt" = now(), "updatedAt" = now()
      FROM public.integrations i
      WHERE i.id = m."integrationId"
        AND i.platform = $(platform)
        AND i."deletedAt" IS NULL
        AND i."segmentId" IS NOT NULL
        AND ${OWNS_NO_ACTIVE_SYNC_UNITS}
        AND m."deactivatedAt" IS NULL
        AND NOT EXISTS (
          SELECT 1
          FROM unnest($(integrationIds)::uuid[], $(keywordIds)::int[]) AS s("integrationId", "keywordId")
          WHERE s."integrationId" = m."integrationId" AND s."keywordId" = m."keywordId"
        )
    `,
    {
      platform: PlatformType.OCTOLENS,
      integrationIds: seen.map((s) => s.integrationId),
      keywordIds: seen.map((s) => s.keywordId),
    },
  )
}

// Run only after the same discovery run's mapping upserts and deactivation, and never
// concurrently with another discovery run, or it deletes integrations that are still mapped.
export async function softDeleteOctolensSubprojectIntegrationsWithoutActiveMappings(
  qx: QueryExecutor,
): Promise<string[]> {
  return qx.tx(async (tx) => {
    await lockOctolensIntegrations(tx)

    const rows: { id: string }[] = await tx.select(
      `
        UPDATE public.integrations i
        SET "deletedAt" = now(), "updatedAt" = now()
        WHERE i.platform = $(platform)
          AND i."deletedAt" IS NULL
          AND i."segmentId" IS NOT NULL
          AND ${OWNS_NO_ACTIVE_SYNC_UNITS}
          AND NOT EXISTS (
            SELECT 1
            FROM integration."octolensKeywordMappings" m
            WHERE m."integrationId" = i.id AND m."deactivatedAt" IS NULL
          )
        RETURNING i.id
      `,
      { platform: PlatformType.OCTOLENS },
    )
    return rows.map((r) => r.id)
  })
}

export interface IOctolensKeywordMappingInput {
  keywordId: number
  keyword: string
  segmentId: string
}

export interface IOctolensReconcileOutcome {
  deactivated: number
  created: string[]
  restored: string[]
  softDeleted: string[]
}

// One transaction, so a poll never sees a moving keyword active under two subprojects, and the
// soft-delete (last) never drops an integration this run just mapped.
export async function reconcileOctolensKeywordMappings(
  qx: QueryExecutor,
  mappings: IOctolensKeywordMappingInput[],
): Promise<IOctolensReconcileOutcome> {
  return qx.tx(async (tx) => {
    const integrationBySegment = new Map<string, string>()
    const seen: IOctolensKeywordMappingRef[] = []
    const created: string[] = []
    const restored: string[] = []
    for (const mapping of mappings) {
      let integrationId = integrationBySegment.get(mapping.segmentId)
      if (!integrationId) {
        const integration = await findOrCreateOctolensSubprojectIntegration(tx, mapping.segmentId)
        if (integration.created) created.push(integration.id)
        if (integration.restored) restored.push(integration.id)
        integrationId = integration.id
        integrationBySegment.set(mapping.segmentId, integrationId)
      }
      await upsertOctolensKeywordMapping(tx, { integrationId, ...mapping })
      seen.push({ integrationId, keywordId: mapping.keywordId })
    }

    const deactivated = await deactivateOctolensKeywordMappingsNotSeenAcross(tx, seen)
    const softDeleted = await softDeleteOctolensSubprojectIntegrationsWithoutActiveMappings(tx)

    return { deactivated, created, restored, softDeleted }
  })
}

const MENTION_POLL_SYNC_NAME = 'mentionPoll'

export interface IOctolensMentionPollUnit {
  id: string
  integrationId: string
  status: string
  lockedAt: Date | null
  watermark: Record<string, unknown> | null
}

// 'changed': the unit was idle but its watermark no longer matched what the write was planned on.
export type OctolensMentionPollWriteOutcome =
  | 'applied'
  | 'locked'
  | 'changed'
  | 'inactive'
  | 'missing'

export interface IOctolensMentionPollWriteResult {
  outcome: OctolensMentionPollWriteOutcome
  watermark: Record<string, unknown> | null
}

export async function listOctolensMentionPollUnits(
  qx: QueryExecutor,
): Promise<IOctolensMentionPollUnit[]> {
  return qx.select(
    `
      SELECT su.id, su."integrationId", su.status, su."lockedAt", su.watermark
      FROM integration.sync_units su
      JOIN public.integrations i ON i.id = su."integrationId"
      WHERE su.platform = $(platform)
        AND su."syncName" = $(syncName)
        AND i.platform = $(platform)
        AND i."segmentId" IS NULL
        AND i."deletedAt" IS NULL
      ORDER BY su.id
    `,
    { platform: PlatformType.OCTOLENS, syncName: MENTION_POLL_SYNC_NAME },
  )
}

const STORED_WATERMARK = `(CASE WHEN jsonb_typeof(su.watermark) = 'object' THEN su.watermark ELSE '{}'::jsonb END)`

const STORED_BACKFILL = `(CASE WHEN jsonb_typeof(su.watermark->'backfill') = 'array' THEN su.watermark->'backfill' ELSE '[]'::jsonb END)`

// A run keeps the watermark in memory and overwrites it when it ends, so this only writes while
// the unit is unclaimed; the dispatcher keeps lockedAt set until the run records its end.
async function updateIdleMentionPollWatermark(
  qx: QueryExecutor,
  unitId: string,
  watermarkSql: string,
  params: Record<string, unknown>,
  guardSql = 'true',
): Promise<IOctolensMentionPollWriteResult> {
  const filter = { unitId, platform: PlatformType.OCTOLENS, syncName: MENTION_POLL_SYNC_NAME }
  const updated: { watermark: Record<string, unknown> }[] = await qx.select(
    `
      UPDATE integration.sync_units su
      SET watermark = ${watermarkSql}, "updatedAt" = now()
      WHERE su.id = $(unitId)
        AND su.platform = $(platform)
        AND su."syncName" = $(syncName)
        AND su.status = 'active'
        AND su."lockedAt" IS NULL
        AND ${guardSql}
      RETURNING su.watermark
    `,
    { ...params, ...filter },
  )
  if (updated.length > 0) {
    return { outcome: 'applied', watermark: updated[0].watermark }
  }

  const unit: {
    status: string
    lockedAt: Date | null
    watermark: Record<string, unknown> | null
  } | null = await qx.selectOneOrNone(
    `
        SELECT su.status, su."lockedAt", su.watermark
        FROM integration.sync_units su
        WHERE su.id = $(unitId) AND su.platform = $(platform) AND su."syncName" = $(syncName)
      `,
    filter,
  )
  if (!unit) {
    return { outcome: 'missing', watermark: null }
  }
  if (unit.status !== 'active') {
    return { outcome: 'inactive', watermark: unit.watermark }
  }
  return { outcome: unit.lockedAt ? 'locked' : 'changed', watermark: unit.watermark }
}

export async function queueOctolensKeywordBackfills(
  qx: QueryExecutor,
  unitId: string,
  keywordIds: number[],
): Promise<IOctolensMentionPollWriteResult> {
  return updateIdleMentionPollWatermark(
    qx,
    unitId,
    `jsonb_set(
      ${STORED_WATERMARK},
      '{backfill}',
      ${STORED_BACKFILL} || COALESCE(
        (
          SELECT jsonb_agg(jsonb_build_object('keywordId', k.id, 'cursor', NULL) ORDER BY k.ord)
          FROM unnest($(keywordIds)::int[]) WITH ORDINALITY AS k(id, ord)
          WHERE NOT EXISTS (
            SELECT 1 FROM jsonb_array_elements(${STORED_BACKFILL}) e
            WHERE e->'keywordId' = to_jsonb(k.id)
              -- The poll drops entries whose cursor is missing or not a string, so they don't count.
              AND jsonb_typeof(e->'cursor') IN ('null', 'string')
          )
        ),
        '[]'::jsonb
      )
    )`,
    { keywordIds: [...new Set(keywordIds)] },
  )
}

export async function rewindOctolensMentionPollStartDate(
  qx: QueryExecutor,
  unitId: string,
  startDate: string,
  expectedStartDate: string | null,
): Promise<IOctolensMentionPollWriteResult> {
  return updateIdleMentionPollWatermark(
    qx,
    unitId,
    `${STORED_WATERMARK} || jsonb_build_object(
      'startDate', $(startDate)::text,
      'cursor', NULL,
      'pendingMaxTimestamp', NULL
    )`,
    { startDate, expectedStartDate },
    // Only over the start date the rewind was validated against, so a concurrent rewind or the
    // first poll run can't be silently overwritten.
    `(${STORED_WATERMARK}->>'startDate') IS NOT DISTINCT FROM $(expectedStartDate)`,
  )
}
