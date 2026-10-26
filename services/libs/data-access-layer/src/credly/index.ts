import { DEFAULT_TENANT_ID, generateUUIDv4 } from '@crowd/common'
import { IntegrationState, PlatformType } from '@crowd/types'

import { QueryExecutor } from '../queryExecutor'

export interface IFoundCredlyIntegration {
  id: string
  created: boolean
  restored: boolean
}

// integrations has no unique key on (platform, segmentId), so every writer of
// Credly integration rows serializes on this lock to avoid duplicates.
async function lockCredlyIntegrations(tx: QueryExecutor): Promise<void> {
  await tx.select(`SELECT pg_advisory_xact_lock(hashtextextended('credlyIntegration', 0))`)
}

async function insertCredlyIntegration(tx: QueryExecutor, segmentId: string): Promise<string> {
  const id = generateUUIDv4()
  await tx.result(
    `
      INSERT INTO public.integrations
        (id, platform, status, "segmentId", "tenantId", "createdAt", "updatedAt")
      VALUES ($(id), $(platform), $(status), $(segmentId), $(tenantId), now(), now())
    `,
    {
      id,
      platform: PlatformType.CREDLY,
      status: IntegrationState.DONE,
      segmentId,
      tenantId: DEFAULT_TENANT_ID,
    },
  )
  return id
}

export async function findOrCreateCredlyIntegration(
  qx: QueryExecutor,
  segmentId: string,
): Promise<IFoundCredlyIntegration> {
  return qx.tx(async (tx) => {
    await lockCredlyIntegrations(tx)

    const rows: { id: string; deletedAt: Date | null }[] = await tx.select(
      `
        SELECT i.id, i."deletedAt"
        FROM public.integrations i
        WHERE i.platform = $(platform) AND i."segmentId" = $(segmentId)
        ORDER BY i."updatedAt" DESC, i."createdAt" DESC, i.id
      `,
      { platform: PlatformType.CREDLY, segmentId },
    )

    const live = rows.filter((row) => row.deletedAt === null)
    if (live.length > 1) {
      throw new Error(`Found ${live.length} live Credly integrations for segment ${segmentId}!`)
    }
    if (live.length === 1) {
      return { id: live[0].id, created: false, restored: false }
    }

    if (rows.length > 0) {
      const { id } = rows[0]
      await tx.result(
        `UPDATE public.integrations SET "deletedAt" = NULL, "updatedAt" = now() WHERE id = $(id)`,
        { id },
      )
      return { id, created: false, restored: true }
    }

    const id = await insertCredlyIntegration(tx, segmentId)
    return { id, created: true, restored: false }
  })
}

export interface ICredlyTemplateMapping {
  badgeTemplateId: string
  badgeTemplateName: string
  integrationId: string
  segmentId: string
}

// lastSeenAt moves on every call so a stale seed shows up; updatedAt only on a real change.
// The pair must be a live Credly integration on that segment, or routing would never see the row.
export async function upsertCredlyTemplateMapping(
  qx: QueryExecutor,
  data: ICredlyTemplateMapping,
): Promise<void> {
  const written = await qx.result(
    `
      INSERT INTO integration."credlyTemplateMappings" AS m
        ("badgeTemplateId", "badgeTemplateName", "integrationId", "segmentId", "lastSeenAt")
      SELECT $(badgeTemplateId), $(badgeTemplateName), i.id, i."segmentId", now()
      FROM public.integrations i
      WHERE i.id = $(integrationId)
        AND i."segmentId" = $(segmentId)
        AND i.platform = $(platform)
        AND i."deletedAt" IS NULL
      ON CONFLICT ("badgeTemplateId") DO UPDATE
      SET "badgeTemplateName" = EXCLUDED."badgeTemplateName",
          "integrationId" = EXCLUDED."integrationId",
          "segmentId" = EXCLUDED."segmentId",
          "deactivatedAt" = NULL,
          "lastSeenAt" = now(),
          "updatedAt" = CASE
            WHEN m."integrationId" IS DISTINCT FROM EXCLUDED."integrationId"
              OR m."segmentId" IS DISTINCT FROM EXCLUDED."segmentId"
              OR m."badgeTemplateName" IS DISTINCT FROM EXCLUDED."badgeTemplateName"
              OR m."deactivatedAt" IS NOT NULL
            THEN now()
            ELSE m."updatedAt"
          END
    `,
    { ...data, platform: PlatformType.CREDLY },
  )
  if (written === 0) {
    throw new Error(
      `integration ${data.integrationId} is not a live Credly integration on segment ${data.segmentId}`,
    )
  }
}

export async function listActiveCredlyTemplateMappings(
  qx: QueryExecutor,
): Promise<ICredlyTemplateMapping[]> {
  return qx.select(
    `
      SELECT m."badgeTemplateId", m."badgeTemplateName", m."integrationId", m."segmentId"
      FROM integration."credlyTemplateMappings" m
      JOIN public.integrations i ON i.id = m."integrationId"
      WHERE m."deactivatedAt" IS NULL
        AND i.platform = $(platform)
        AND i."deletedAt" IS NULL
        AND i."segmentId" = m."segmentId"
      ORDER BY m."badgeTemplateId"
    `,
    { platform: PlatformType.CREDLY },
  )
}

export async function deactivateCredlyTemplateMapping(
  qx: QueryExecutor,
  badgeTemplateId: string,
): Promise<number> {
  return qx.result(
    `
      UPDATE integration."credlyTemplateMappings"
      SET "deactivatedAt" = now(), "updatedAt" = now()
      WHERE "badgeTemplateId" = $(badgeTemplateId) AND "deactivatedAt" IS NULL
    `,
    { badgeTemplateId },
  )
}
