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
