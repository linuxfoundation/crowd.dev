import { DEFAULT_TENANT_ID, generateUUIDv4 } from '@crowd/common'
import { IntegrationState, PlatformType } from '@crowd/types'

import { QueryExecutor } from '../queryExecutor'

export interface IFoundIntegration {
  id: string
  created: boolean
  restored: boolean
}

async function lockPlatformIntegrations(tx: QueryExecutor, platform: PlatformType): Promise<void> {
  await tx.select(`SELECT pg_advisory_xact_lock(hashtextextended($(key), 0))`, {
    key: `${platform}Integration`,
  })
}

async function insertIntegration(
  tx: QueryExecutor,
  platform: PlatformType,
  segmentId: string,
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
      platform,
      status: IntegrationState.DONE,
      segmentId,
      tenantId: DEFAULT_TENANT_ID,
    },
  )
  return id
}

export async function findOrCreateAutoManagedIntegration(
  qx: QueryExecutor,
  platform: PlatformType,
  segmentId: string,
): Promise<IFoundIntegration> {
  return qx.tx(async (tx) => {
    await lockPlatformIntegrations(tx, platform)

    const rows: { id: string; deletedAt: Date | null }[] = await tx.select(
      `
        SELECT i.id, i."deletedAt"
        FROM public.integrations i
        WHERE i.platform = $(platform) AND i."segmentId" = $(segmentId)
        ORDER BY i."updatedAt" DESC, i."createdAt" DESC, i.id
      `,
      { platform, segmentId },
    )

    const live = rows.filter((row) => row.deletedAt === null)
    if (live.length > 1) {
      throw new Error(
        `Found ${live.length} live ${platform} integrations for segment ${segmentId}!`,
      )
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

    const id = await insertIntegration(tx, platform, segmentId)
    return { id, created: true, restored: false }
  })
}
