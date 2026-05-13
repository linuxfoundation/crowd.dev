/**
 * Resolves segment and integration identifiers from a segment slug.
 *
 * Looks up the segment by slug + sourceId, then finds or creates an
 * integration for the given platform + segment pair. Results are cached
 * in Redis to avoid repeated DB queries across consumer restarts.
 */
import { DEFAULT_TENANT_ID, generateUUIDv4 } from '@crowd/common'
import type { DbConnection } from '@crowd/database'
import { getServiceChildLogger } from '@crowd/logging'
import { RedisCache } from '@crowd/redis'
import { IntegrationState, PlatformType } from '@crowd/types'

import type { SegmentRef } from './transformerBase'

const log = getServiceChildLogger('integrationResolver')

const CACHE_TTL_SECONDS = 24 * 60 * 60 // 24 hours
const LOCK_TTL_SECONDS = 30
const LOCK_WAIT_RETRIES = 5
const LOCK_WAIT_BASE_MS = 200

export interface ResolvedIntegration {
  segmentId: string
  integrationId: string
}

export class IntegrationResolver {
  constructor(
    private readonly db: DbConnection,
    private readonly cache: RedisCache,
  ) {}

  async preloadPlatforms(platforms: PlatformType[]): Promise<void> {
    if (platforms.length === 0) return

    const rows = await this.db.manyOrNone<{
      segmentId: string
      slug: string
      sourceId: string
      integrationId: string
      platform: string
    }>(
      `SELECT s.id AS "segmentId", s.slug, s."sourceId",
              i.id AS "integrationId", i.platform
       FROM segments s
       JOIN integrations i
         ON i."segmentId" = s.id
         AND i."deletedAt" IS NULL
         AND i.platform IN ($1:csv)
       WHERE s.type = 'subproject'`,
      [platforms],
    )

    for (const row of rows) {
      const cacheKey = `${row.platform}:${row.slug.toLowerCase()}:${row.sourceId.toLowerCase()}`
      const value: ResolvedIntegration = {
        segmentId: row.segmentId,
        integrationId: row.integrationId,
      }
      await this.cache.set(cacheKey, JSON.stringify(value), CACHE_TTL_SECONDS)
    }

    log.info({ platforms, cachedCount: rows.length }, 'Integration resolver cache warmed')
  }

  async resolve(platform: PlatformType, segment: SegmentRef): Promise<ResolvedIntegration | null> {
    const cacheKey = `${platform}:${segment.slug.toLowerCase()}:${segment.sourceId.toLowerCase()}`
    const cached = await this.cache.get(cacheKey)
    if (cached) {
      return JSON.parse(cached)
    }

    log.info({ platform, segment }, 'Cache miss, querying DB')

    const segmentRow = await this.db.oneOrNone<{ id: string }>(
      `SELECT id
       FROM segments
       WHERE LOWER(slug) = LOWER($1)
         AND LOWER("sourceId") = LOWER($2)
         AND type = 'subproject'
       LIMIT 1`,
      [segment.slug, segment.sourceId],
    )

    if (!segmentRow) {
      // TODO: decide how to handle missing segments — queue for review, or alert
      log.warn({ platform, segment }, 'Segment not found, skipping')
      return null
    }

    let integration = await this.db.oneOrNone<{ id: string }>(
      `SELECT id
       FROM integrations
       WHERE platform = $1
         AND "segmentId" = $2
         AND "deletedAt" IS NULL
       LIMIT 1`,
      [platform, segmentRow.id],
    )

    if (!integration) {
      const lockKey = `lock:integration:${platform}:${segmentRow.id}`
      const lockValue = generateUUIDv4()
      const existing = await this.cache.setIfNotExistsOrGet(lockKey, lockValue, LOCK_TTL_SECONDS)
      const acquired = existing === lockValue

      if (acquired) {
        try {
          const id = generateUUIDv4()
          log.info(
            { platform, segmentId: segmentRow.id, integrationId: id },
            'Creating integration for Snowflake connector',
          )
          await this.db.none(
            `INSERT INTO integrations (id, platform, status, "segmentId", "tenantId", "createdAt", "updatedAt")
             VALUES ($1, $2, $3, $4, $5, NOW(), NOW())`,
            [id, platform, IntegrationState.DONE, segmentRow.id, DEFAULT_TENANT_ID],
          )
          integration = { id }
        } finally {
          await this.cache.delete(lockKey)
        }
      } else {
        log.info(
          { platform, segmentId: segmentRow.id },
          'Lock held by another consumer, waiting for commit',
        )
        for (let attempt = 0; attempt < LOCK_WAIT_RETRIES; attempt++) {
          await new Promise((r) => setTimeout(r, LOCK_WAIT_BASE_MS * 2 ** attempt))
          integration = await this.db.oneOrNone<{ id: string }>(
            `SELECT id
             FROM integrations
             WHERE platform = $1
               AND "segmentId" = $2
               AND "deletedAt" IS NULL
             LIMIT 1`,
            [platform, segmentRow.id],
          )
          if (integration) break
        }
      }
    }

    if (!integration) {
      log.warn(
        { platform, segmentId: segmentRow.id },
        'Integration not found after lock contention, skipping',
      )
      return null
    }

    const result: ResolvedIntegration = {
      segmentId: segmentRow.id,
      integrationId: integration.id,
    }
    await this.cache.set(cacheKey, JSON.stringify(result), CACHE_TTL_SECONDS)
    return result
  }
}
