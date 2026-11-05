import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceLogger } from '@crowd/logging'

import { backfillWatermark } from '../credlyTemplateResolve'

const log = getServiceLogger()

function usage(): never {
  log.error('Usage: backfill-stale-credly-templates [--apply]')
  process.exit(1)
}

setImmediate(async () => {
  try {
    const argv = process.argv.slice(2).filter((arg) => arg !== '--')
    const apply = argv.includes('--apply')
    if (argv.some((arg) => arg !== '--apply')) usage()

    const db = await getDbConnection(WRITE_DB_CONFIG())
    const qx = pgpQx(db)

    const pending: { id: string }[] = await qx.select(
      `SELECT su.id
       FROM integration.sync_units su
       JOIN integration."credlyTemplateReleases" r ON r."badgeTemplateId" = su."channelId"
       WHERE su.platform = 'credly' AND su."syncName" = 'badgePoll' AND su.status = 'active'
         AND r."completedAt" IS NULL
         AND su."lockedAt" IS NULL
         AND NOT (COALESCE(su.watermark, '{}'::jsonb) ? 'backfill')`,
      {},
    )

    if (!apply) {
      log.info({ toBackfill: pending.length }, 'dry run: nothing written')
      process.exit(0)
    }

    const watermark = backfillWatermark(null, new Date())
    let updated = 0
    for (const row of pending) {
      // A direct overwrite, not mergeSyncUnitWatermark: that helper skips a row whose watermark
      // already has any patch key, and every live-only unit here already has `since`.
      updated += await qx.result(
        `UPDATE integration.sync_units
         SET watermark = $(watermark)::jsonb, "updatedAt" = now()
         WHERE id = $(id)
           AND "lockedAt" IS NULL
           AND NOT (COALESCE(watermark, '{}'::jsonb) ? 'backfill')`,
        { id: row.id, watermark: JSON.stringify(watermark) },
      )
    }

    log.info(
      { total: pending.length, updated },
      'reset stale credly templates from live-only to full backfill',
    )
    process.exit(0)
  } catch (err) {
    log.error(err, 'credly stale-backfill reset failed')
    process.exit(1)
  }
})
