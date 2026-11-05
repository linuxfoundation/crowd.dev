import { WINDOW_LAG_MS } from '@crowd/connectors/src/connectors/credly/syncs/badgePoll'
import { findSyncUnitByChannelId } from '@crowd/data-access-layer/src/connectors'
import {
  listActiveCredlyTemplateMappings,
  lockCredlyTemplateMapping,
} from '@crowd/data-access-layer/src/credly'
import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceLogger } from '@crowd/logging'
import { PlatformType } from '@crowd/types'

import { onboardCredlyTemplate } from '../onboardCredlyTemplate'

const log = getServiceLogger()

// One-time migration only: a template mapped before per-template units existed was already fully
// covered by the retired org-wide poller, so this seeds live-only, unlike the resolver's full backfill.
export function liveOnlyWatermark(now: Date): Record<string, unknown> {
  return { since: new Date(now.getTime() - WINDOW_LAG_MS).toISOString() }
}

function usage(): never {
  log.error('Usage: migrate-credly-mapped-templates [--apply]')
  process.exit(1)
}

setImmediate(async () => {
  try {
    const argv = process.argv.slice(2).filter((arg) => arg !== '--')
    const apply = argv.includes('--apply')
    if (argv.some((arg) => arg !== '--apply')) usage()

    const db = await getDbConnection(WRITE_DB_CONFIG())
    const qx = pgpQx(db)
    const mappings = await listActiveCredlyTemplateMappings(qx)
    const now = new Date()

    const result = { alreadyPresent: 0, created: 0, reactivated: 0, skipped: 0 }
    for (const mapping of mappings) {
      if (!apply) {
        const existing = await findSyncUnitByChannelId(
          qx,
          mapping.integrationId,
          PlatformType.CREDLY,
          mapping.badgeTemplateId,
          'badgePoll',
        )
        if (existing) result.alreadyPresent += 1
        continue
      }

      // Attempted for every mapping, not only one missing a unit: it is a cheap no-op once the
      // unit exists, and it self-heals a mapping a prior run left unlocked next to a live unit.
      const onboarded = await qx.tx(async (txQx) => {
        const outcome = await onboardCredlyTemplate(
          txQx,
          mapping.integrationId,
          mapping.badgeTemplateId,
          mapping.badgeTemplateName,
          liveOnlyWatermark(now),
        )
        if (
          outcome.outcome === 'created' ||
          outcome.outcome === 'reactivated' ||
          outcome.reason === 'already_present'
        ) {
          await lockCredlyTemplateMapping(txQx, mapping.badgeTemplateId, {
            segmentId: mapping.segmentId,
            integrationId: mapping.integrationId,
          })
        }
        return outcome
      })

      if (onboarded.outcome === 'created') {
        result.created += 1
      } else if (onboarded.outcome === 'reactivated') {
        result.reactivated += 1
      } else if (onboarded.reason === 'already_present') {
        result.alreadyPresent += 1
      } else {
        result.skipped += 1
        log.warn({ ...mapping, ...onboarded }, 'credly template migration skipped')
      }
    }

    if (apply) {
      log.info(
        { total: mappings.length, ...result },
        'credly templates migrated to per-template units',
      )
    } else {
      log.info(
        { total: mappings.length, toMigrate: mappings.length - result.alreadyPresent },
        'dry run: nothing written',
      )
    }
    process.exit(0)
  } catch (err) {
    log.error(err, 'credly template migration failed')
    process.exit(1)
  }
})
