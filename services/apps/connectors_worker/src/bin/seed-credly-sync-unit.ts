import { assertCredentialKind, getCredential } from '@crowd/connectors'
import { credlyConnector } from '@crowd/connectors/src/connectors/credly'
import { mintCredlyAccessToken } from '@crowd/connectors/src/connectors/credly/oauth'
import {
  findSyncUnitByChannelName,
  mergeSyncUnitWatermark,
  upsertSyncUnits,
} from '@crowd/data-access-layer/src/connectors'
import type { ISyncUnit } from '@crowd/data-access-layer/src/connectors/types'
import { findOrCreateCredlyIntegration } from '@crowd/data-access-layer/src/credly'
import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { findSegmentById } from '@crowd/data-access-layer/src/segments'
import { getServiceLogger } from '@crowd/logging'

const log = getServiceLogger()

function usage(): never {
  log.error(
    'Usage: seed-credly-sync-unit --segment-id <uuid> [--since <iso timestamp>] [--backfill-until <iso timestamp> --backfill-floor <iso timestamp>]',
  )
  process.exit(1)
}

function takeFlag(argv: string[], flag: string): string | undefined {
  const flagIndex = argv.indexOf(flag)
  if (flagIndex === -1) {
    return undefined
  }
  const value = argv[flagIndex + 1]
  argv.splice(flagIndex, 2)
  return value
}

interface Backfill {
  floor: string
  top: string
}

function parseBackfill(until: string | undefined, floor: string | undefined): Backfill | undefined {
  if (!until && !floor) return undefined
  const untilMs = Date.parse(until ?? '')
  const floorMs = Date.parse(floor ?? '')
  if (!Number.isFinite(untilMs) || !Number.isFinite(floorMs) || floorMs >= untilMs) usage()
  return { floor: new Date(floorMs).toISOString(), top: new Date(untilMs).toISOString() }
}

function parseArgs(rawArgv: string[]): { segmentId: string; since?: string; backfill?: Backfill } {
  const argv = rawArgv.filter((arg) => arg !== '--')
  const segmentId = takeFlag(argv, '--segment-id')
  const since = takeFlag(argv, '--since')
  const backfill = parseBackfill(
    takeFlag(argv, '--backfill-until'),
    takeFlag(argv, '--backfill-floor'),
  )
  if (!segmentId || argv.length > 0 || (since && !Number.isFinite(Date.parse(since)))) {
    usage()
  }
  return { segmentId, since, backfill }
}

// The backfill top must meet the live cursor, or the badges between the two are never listed.
async function armBackfill(qx: QueryExecutor, unit: ISyncUnit, backfill: Backfill): Promise<void> {
  const liveSince = unit.watermark?.since
  if (typeof liveSince !== 'string' || !Number.isFinite(Date.parse(liveSince))) {
    throw new Error(
      `badge poll unit ${unit.id} has no live cursor yet: let it run once or seed it with --since`,
    )
  }
  if (Date.parse(backfill.top) < Date.parse(liveSince)) {
    throw new Error(
      `--backfill-until ${backfill.top} is before the live cursor ${liveSince} of unit ${unit.id}`,
    )
  }
  const armed = await mergeSyncUnitWatermark(qx, unit.id, { backfill })
  if (armed === 0) {
    throw new Error(
      `badge poll unit ${unit.id} could not be armed: it is running or a backfill is already pending`,
    )
  }
  log.info({ unitId: unit.id, ...backfill }, 'credly backfill armed')
}

setImmediate(async () => {
  try {
    const { segmentId, since, backfill } = parseArgs(process.argv.slice(2))

    const db = await getDbConnection(WRITE_DB_CONFIG())
    const qx = pgpQx(db)

    const segment = await findSegmentById(qx, segmentId)
    if (!segment) {
      throw new Error(`segment ${segmentId} not found`)
    }

    const integration = await findOrCreateCredlyIntegration(qx, segmentId)
    log.info({ ...integration, segmentSlug: segment.slug }, 'credly integration ready')

    const credential = await getCredential(qx, integration.id)
    assertCredentialKind(credential, 'credly-oauth-client')
    const { expiresAt } = await mintCredlyAccessToken(credential)
    log.info({ expiresAt }, 'credly oauth credential verified')

    const channels = await credlyConnector.discover(credential)
    const syncNames = credlyConnector.syncs.map((sync) => sync.name)
    const units = channels.flatMap((channel) =>
      syncNames.map((syncName) => ({
        integrationId: integration.id,
        platform: credlyConnector.platform,
        channelId: channel.channelId,
        channelName: channel.channelName,
        syncName,
        watermark: since ? { since: new Date(since).toISOString() } : undefined,
      })),
    )

    const existingUnits: ISyncUnit[] = []
    for (const unit of units) {
      const existing = await findSyncUnitByChannelName(
        qx,
        unit.integrationId,
        unit.platform,
        unit.channelName,
        unit.syncName,
      )
      if (existing && since) {
        throw new Error(
          `sync unit ${existing.id} (${unit.syncName}) already exists and owns its watermark: re-run without --since`,
        )
      }
      if (existing) existingUnits.push(existing)
    }

    // emitEnabled stays false (shadow mode) until the shadow run is reviewed; it is insert-only.
    const unitsUpserted = await upsertSyncUnits(qx, units)
    if (backfill) {
      const badgePollUnits = existingUnits.filter((unit) => unit.syncName === 'badgePoll')
      if (badgePollUnits.length === 0) {
        throw new Error('seed the badge poll unit first, then arm the backfill on a second run')
      }
      for (const unit of badgePollUnits) await armBackfill(qx, unit, backfill)
    }

    log.info(
      { integrationId: integration.id, segmentId, syncNames, since, backfill, unitsUpserted },
      'seeding complete',
    )
    process.exit(0)
  } catch (err) {
    log.error(err, 'seeding failed')
    process.exit(1)
  }
})
