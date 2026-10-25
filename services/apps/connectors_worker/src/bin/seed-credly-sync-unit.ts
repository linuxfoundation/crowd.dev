import { assertCredentialKind, getCredential } from '@crowd/connectors'
import { credlyConnector } from '@crowd/connectors/src/connectors/credly'
import { mintCredlyAccessToken } from '@crowd/connectors/src/connectors/credly/oauth'
import { findSyncUnitByChannelName, upsertSyncUnits } from '@crowd/data-access-layer/src/connectors'
import { findOrCreateCredlyIntegration } from '@crowd/data-access-layer/src/credly'
import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { findSegmentById } from '@crowd/data-access-layer/src/segments'
import { getServiceLogger } from '@crowd/logging'

const log = getServiceLogger()

function usage(): never {
  log.error('Usage: seed-credly-sync-unit --segment-id <uuid> [--since <iso timestamp>]')
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

function parseArgs(rawArgv: string[]): { segmentId: string; since?: string } {
  const argv = rawArgv.filter((arg) => arg !== '--')
  const segmentId = takeFlag(argv, '--segment-id')
  const since = takeFlag(argv, '--since')
  if (!segmentId || argv.length > 0 || (since && !Number.isFinite(Date.parse(since)))) {
    usage()
  }
  return { segmentId, since }
}

setImmediate(async () => {
  try {
    const { segmentId, since } = parseArgs(process.argv.slice(2))

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
    }

    // emitEnabled stays false (shadow mode) until the shadow run is reviewed; it is insert-only.
    const unitsUpserted = await upsertSyncUnits(qx, units)

    log.info(
      { integrationId: integration.id, segmentId, syncNames, since, unitsUpserted },
      'seeding complete',
    )
    process.exit(0)
  } catch (err) {
    log.error(err, 'seeding failed')
    process.exit(1)
  }
})
