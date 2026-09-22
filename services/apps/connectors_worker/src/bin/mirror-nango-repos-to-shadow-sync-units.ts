import { getCredential } from '@crowd/connectors'
import { githubConnector } from '@crowd/connectors/src/connectors/github'
import { mintInstallationToken } from '@crowd/connectors/src/connectors/github/appToken'
import { resolveRepoChannel } from '@crowd/connectors/src/connectors/github/discover'
import type { SyncUnitUpsert } from '@crowd/data-access-layer/src/connectors'
import { upsertSyncUnits } from '@crowd/data-access-layer/src/connectors'
import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceLogger } from '@crowd/logging'
import { NangoIntegration, getNangoConnectionData, initNangoCloudClient } from '@crowd/nango'

const log = getServiceLogger()

const MIRROR_INITIAL_LOOKBACK_MS = 48 * 60 * 60 * 1000

interface RepoRef {
  owner: string
  name: string
}

interface NangoMappingRow {
  integrationId: string
  connectionId: string
}

function usage(): never {
  log.error(
    'Usage: mirror-nango-repos-to-shadow-sync-units <owner/repo | github url> [more repos ...]',
  )
  process.exit(1)
}

function parseRepoArg(arg: string): RepoRef {
  const path = arg.replace(/^https:\/\/github\.com\//, '').replace(/\/+$/, '')
  const parts = path.split('/')
  if (parts.length !== 2 || !parts[0] || !parts[1]) {
    throw new Error(`invalid repo argument "${arg}" - expected owner/repo or a github repo URL`)
  }
  return { owner: parts[0], name: parts[1] }
}

function parseArgs(rawArgv: string[]): RepoRef[] {
  const argv = rawArgv.filter((arg) => arg !== '--')
  if (argv.length === 0) {
    usage()
  }
  return argv.map(parseRepoArg)
}

async function getNangoMappingForOwnerRepo(
  qx: QueryExecutor,
  owner: string,
  repoName: string,
): Promise<NangoMappingRow> {
  const row: NangoMappingRow | null = await qx.selectOneOrNone(
    `SELECT nm."integrationId", nm."connectionId"
     FROM integration.nango_mapping nm
     JOIN integrations i ON i.id = nm."integrationId"
     WHERE lower(nm.owner) = lower($(owner))
       AND lower(nm."repoName") = lower($(repoName))
       AND i.platform = 'github-nango'
       AND i."deletedAt" IS NULL
     ORDER BY nm."updatedAt" DESC
     LIMIT 1`,
    { owner, repoName },
  )
  if (!row) {
    throw new Error(`no active github-nango integration found for ${owner}/${repoName}`)
  }
  return row
}

setImmediate(async () => {
  try {
    const repos = parseArgs(process.argv.slice(2))

    const db = await getDbConnection(WRITE_DB_CONFIG())
    const qx = pgpQx(db)

    await initNangoCloudClient()

    const syncNames = githubConnector.syncs.map((s) => s.name)
    if (syncNames.length === 0) {
      log.warn('github manifest has no syncs registered yet - mirroring channels only')
    }

    const cutoff = new Date(Date.now() - MIRROR_INITIAL_LOOKBACK_MS)
    const since = new Date(
      Date.UTC(cutoff.getUTCFullYear(), cutoff.getUTCMonth(), cutoff.getUTCDate()),
    ).toISOString()
    const watermark = { phase: 'incremental', since, cursor: null }

    const units: SyncUnitUpsert[] = []
    for (const repo of repos) {
      const mapping = await getNangoMappingForOwnerRepo(qx, repo.owner, repo.name)
      const credential = await getCredential(qx, mapping.integrationId)

      const connectionData = await getNangoConnectionData(
        NangoIntegration.GITHUB,
        mapping.connectionId,
      )
      const installationId: string = connectionData.connection_config.installation_id
      const { token } = await mintInstallationToken(credential, installationId)

      const channel = await resolveRepoChannel(token, repo.owner, repo.name)
      log.info(
        { owner: repo.owner, name: repo.name, integrationId: mapping.integrationId, ...channel },
        'repo channel resolved',
      )

      for (const syncName of syncNames) {
        units.push({
          integrationId: mapping.integrationId,
          platform: githubConnector.platform,
          channelId: channel.channelId,
          channelName: channel.channelName,
          syncName,
          watermark,
        })
      }
    }

    const unitsUpserted = await upsertSyncUnits(qx, units)

    log.info({ repos: repos.length, syncNames, unitsUpserted }, 'mirroring complete')
    process.exit(0)
  } catch (err) {
    log.error(err, 'mirroring failed')
    process.exit(1)
  }
})
