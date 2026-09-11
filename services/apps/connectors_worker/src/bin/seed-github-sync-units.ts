import { generateUUIDv4 } from '@crowd/common'
import type { Channel } from '@crowd/connectors'
import { getCredential } from '@crowd/connectors'
import { githubConnector } from '@crowd/connectors/src/connectors/github'
import {
  mintInstallationToken,
  resolveInstallationId,
} from '@crowd/connectors/src/connectors/github/appToken'
import { resolveRepoChannel } from '@crowd/connectors/src/connectors/github/discover'
import type { SyncUnitUpsert } from '@crowd/data-access-layer/src/connectors'
import { upsertSyncUnits } from '@crowd/data-access-layer/src/connectors'
import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { upsertRepository } from '@crowd/data-access-layer/src/repositories'
import { getServiceLogger } from '@crowd/logging'

const log = getServiceLogger()

interface RepoRef {
  owner: string
  name: string
}

function usage(): never {
  log.error(
    'Usage: seed-github-sync-units --integration-id <uuid> [--installation-id <id>] <owner/repo | github url> [more repos ...]',
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

function parseRepoArg(arg: string): RepoRef {
  const path = arg.replace(/^https:\/\/github\.com\//, '').replace(/\/+$/, '')
  const parts = path.split('/')
  if (parts.length !== 2 || !parts[0] || !parts[1]) {
    throw new Error(`invalid repo argument "${arg}" - expected owner/repo or a github repo URL`)
  }
  return { owner: parts[0], name: parts[1] }
}

function parseArgs(rawArgv: string[]): {
  integrationId: string
  installationId?: string
  repos: RepoRef[]
} {
  const argv = rawArgv.filter((arg) => arg !== '--')
  const integrationId = takeFlag(argv, '--integration-id')
  const installationId = takeFlag(argv, '--installation-id')
  if (!integrationId || argv.length === 0) {
    usage()
  }
  return { integrationId, installationId, repos: argv.map(parseRepoArg) }
}

async function getSegmentContext(
  qx: QueryExecutor,
  integrationId: string,
): Promise<{ segmentId: string; gitIntegrationId: string; insightsProjectId: string }> {
  const integration: { segmentId: string | null } | null = await qx.selectOneOrNone(
    `SELECT "segmentId" FROM integrations WHERE id = $(integrationId) AND "deletedAt" IS NULL`,
    { integrationId },
  )
  if (!integration?.segmentId) {
    throw new Error(`integration ${integrationId} not found or has no segmentId`)
  }

  const gitIntegration: { id: string } | null = await qx.selectOneOrNone(
    `SELECT id FROM integrations
     WHERE "segmentId" = $(segmentId) AND platform = 'git' AND "deletedAt" IS NULL`,
    { segmentId: integration.segmentId },
  )
  if (!gitIntegration) {
    throw new Error(`git integration not found for segment ${integration.segmentId}`)
  }

  const insightsProject: { id: string } | null = await qx.selectOneOrNone(
    `SELECT id FROM "insightsProjects"
     WHERE "segmentId" = $(segmentId) AND "deletedAt" IS NULL`,
    { segmentId: integration.segmentId },
  )
  if (!insightsProject) {
    throw new Error(`insights project not found for segment ${integration.segmentId}`)
  }

  return {
    segmentId: integration.segmentId,
    gitIntegrationId: gitIntegration.id,
    insightsProjectId: insightsProject.id,
  }
}

setImmediate(async () => {
  try {
    const args = parseArgs(process.argv.slice(2))
    const { integrationId, repos } = args

    const db = await getDbConnection(WRITE_DB_CONFIG())
    const qx = pgpQx(db)

    const credential = await getCredential(qx, integrationId)
    const installationId = args.installationId ?? (await resolveInstallationId(credential))
    const { token, expiresAt } = await mintInstallationToken(credential, installationId)
    log.info({ installationId, expiresAt }, 'github app auth verified, installation token minted')

    const segmentContext = await getSegmentContext(qx, integrationId)

    const channels: Channel[] = []
    for (const repo of repos) {
      const channel = await resolveRepoChannel(token, repo.owner, repo.name)
      const repositoryUpsert = await upsertRepository(qx, {
        id: generateUUIDv4(),
        url: channel.channelName,
        segmentId: segmentContext.segmentId,
        gitIntegrationId: segmentContext.gitIntegrationId,
        sourceIntegrationId: integrationId,
        insightsProjectId: segmentContext.insightsProjectId,
      })
      log.info({ ...channel, repositoryUpsert }, 'repo resolved')
      channels.push(channel)
    }

    const syncNames = githubConnector.syncs.map((s) => s.name)
    if (syncNames.length === 0) {
      log.warn('github manifest has no syncs registered yet - seeded repositories only')
    }

    const units: SyncUnitUpsert[] = channels.flatMap((channel) =>
      syncNames.map((syncName) => ({
        integrationId,
        platform: githubConnector.platform,
        channelId: channel.channelId,
        channelName: channel.channelName,
        syncName,
      })),
    )
    const unitsUpserted = await upsertSyncUnits(qx, units)

    log.info({ repos: channels.length, syncNames, unitsUpserted }, 'seeding complete')
    process.exit(0)
  } catch (err) {
    log.error(err, 'seeding failed')
    process.exit(1)
  }
})
