import { ApplicationFailure } from '@temporalio/activity'

import { singleOrDefault } from '@crowd/common'
import { generateUUIDv4 as uuid } from '@crowd/common'
import { GithubIntegrationService } from '@crowd/common_services'
import {
  addGithubNangoConnection,
  addRepoToGitIntegration,
  fetchIntegrationById,
  findIntegrationDataForNangoWebhookProcessing,
  getNangoCursor,
  getNangoMappingsForIntegrations,
  linkNangoMappingToRepository,
  removeGithubNangoConnection,
  removeNangoCursorsByConnection,
  setNangoCursor,
  updateNangoCursorLastCheckedAt,
} from '@crowd/data-access-layer/src/integrations'
import IntegrationStreamRepository from '@crowd/data-access-layer/src/old/apps/integration_stream_worker/integrationStream.repo'
import { dbStoreQx } from '@crowd/data-access-layer/src/queryExecutor'
import {
  getReposForGithubIntegration,
  softDeleteRepositories,
  upsertRepository,
} from '@crowd/data-access-layer/src/repositories'
import { getChildLogger } from '@crowd/logging'
import {
  ALL_NANGO_INTEGRATIONS,
  NANGO_INTEGRATION_CONFIG,
  NangoIntegration,
  deleteNangoConnection,
  getNangoCloudRecords,
  initNangoCloudClient,
  startNangoSync as startNangoSyncCloud,
} from '@crowd/nango'
import { RedisCache } from '@crowd/redis'
import { IntegrationResultType, PlatformType } from '@crowd/types'

import { isSwitchedToConnectors } from '../connectorSwitchGuard'
import { svc } from '../main'
import {
  IGithubIntegrationSyncInstructions,
  IGithubRepoData,
  IProcessNangoWebhookArguments,
} from '../types'

const TOKEN_CONNECTION_IDS_CACHE_KEY = 'tokenConnectionIds'
export async function invalidateGithubTokenConnectionIdsCache(): Promise<void> {
  const redisCache = new RedisCache('nangoGh', svc.redis, svc.log)
  await redisCache.delete(TOKEN_CONNECTION_IDS_CACHE_KEY)
  svc.log.info('Invalidated github token connection IDs cache')
}

export async function canCreateGithubConnection(): Promise<boolean> {
  svc.log.info(
    'GitHub Nango connection creation is disabled: new repositories are onboarded by the connectors lifecycle',
  )
  return false
}

export async function processNangoWebhook(
  args: IProcessNangoWebhookArguments,
): Promise<string | undefined> {
  let logger = getChildLogger(processNangoWebhook.name, svc.log, {
    provider: args.providerConfigKey,
    connectionId: args.connectionId,
    model: args.model,
  })

  if (!ALL_NANGO_INTEGRATIONS.includes(args.providerConfigKey as NangoIntegration)) {
    logger.info({ providerConfigKey: args.providerConfigKey }, 'Skipping non-Nango integration!')
    return
  }

  if (
    args.providerConfigKey === NangoIntegration.GITHUB &&
    args.model === NANGO_INTEGRATION_CONFIG[NangoIntegration.GITHUB].models.STAR
  ) {
    logger.info('Skipping GithubStar records!')
    return
  }

  const integration = await findIntegrationDataForNangoWebhookProcessing(
    dbStoreQx(svc.postgres.reader),
    args.connectionId,
  )

  if (!integration) {
    svc.log.warn(
      { connectionId: args.connectionId, provider: args.providerConfigKey },
      'Integration not found!',
    )
    return
  }

  logger = getChildLogger(processNangoWebhook.name, logger, {
    integrationId: integration.id,
  })

  if (
    args.providerConfigKey === NangoIntegration.GITHUB &&
    (await isSwitchedToConnectors(
      dbStoreQx(svc.postgres.writer),
      integration.id,
      args.connectionId,
      args.model,
    ))
  ) {
    logger.info('Skipping records: sync is switched to the connectors pipeline')
    await updateNangoCursorLastCheckedAt(
      dbStoreQx(svc.postgres.writer),
      integration.id,
      args.connectionId,
      args.model,
    )
    return
  }

  let cursor = args.nextPageCursor
  let existingCursor = false
  if (!cursor) {
    const existingCursorValue = await getNangoCursor(
      dbStoreQx(svc.postgres.reader),
      integration.id,
      args.connectionId,
      args.model,
    )
    if (existingCursorValue && !['<no-cursor>', '<no-records>'].includes(existingCursorValue)) {
      cursor = existingCursorValue
      existingCursor = true
    }
  }

  await initNangoCloudClient()

  const records = await getNangoCloudRecords(
    args.providerConfigKey as NangoIntegration,
    args.connectionId,
    args.model,
    cursor,
  )

  const repo = new IntegrationStreamRepository(svc.postgres.writer, logger)

  if (records.records.length > 0) {
    logger.info(`Processing ${records.records.length} nango records!`)
    for (const record of records.records) {
      // process record
      const resultId = await repo.publishExternalResult(integration.id, {
        type: IntegrationResultType.ACTIVITY,
        // github uses public.repositories via findSegmentsForRepos() to determine segmentId
        segmentId:
          args.providerConfigKey !== NangoIntegration.GITHUB ? integration.segmentId : undefined,
        data: record.activity,
      })
      await svc.dataSinkWorkerEmitter.triggerResultProcessing(
        resultId,
        resultId,
        args.syncType === 'INITIAL',
      )
    }

    if (records.nextCursor) {
      await setNangoCursor(
        dbStoreQx(svc.postgres.writer),
        integration.id,
        args.connectionId,
        args.providerConfigKey,
        args.model,
        records.nextCursor,
      )

      return records.nextCursor
    } else {
      let cursor = '<no-cursor>'
      const lastRecord = records.records[records.records.length - 1]
      if (lastRecord.metadata?.cursor) {
        cursor = lastRecord.metadata.cursor
      }

      // if we dont have a cursor but we have an existing one we keep existing one
      // if we have a cursor from the last record we also set it
      if ((cursor === '<no-cursor>' && !existingCursor) || (cursor && cursor !== '<no-cursor>')) {
        await setNangoCursor(
          dbStoreQx(svc.postgres.writer),
          integration.id,
          args.connectionId,
          args.providerConfigKey,
          args.model,
          cursor,
        )
      }
    }
  } else if (!existingCursor) {
    // only update if we don't have an existing cursor
    await setNangoCursor(
      dbStoreQx(svc.postgres.writer),
      integration.id,
      args.connectionId,
      args.providerConfigKey,
      args.model,
      '<no-records>',
    )
  }

  // Update lastCheckedAt after successful processing
  await updateNangoCursorLastCheckedAt(
    dbStoreQx(svc.postgres.writer),
    integration.id,
    args.connectionId,
    args.model,
  )
}

export async function analyzeGithubIntegration(
  integrationId: string,
): Promise<IGithubIntegrationSyncInstructions> {
  const reposToDelete: {
    repo: IGithubRepoData
    connectionId: string
  }[] = []
  const duplicatesToDelete: {
    repo: IGithubRepoData
    connectionId: string
  }[] = []
  const reposToSync: IGithubRepoData[] = []

  const integration = await fetchIntegrationById(dbStoreQx(svc.postgres.writer), integrationId)

  if (integration) {
    if (integration.platform === PlatformType.GITHUB_NANGO) {
      const settings = integration.settings
      const qx = dbStoreQx(svc.postgres.writer)

      // Build desired repos list from repositories table
      const repoRows = await getReposForGithubIntegration(qx, integrationId)
      const repoSet = new Map<string, IGithubRepoData>()
      for (const row of repoRows) {
        repoSet.set(`${row.owner}/${row.name}`, { owner: row.owner, repoName: row.name })
      }

      // For fullSync orgs, discover new repos from GitHub API
      for (const org of settings.orgs) {
        if (org.fullSync) {
          const results = await GithubIntegrationService.getOrgRepos(org.name)
          for (const result of results) {
            const parsed = parseGithubUrl(result.url)
            const key = `${parsed.owner}/${parsed.repoName}`
            if (!repoSet.has(key)) {
              repoSet.set(key, parsed)
              svc.log.info(`fullSync: discovered new repo ${result.url} for org ${org.name}`)
            }
          }
        }
      }

      const finalRepos = Array.from(repoSet.values())

      // fetch nango mappings from the dedicated table
      const allNangoMappings = await getNangoMappingsForIntegrations(
        dbStoreQx(svc.postgres.writer),
        [integrationId],
      )
      const nangoMapping = allNangoMappings[integrationId] || {}
      const connectionIds = Object.keys(nangoMapping)

      // determine which connections to delete if needed
      if (connectionIds.length > 0) {
        // check for duplicates as well by tracking which repos have connectionIds
        const existingConnectedRepos: IGithubRepoData[] = []
        for (const connectionId of connectionIds) {
          const mappedRepo = nangoMapping[connectionId]

          if (
            existingConnectedRepos.some(
              (r) => r.owner === mappedRepo.owner && r.repoName === mappedRepo.repoName,
            )
          ) {
            // found duplicate connectionId for the same repo
            duplicatesToDelete.push({
              repo: mappedRepo,
              connectionId,
            })
          } else {
            const found = singleOrDefault(
              finalRepos,
              (r) => r.owner === mappedRepo.owner && r.repoName === mappedRepo.repoName,
            )

            // if repo is in nangoMapping but not in settings delete the connection
            if (!found) {
              reposToDelete.push({
                repo: mappedRepo,
                connectionId,
              })
            } else {
              existingConnectedRepos.push(mappedRepo)
            }
          }
        }
      }

      svc.log.info(
        `For integration ${integrationId} found ${reposToDelete.length} connections to delete!`,
      )
    } else {
      svc.log.warn(`Integration ${integrationId} is not a Github Nango integration!`)
    }
  } else {
    svc.log.warn(`Integration ${integrationId} not found!`)
  }

  return {
    providerConfigKey: NangoIntegration.GITHUB,
    reposToDelete,
    duplicatesToDelete,
    reposToSync,
  }
}

export async function createGithubConnection(
  integrationId: string,
  repo: IGithubRepoData,
): Promise<string> {
  throw ApplicationFailure.nonRetryable(
    `GitHub Nango connection creation is disabled, repo ${repo.owner}/${repo.repoName} of integration ${integrationId} is onboarded by the connectors lifecycle`,
  )
}

export async function setGithubConnection(
  integrationId: string,
  repo: IGithubRepoData,
  connectionId: string,
): Promise<void> {
  svc.log.info(
    { integrationId },
    `Setting github connection for repo ${repo.owner}/${repo.repoName}!`,
  )
  // store connectionId - repo mapping in integration.nango_mapping table
  await addGithubNangoConnection(
    dbStoreQx(svc.postgres.writer),
    integrationId,
    connectionId,
    repo.owner,
    repo.repoName,
  )
}

export async function removeGithubConnection(
  integrationId: string,
  connectionId: string,
): Promise<void> {
  svc.log.info({ integrationId }, `Removing github connection ${connectionId}!`)
  // remove connectionId - repo mapping from integration.nango_mapping table
  await removeGithubNangoConnection(dbStoreQx(svc.postgres.writer), integrationId, connectionId)
  // remove cursor rows for this connection
  await removeNangoCursorsByConnection(dbStoreQx(svc.postgres.writer), integrationId, connectionId)
}

export async function startNangoSync(
  integrationId: string,
  providerConfigKey: string,
  connectionId: string,
): Promise<void> {
  svc.log.info(
    { integrationId },
    `Starting nango sync for connection ${connectionId} for provider ${providerConfigKey}!`,
  )

  await initNangoCloudClient()
  await startNangoSyncCloud(providerConfigKey as NangoIntegration, connectionId)
}

export async function deleteConnection(
  integrationId: string,
  providerConfigKey: string,
  connectionId: string,
): Promise<void> {
  svc.log.info(
    { integrationId },
    `Deleting nango connection ${connectionId} for provider ${providerConfigKey}!`,
  )

  await initNangoCloudClient()
  await deleteNangoConnection(providerConfigKey as NangoIntegration, connectionId)
}

export async function unmapGithubRepo(integrationId: string, repo: IGithubRepoData): Promise<void> {
  svc.log.info(
    { integrationId },
    `Removing github repo mapping for repo ${repo.owner}/${repo.repoName}!`,
  )
  const repoUrl = `https://github.com/${repo.owner}/${repo.repoName}`

  // soft-delete from public.repositories
  const affected = await softDeleteRepositories(
    dbStoreQx(svc.postgres.writer),
    [repoUrl],
    integrationId,
  )
  svc.log.info(
    { integrationId, repoUrl, affected },
    `Soft-delete from public.repositories affected ${affected} row(s)`,
  )
}

export async function updateGitIntegrationWithRepo(
  integrationId: string,
  repo: IGithubRepoData,
): Promise<void> {
  svc.log.info(
    { integrationId },
    `Updating git integration with repo ${repo.owner}/${repo.repoName} for integration ${integrationId}!`,
  )
  const repoUrl = `https://github.com/${repo.owner}/${repo.repoName}`
  await addRepoToGitIntegration(dbStoreQx(svc.postgres.writer), integrationId, repoUrl)
}

function parseGithubUrl(url: string): IGithubRepoData {
  // Create URL object
  const parsedUrl = new URL(url)

  // Get pathname (e.g., "/islet-project/3rd-kvmtool")
  const pathname = parsedUrl.pathname

  // Split by '/' and remove empty elements
  const parts = pathname.split('/').filter(Boolean)

  // First part is owner, second is repo
  if (parts.length >= 2) {
    return {
      owner: parts[0],
      repoName: parts[1],
    }
  }

  throw new Error('Invalid GitHub URL format')
}

export async function logInfo(message: string, serializedParams?: string): Promise<void> {
  svc.log.info(serializedParams ? JSON.parse(serializedParams) : {}, message)
}

export async function mapGithubRepoToRepositories(
  integrationId: string,
  repo: IGithubRepoData,
): Promise<void> {
  svc.log.info(
    { integrationId },
    `Upserting github repo ${repo.owner}/${repo.repoName} to public.repositories!`,
  )
  const repoUrl = `https://github.com/${repo.owner}/${repo.repoName}`
  const forkedFrom = await GithubIntegrationService.getForkedFrom(repo.owner, repo.repoName)
  const qx = dbStoreQx(svc.postgres.writer)

  const githubIntegration = await qx.selectOneOrNone(
    `SELECT "segmentId" FROM integrations WHERE id = $(integrationId) AND "deletedAt" IS NULL`,
    { integrationId },
  )
  if (!githubIntegration) {
    throw new Error(`GitHub integration ${integrationId} not found!`)
  }

  const gitIntegration = await qx.selectOneOrNone(
    `SELECT id FROM integrations WHERE "segmentId" = $(segmentId) AND platform = 'git' AND "deletedAt" IS NULL`,
    { segmentId: githubIntegration.segmentId },
  )
  if (!gitIntegration) {
    throw new Error(`Git integration not found for segment ${githubIntegration.segmentId}!`)
  }

  const insightsProject = await qx.selectOneOrNone(
    `SELECT id FROM "insightsProjects" WHERE "segmentId" = $(segmentId) AND "deletedAt" IS NULL`,
    { segmentId: githubIntegration.segmentId },
  )
  if (!insightsProject) {
    throw new Error(`Insights project not found for segment ${githubIntegration.segmentId}!`)
  }

  try {
    const result = await upsertRepository(qx, {
      id: uuid(),
      url: repoUrl,
      segmentId: githubIntegration.segmentId,
      gitIntegrationId: gitIntegration.id,
      sourceIntegrationId: integrationId,
      insightsProjectId: insightsProject.id,
      forkedFrom,
    })

    svc.log.info(
      { integrationId, repoUrl, result },
      `Upsert to public.repositories result: ${result}`,
    )
  } catch (err) {
    svc.log.error(
      { integrationId, repoUrl, segmentId: githubIntegration.segmentId, err },
      `Failed to upsert repository to public.repositories`,
    )
    throw err
  }
}

export async function linkNangoMappingToRepo(
  integrationId: string,
  connectionId: string,
  repo: IGithubRepoData,
): Promise<void> {
  const repoUrl = `https://github.com/${repo.owner}/${repo.repoName}`
  const qx = dbStoreQx(svc.postgres.writer)

  const repository = await qx.selectOneOrNone(
    `SELECT id FROM repositories WHERE url = $(url) AND "sourceIntegrationId" = $(integrationId) AND "deletedAt" IS NULL`,
    { url: repoUrl, integrationId },
  )

  if (repository) {
    await linkNangoMappingToRepository(qx, integrationId, connectionId, repository.id)
    svc.log.info(
      { integrationId, connectionId, repositoryId: repository.id },
      `Linked nango_mapping to repository`,
    )
  } else {
    svc.log.warn(
      { integrationId, connectionId, repoUrl },
      `Repository not found, nango_mapping.repositoryId remains NULL`,
    )
  }
}
