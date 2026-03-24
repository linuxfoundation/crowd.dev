import { IntegrationRunWorkerEmitter } from '@crowd/common_services'
import { DbStore, getDbConnection } from '@crowd/data-access-layer/src/database'
import IntegrationRunRepository from '@crowd/data-access-layer/src/old/apps/integration_run_worker/integrationRun.repo'
import { dbStoreQx } from '@crowd/data-access-layer/src/queryExecutor'
import { populateGithubSettingsWithRepos } from '@crowd/data-access-layer/src/repositories'
import {
  GithubIntegrationSettings,
  GithubManualIntegrationSettings,
  GithubManualStreamType,
} from '@crowd/integrations'
import { getServiceLogger } from '@crowd/logging'
import { QueueFactory } from '@crowd/queue'
import { IntegrationState } from '@crowd/types'

import { DB_CONFIG, QUEUE_CONFIG } from '../conf'

const mapStreamTypeToEnum = (stream: string): GithubManualStreamType => {
  switch (stream) {
    case 'stars':
      return GithubManualStreamType.STARGAZERS
    case 'forks':
      return GithubManualStreamType.FORKS
    case 'pulls':
      return GithubManualStreamType.PULLS
    case 'issues':
      return GithubManualStreamType.ISSUES
    case 'discussions':
      return GithubManualStreamType.DISCUSSIONS
    case 'all':
      return GithubManualStreamType.ALL
    default:
      // wrong stream type
      return null
  }
}

// example call
// pnpm run script:process-repo 5f8b1a3a-0b0a-4c0a-8b0a-4c0a8b0a4c0a  CrowdDotDev/crowd.dev stars

// example call for all repos
// pnpm run script:process-repo 5f8b1a3a-0b0a-4c0a-8b0a-4c0a8b0a4c0a all forks -- this will trigger forks streams for all repos in settings

const log = getServiceLogger()

const processArguments = process.argv.slice(2)

const integrationId = processArguments[0]
const repoFullNames = processArguments[1] ? processArguments[1].split(',') : [] // it should be in format of owner/repo

// this is optional, if not provided we will trigger all streams
// if provided we will trigger only this stream type
// possible values are: stars, forks, pulls, issues, discussions, all
const streamString = processArguments.length > 2 ? processArguments[2] : null
const streamType = streamString ? mapStreamTypeToEnum(streamString) : GithubManualStreamType.ALL

setImmediate(async () => {
  if (!integrationId) {
    log.error(`Integration id is required!`)
    process.exit(1)
  }

  if (!repoFullNames.length) {
    log.error(`At least one repo full name is required!`)
    process.exit(1)
  }

  if (!streamType) {
    log.error(`Unknown stream type provided!`)
    process.exit(1)
  }

  const dbConnection = await getDbConnection(DB_CONFIG())
  const store = new DbStore(log, dbConnection)

  const queueClient = QueueFactory.createQueueService(QUEUE_CONFIG())
  const emitter = new IntegrationRunWorkerEmitter(queueClient, log)
  await emitter.init()

  const repo = new IntegrationRunRepository(store, log)

  const integration = await repo.getIntegrationData(integrationId)

  if (integration) {
    if (integration.state == IntegrationState.IN_PROGRESS) {
      log.warn(`Integration already running!`)
      process.exit(1)
    }

    if (integration.state == IntegrationState.INACTIVE) {
      log.warn(`Integration is not active!`)
      process.exit(1)
    }

    if (integration.state == IntegrationState.WAITING_APPROVAL) {
      log.warn(`Integration is waiting for approval!`)
      process.exit(1)
    }

    log.info(`Triggering integration run for ${integrationId}!`)

    // let's get current settings from integration
    const rawSettings = await repo.getIntegrationSettings(integrationId)
    const currentSettings = (await populateGithubSettingsWithRepos(
      dbStoreQx(store),
      integrationId,
      rawSettings,
    )) as GithubIntegrationSettings

    let repos = []
    if (repoFullNames[0] === 'all' && currentSettings.orgs[0].repos) {
      repos = currentSettings.orgs[0].repos
    } else {
      for (const repoFullName of repoFullNames) {
        const repoURL = `https://github.com/${repoFullName}`
        const repoExists = currentSettings.orgs[0].repos.find((r) => r.url === repoURL)
        if (!repoExists) {
          log.error(`Repo ${repoURL} is not configured in integration settings, skipping!`)
          continue
        }
        repos.push(repoExists)
      }
    }

    if (!repos.length) {
      log.error(`No valid repos found, exiting!`)
      process.exit(1)
    }

    const settings: GithubManualIntegrationSettings = {
      manualSettingsType: 'default',
      orgs: [
        {
          name: currentSettings.orgs[0].name,
          logo: currentSettings.orgs[0].logo,
          url: currentSettings.orgs[0].url,
          fullSync: currentSettings.orgs[0].fullSync,
          updatedAt: currentSettings.orgs[0].updatedAt,
          repos,
        },
      ],
      unavailableRepos: [],
      streamType,
    }

    await emitter.triggerIntegrationRun(
      integration.type,
      integration.id,
      false, // disable onboarding
      true, // this is to enable manual run
      settings, // we are injecting manual settings here
    )
  } else {
    log.error({ integrationId }, 'Integration not found!')
    process.exit(1)
  }
})
