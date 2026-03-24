import axios from 'axios'

import { IntegrationRunWorkerEmitter } from '@crowd/common_services'
import { DbStore, getDbConnection } from '@crowd/data-access-layer/src/database'
import IntegrationRunRepository from '@crowd/data-access-layer/src/old/apps/integration_run_worker/integrationRun.repo'
import { dbStoreQx } from '@crowd/data-access-layer/src/queryExecutor'
import { populateGithubSettingsWithRepos } from '@crowd/data-access-layer/src/repositories'
import {
  GithubIntegrationSettings,
  GithubManualIntegrationSettings,
  GithubManualStreamType,
  Repo,
} from '@crowd/integrations'
import { getServiceLogger } from '@crowd/logging'
import { QueueFactory } from '@crowd/queue'
import { IntegrationState } from '@crowd/types'

import { DB_CONFIG, LOKI_DB_CONFIG, QUEUE_CONFIG } from '../conf'

const query = (integrationId: string) => {
  return {
    requests: [
      {
        type: 'execute',
        stmt: {
          sql: `SELECT 
                    repoName, group_concat(metricName, ',') AS metrics 
                from 
                    githubCache 
                where 
                    integrationId = '${integrationId}' and dbValue / remoteValue <= 0.98 and metricName != 'commitsOnMain'
                group by
                    repoName
                order by
                    repoName;
                `,
        },
      },
      {
        type: 'close',
      },
    ],
  }
}

interface DataItem {
  type: string
  value: string
}

type Rows = Array<Array<DataItem>>

const lokiMetricToStreamType = (metric: string): GithubManualStreamType => {
  switch (metric) {
    case 'stars':
      return GithubManualStreamType.STARGAZERS
    case 'forks':
      return GithubManualStreamType.FORKS
    case 'totalIssues':
      return GithubManualStreamType.ISSUES
    case 'totalPRs':
      return GithubManualStreamType.PULLS
    default:
      return null
  }
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function mapToObject(map: Map<any, any>) {
  const object = {}
  for (const [key, value] of map) {
    object[key] = value
  }
  return object
}

// example call
// pnpm run script:auto-heal-github-integration 5f8b1a3a-0b0a-4c0a-8b0a-4c0a8b0a4c0a

const log = getServiceLogger()

const processArguments = process.argv.slice(2)

const integrationId = processArguments[0]

setImmediate(async () => {
  if (!integrationId) {
    log.error(`Integration id is required!`)
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

    // get stats for integration data quality

    const lokiDbConfig = LOKI_DB_CONFIG()

    //eslint-disable-next-line @typescript-eslint/no-explicit-any
    let response: any
    try {
      log.info(`Querying loki db for ${integrationId}!`)
      response = await axios.post(`${lokiDbConfig.url}/v2/pipeline`, query(integrationId), {
        headers: {
          Authorization: `Bearer ${lokiDbConfig.token}`,
        },
      })
    } catch (error) {
      log.error({ error }, 'Error while querying loki db!')
      process.exit(1)
    }

    const rows: Rows = response?.data?.results?.[0]?.response?.result?.rows

    log.info(`Fetched ${rows.length} rows from loki db!`)

    if (!rows || !rows.length) {
      log.error('No rows found from loki db!')
      process.exit(1)
    }

    const repoFullNames = []
    const partialMap = new Map<string, Array<GithubManualStreamType>>()
    const detailedMap = new Map<string, Array<GithubManualStreamType>>()

    for (const row of rows) {
      const repoFullName = row[0].value
      const metrics = row[1].value.split(',')
      repoFullNames.push(repoFullName)

      for (const metric of metrics) {
        log.info(`Processing metric ${metric} for ${repoFullName}!`)
        const streamType = lokiMetricToStreamType(metric)
        if (!streamType) {
          log.error(`Unknown metric ${metric} found!`)
          process.exit(1)
        }
        if (streamType) {
          if (partialMap.has(repoFullName)) {
            partialMap.get(repoFullName).push(streamType)
          } else {
            partialMap.set(repoFullName, [streamType])
          }
        }
      }
    }

    log.info({ partialMap: mapToObject(partialMap) }, 'Partial map')
    log.info(`Triggering integration run for ${integrationId}!`)

    // let's get current settings from integration
    const rawSettings = await repo.getIntegrationSettings(integrationId)
    const currentSettings = (await populateGithubSettingsWithRepos(
      dbStoreQx(store),
      integrationId,
      rawSettings,
    )) as GithubIntegrationSettings

    const repos: Repo[] = []

    for (const repoFullName of repoFullNames) {
      const repoExists = currentSettings.orgs[0].repos.find((r) => r.url === repoFullName)
      if (!repoExists) {
        log.error(`Repo ${repoFullName} is not configured in integration settings, skipping!`)
        continue
      }
      repos.push(repoExists)
    }

    if (!repos.length) {
      log.error(`No valid repos found, exiting!`)
      process.exit(1)
    }

    // let's build detailed map
    for (const repo of repos) {
      const repoFullName = repo.url
      if (partialMap.has(repoFullName)) {
        detailedMap.set(repoFullName, partialMap.get(repoFullName))
      } else {
        log.error(`No metrics found for ${repoFullName} when building detailed map!`)
      }
    }

    if (!detailedMap.size) {
      log.error(`No valid repos found, exiting!`)
      process.exit(1)
    }

    log.info({ detailedMap: mapToObject(detailedMap) }, 'Detailed map')

    const settings: GithubManualIntegrationSettings = {
      manualSettingsType: 'detailed_map',
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
      map: mapToObject(detailedMap),
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
