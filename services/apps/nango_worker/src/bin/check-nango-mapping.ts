import { READ_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import {
  fetchNangoCursorRowsForIntegration,
  fetchNangoIntegrationData,
  getNangoMappingsForIntegrations,
} from '@crowd/data-access-layer/src/integrations'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getReposForGithubIntegration } from '@crowd/data-access-layer/src/repositories'
import { getServiceLogger } from '@crowd/logging'
import { PlatformType } from '@crowd/types'

const log = getServiceLogger()

interface Stats {
  totalRepos: number
  connectedRepos: number
  missingConnectionCount: number
  totalConnectionIds: number
  connectionIdsWithoutCursor: number
  connectionIdsWithCursor: number
}

function formatDelta(current: number, previous: number): string {
  const delta = current - previous
  if (delta === 0) return '(±0)'
  return delta > 0 ? `(+${delta})` : `(${delta})`
}

async function collectStats(): Promise<Stats> {
  const db = await getDbConnection(READ_DB_CONFIG())
  const integrations = await fetchNangoIntegrationData(pgpQx(db), [PlatformType.GITHUB_NANGO])

  let totalRepos = 0
  let connectedRepos = 0
  let missingConnectionCount = 0
  let totalConnectionIds = 0
  let connectionIdsWithoutCursor = 0

  const integrationsWithoutConnections = new Set<string>()
  const integrationsWithoutCursors = new Set<string>()

  const qx = pgpQx(db)

  for (const integration of integrations) {
    // Fetch nango mappings from the dedicated table
    const allNangoMappings = await getNangoMappingsForIntegrations(qx, [integration.id])
    const nangoMapping = allNangoMappings[integration.id] || {}
    const connectionIds = Object.keys(nangoMapping)

    // Track connectionIds that don't have cursors
    if (connectionIds.length > 0) {
      totalConnectionIds += connectionIds.length

      const cursorRows = await fetchNangoCursorRowsForIntegration(qx, integration.id)
      const connectionIdsWithCursorsSet = new Set(cursorRows.map((r) => r.connectionId))

      for (const connectionId of connectionIds) {
        if (!connectionIdsWithCursorsSet.has(connectionId)) {
          connectionIdsWithoutCursor++
          integrationsWithoutCursors.add(integration.id)
        }
      }
    }

    // Get repos from repositories table
    const repos = await getReposForGithubIntegration(qx, integration.id)
    for (const repo of repos) {
      totalRepos++

      if (connectionIds.length === 0) {
        missingConnectionCount++
        integrationsWithoutConnections.add(integration.id)
        continue
      }

      let found = false
      for (const mapping of Object.values(nangoMapping)) {
        if (mapping.owner === repo.owner && mapping.repoName === repo.name) {
          found = true
          break
        }
      }

      if (!found) {
        missingConnectionCount++
        integrationsWithoutConnections.add(integration.id)
      } else {
        connectedRepos++
      }
    }
  }

  log.info(
    `Integrations without connections: ${integrationsWithoutConnections.size} - ${Array.from(integrationsWithoutConnections).join(', ')}`,
  )
  log.info(
    `Integrations without cursors: ${integrationsWithoutCursors.size} - ${Array.from(integrationsWithoutCursors).join(', ')}`,
  )

  return {
    totalRepos,
    connectedRepos,
    missingConnectionCount,
    totalConnectionIds,
    connectionIdsWithoutCursor,
    connectionIdsWithCursor: totalConnectionIds - connectionIdsWithoutCursor,
  }
}

function printSummary(
  current: Stats,
  previous: Stats | null,
  baseline: Stats,
  iteration: number,
): void {
  const timestamp = new Date().toISOString()

  log.info('='.repeat(80))
  log.info(`ITERATION #${iteration} - ${timestamp}`)
  log.info('='.repeat(80))
  log.info('')
  log.info('REPOSITORIES:')
  log.info(
    `  Total repositories:                ${current.totalRepos}${previous ? ' ' + formatDelta(current.totalRepos, previous.totalRepos) + ' from last' : ''} ${formatDelta(current.totalRepos, baseline.totalRepos)} from baseline`,
  )
  log.info(
    `  Connected repositories (mapping):  ${current.connectedRepos}${previous ? ' ' + formatDelta(current.connectedRepos, previous.connectedRepos) + ' from last' : ''} ${formatDelta(current.connectedRepos, baseline.connectedRepos)} from baseline`,
  )
  log.info(
    `  Missing connection:                ${current.missingConnectionCount}${previous ? ' ' + formatDelta(current.missingConnectionCount, previous.missingConnectionCount) + ' from last' : ''} ${formatDelta(current.missingConnectionCount, baseline.missingConnectionCount)} from baseline`,
  )
  log.info('')
  log.info('CONNECTION IDS:')
  log.info(
    `  Total in nangoMapping:             ${current.totalConnectionIds}${previous ? ' ' + formatDelta(current.totalConnectionIds, previous.totalConnectionIds) + ' from last' : ''} ${formatDelta(current.totalConnectionIds, baseline.totalConnectionIds)} from baseline`,
  )
  log.info(
    `  Without cursor:                    ${current.connectionIdsWithoutCursor}${previous ? ' ' + formatDelta(current.connectionIdsWithoutCursor, previous.connectionIdsWithoutCursor) + ' from last' : ''} ${formatDelta(current.connectionIdsWithoutCursor, baseline.connectionIdsWithoutCursor)} from baseline`,
  )
  log.info(
    `  With cursor:                       ${current.connectionIdsWithCursor}${previous ? ' ' + formatDelta(current.connectionIdsWithCursor, previous.connectionIdsWithCursor) + ' from last' : ''} ${formatDelta(current.connectionIdsWithCursor, baseline.connectionIdsWithCursor)} from baseline`,
  )
  log.info('='.repeat(80))
  log.info('')
}

setImmediate(async () => {
  let iteration = 0
  let baseline: Stats | null = null
  let previous: Stats | null = null

  // eslint-disable-next-line no-constant-condition
  while (true) {
    iteration++

    const current = await collectStats()

    if (!baseline) {
      baseline = current
    }

    printSummary(current, previous, baseline, iteration)

    previous = current

    // Wait 60 seconds before next iteration
    await new Promise((resolve) => setTimeout(resolve, 60000))
  }
})
