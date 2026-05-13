import { getServiceLogger } from '@crowd/logging'
import {
  NangoIntegration,
  getNangoConnectionData,
  getNangoConnections,
  initNangoCloudClient,
  setNangoMetadata,
} from '@crowd/nango'

const log = getServiceLogger()

const REQUIRED_ENV_VARS = ['NANGO_CLOUD_SECRET_KEY', 'NANGO_CLOUD_INTEGRATIONS']

function validateEnvVars(): void {
  const missing: string[] = []
  for (const envVar of REQUIRED_ENV_VARS) {
    if (!process.env[envVar]) {
      missing.push(envVar)
    }
  }

  if (missing.length > 0) {
    log.error(`Missing required environment variables: ${missing.join(', ')}`)
    process.exit(1)
  }
}

const processArguments = process.argv.slice(2)

const executeMode = processArguments.includes('--execute')
const dryRunMode = !executeMode

if (dryRunMode) {
  log.info('Running in DRY-RUN mode. Use --execute flag to actually update connection metadata.')
}

setImmediate(async () => {
  validateEnvVars()

  await initNangoCloudClient()

  const allConnections = await getNangoConnections()

  // Get all github-token-* connection IDs
  const tokenConnectionIds = allConnections
    .filter(
      (c) =>
        c.provider_config_key === NangoIntegration.GITHUB &&
        c.connection_id.toLowerCase().startsWith('github-token-'),
    )
    .map((c) => c.connection_id)

  log.info(`Found ${tokenConnectionIds.length} github-token-* connections in Nango`)

  if (tokenConnectionIds.length === 0) {
    log.error('No github-token-* connections found in Nango. Nothing to do.')
    process.exit(1)
  }

  // Get all repo connections (non-token GitHub connections)
  const repoConnections = allConnections.filter(
    (c) =>
      c.provider_config_key === NangoIntegration.GITHUB &&
      !c.connection_id.toLowerCase().startsWith('github-token-'),
  )

  log.info(`Found ${repoConnections.length} GitHub repo connections to check`)

  let updatedCount = 0
  let skippedCount = 0
  let errorCount = 0
  let lastLoggedPercent = -1

  for (let i = 0; i < repoConnections.length; i++) {
    const repoConnection = repoConnections[i]

    const percent = Math.floor(((i + 1) / repoConnections.length) * 100)
    if (percent % 5 === 0 && percent !== lastLoggedPercent) {
      lastLoggedPercent = percent
      log.info(`Progress: ${i + 1}/${repoConnections.length} (${percent}%)`)
    }

    try {
      const data = await getNangoConnectionData(
        NangoIntegration.GITHUB,
        repoConnection.connection_id,
      )

      const metadata = data.metadata
      const existingTokenIds = (metadata.connection_ids as string[]) || []

      // Find token connections that are missing from this repo connection's metadata
      const missingTokenIds = tokenConnectionIds.filter((id) => !existingTokenIds.includes(id))

      if (missingTokenIds.length === 0) {
        skippedCount++
        continue
      }

      log.info(
        {
          connectionId: repoConnection.connection_id,
          existingCount: existingTokenIds.length,
          missingCount: missingTokenIds.length,
          missingTokenIds,
        },
        `Connection ${repoConnection.connection_id} is missing ${missingTokenIds.length} token connection(s)`,
      )

      if (executeMode) {
        const newMetadata = {
          ...metadata,
          connection_ids: [...existingTokenIds, ...missingTokenIds],
        }

        await setNangoMetadata(NangoIntegration.GITHUB, repoConnection.connection_id, newMetadata)

        log.info(
          { connectionId: repoConnection.connection_id },
          `Updated metadata for connection ${repoConnection.connection_id}`,
        )
      }

      updatedCount++
    } catch (err) {
      log.error(
        { connectionId: repoConnection.connection_id, err },
        `Failed to process connection ${repoConnection.connection_id}`,
      )
      errorCount++
    }
  }

  log.info('')
  log.info('='.repeat(80))
  log.info('SUMMARY')
  log.info('='.repeat(80))
  log.info(`Total repo connections: ${repoConnections.length}`)
  log.info(`${dryRunMode ? 'Would update' : 'Updated'}: ${updatedCount}`)
  log.info(`Already up-to-date: ${skippedCount}`)
  log.info(`Errors: ${errorCount}`)
  log.info('='.repeat(80))

  if (dryRunMode && updatedCount > 0) {
    log.info('To actually update these connections, run with --execute flag')
  }

  process.exit(0)
})
