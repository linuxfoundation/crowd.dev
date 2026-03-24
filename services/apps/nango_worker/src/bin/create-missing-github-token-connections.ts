import { READ_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceLogger } from '@crowd/logging'
import {
  NangoIntegration,
  createNangoGithubTokenConnection,
  getNangoConnections,
  initNangoCloudClient,
} from '@crowd/nango'

const log = getServiceLogger()

// Required environment variables
const REQUIRED_ENV_VARS = [
  'NANGO_CLOUD_SECRET_KEY',
  'NANGO_CLOUD_INTEGRATIONS',
  'CROWD_GITHUB_APP_ID',
  'CROWD_GITHUB_CLIENT_ID',
]

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
  log.info('Running in DRY-RUN mode. Use --execute flag to actually create connections.')
}

setImmediate(async () => {
  validateEnvVars()

  const appId = process.env.CROWD_GITHUB_APP_ID
  const clientId = process.env.CROWD_GITHUB_CLIENT_ID

  const db = await getDbConnection(READ_DB_CONFIG())
  const qx = pgpQx(db)

  await initNangoCloudClient()

  // Get all github-token-* connections from Nango
  const allConnections = await getNangoConnections()
  const tokenConnections = allConnections.filter(
    (c) =>
      c.provider_config_key === NangoIntegration.GITHUB &&
      c.connection_id.toLowerCase().startsWith('github-token-'),
  )

  log.info(`Found ${tokenConnections.length} github-token-* connections in Nango`)

  // Extract installation IDs from connection names (e.g., github-token-52165842 -> 52165842)
  const nangoInstallationIds = tokenConnections
    .map((c) => {
      const match = c.connection_id.match(/^github-token-(\d+)$/i)
      return match ? match[1] : null
    })
    .filter((id): id is string => id !== null)

  log.info(`Extracted ${nangoInstallationIds.length} installation IDs from Nango connections`)

  // Find all GitHub integrations in the database that are NOT in Nango
  const missingInstallations = await qx.select(
    `
    SELECT
      id,
      "integrationIdentifier",
      "tenantId",
      "segmentId",
      status,
      "createdAt"
    FROM integrations
    WHERE platform = 'github'
      AND "deletedAt" IS NULL
      AND "integrationIdentifier" IS NOT NULL
      ${nangoInstallationIds.length > 0 ? `AND "integrationIdentifier" NOT IN ($(nangoInstallationIds:csv))` : ''}
    ORDER BY "createdAt" DESC
    `,
    { nangoInstallationIds },
  )

  log.info(
    `Found ${missingInstallations.length} GitHub installations in database that are NOT in Nango`,
  )

  // Report existing github-token-* connections
  log.info('='.repeat(80))
  log.info('EXISTING GITHUB TOKEN CONNECTIONS IN NANGO')
  log.info('='.repeat(80))
  log.info(`Total: ${tokenConnections.length}`)

  log.info('')
  log.info('='.repeat(80))
  log.info('MISSING GITHUB INSTALLATIONS (in DB but not in Nango)')
  log.info('='.repeat(80))

  if (missingInstallations.length === 0) {
    log.info('All GitHub installations in the database are present in Nango.')
    log.info('='.repeat(80))
    process.exit(0)
  }

  log.info(`Total missing: ${missingInstallations.length}`)
  log.info('')

  if (dryRunMode) {
    log.info('DRY-RUN: The following connections would be created:')
    for (const installation of missingInstallations) {
      log.info(
        {
          connectionId: `github-token-${installation.integrationIdentifier}`,
          installationId: installation.integrationIdentifier,
          integrationId: installation.id,
          tenantId: installation.tenantId,
          segmentId: installation.segmentId,
          status: installation.status,
        },
        `Would create: github-token-${installation.integrationIdentifier}`,
      )
    }
    log.info('')
    log.info('='.repeat(80))
    log.info('To actually create these connections, run with --execute flag')
    log.info('='.repeat(80))
  } else {
    log.info('EXECUTE MODE: Creating missing connections...')
    log.info('')

    let successCount = 0
    let errorCount = 0
    let lastLoggedPercent = -1

    for (let i = 0; i < missingInstallations.length; i++) {
      const installation = missingInstallations[i]
      const installationId = installation.integrationIdentifier

      const percent = Math.floor(((i + 1) / missingInstallations.length) * 100)
      if (percent % 5 === 0 && percent !== lastLoggedPercent) {
        lastLoggedPercent = percent
        log.info(`Progress: ${i + 1}/${missingInstallations.length} (${percent}%)`)
      }

      try {
        log.info(
          {
            installationId,
            integrationId: installation.id,
            tenantId: installation.tenantId,
          },
          `Creating connection: github-token-${installationId}`,
        )

        const connectionId = await createNangoGithubTokenConnection(installationId, appId, clientId)

        log.info(
          { connectionId, installationId },
          `Successfully created connection: ${connectionId}`,
        )
        successCount++
      } catch (err) {
        log.error(
          {
            installationId,
            integrationId: installation.id,
            err,
          },
          `Failed to create connection for installation ${installationId}`,
        )
        errorCount++
      }
    }

    log.info('')
    log.info('='.repeat(80))
    log.info('SUMMARY')
    log.info('='.repeat(80))
    log.info(`Successfully created: ${successCount}`)
    log.info(`Failed: ${errorCount}`)
    log.info(`Total attempted: ${missingInstallations.length}`)
    log.info('='.repeat(80))
  }

  process.exit(0)
})
