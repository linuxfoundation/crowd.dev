/**
 * Gerrit Activities Cleanup Script
 *
 * PROBLEM:
 * Gerrit activities need to be cleaned up from both PostgreSQL and Tinybird.
 *
 * SOLUTION:
 * This script deletes activities from the Gerrit platform across:
 * - PostgreSQL (activityRelations table only)
 * - Tinybird (activities and activityRelations datasources)
 *
 * Filters (all optional):
 * - platform = 'gerrit'  (always applied)
 * - --type <types>       (optional) restrict to specific activity type(s)
 * - --before-date <date> (optional) restrict to records with updatedAt < date
 *
 * WARNING: Running with no --type and no --before-date will delete ALL gerrit
 * activities regardless of type or age.
 *
 * Usage:
 *   # Via package.json script (recommended):
 *   pnpm run cleanup-gerrit-activities -- [options]
 *
 *   # Or directly with tsx:
 *   npx tsx src/bin/cleanup-gerrit-activities.ts [options]
 *
 * Options:
 *   --dry-run                  Display what would be deleted without actually deleting anything
 *   --tb-token <token>         Tinybird API token (overrides CROWD_TINYBIRD_ACTIVITIES_TOKEN)
 *   --before-date <YYYY-MM-DD> Only delete records with updatedAt before this date
 *   --type <types>             Only delete activities of these types (comma-separated).
 *                              Valid values: changeset-created, changeset-merged, changeset-closed, changeset-abandoned, changeset_comment-created, patchset-created, patchset_comment-created, patchset_approval-created
 *
 * Environment Variables Required:
 *   CROWD_DB_WRITE_HOST - Postgres write host
 *   CROWD_DB_PORT - Postgres port
 *   CROWD_DB_USERNAME - Postgres username
 *   CROWD_DB_PASSWORD - Postgres password
 *   CROWD_DB_DATABASE - Postgres database name
 *   CROWD_TINYBIRD_BASE_URL - Tinybird API base URL
 *   CROWD_TINYBIRD_ACTIVITIES_TOKEN - Tinybird API token
 */
import * as fs from 'fs'
import * as path from 'path'

import {
  TinybirdClient,
  WRITE_DB_CONFIG,
  getDbConnection,
} from '@crowd/data-access-layer/src/database'
import { QueryExecutor, pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceChildLogger } from '@crowd/logging'

const log = getServiceChildLogger('cleanup-gerrit-activities-script')

const VALID_GERRIT_TYPES = [
  'changeset-created',
  'changeset-merged',
  'changeset-closed',
  'changeset-abandoned',
  'changeset_comment-created',
  'patchset-created',
  'patchset_comment-created',
  'patchset_approval-created',
] as const

interface Filters {
  beforeDate?: string
  types?: string[]
}

function buildGerritFilterClause(filters: Filters): string {
  const parts = [`platform = 'gerrit'`]
  if (filters.types?.length) {
    const list = filters.types.map((t) => `'${t}'`).join(', ')
    parts.push(`type IN (${list})`)
  }
  if (filters.beforeDate) {
    parts.push(`updatedAt < '${filters.beforeDate}'`)
  }
  return parts.join(' AND ')
}

interface DeletionStatus {
  success: boolean
  jobId?: string
  error?: string
}

interface CleanupResult {
  status: 'success' | 'failure'
  startTime: string
  endTime: string
  totalBatches: number
  failedBatches: number
  deletions: {
    postgres: DeletionStatus
    tinybird: {
      activities: DeletionStatus
      activityRelations: DeletionStatus
    }
  }
}

/**
 * Initialize Postgres connection using QueryExecutor
 */
async function initPostgresClient(): Promise<QueryExecutor> {
  log.info('Initializing Postgres connection...')

  const dbConnection = await getDbConnection(WRITE_DB_CONFIG())
  const queryExecutor = pgpQx(dbConnection)

  log.info('Postgres connection established')
  return queryExecutor
}

/**
 * Query activity IDs from Tinybird in batches and delete from Postgres immediately
 * Uses batched queries to avoid hitting Tinybird's result size limit (100 MiB)
 */
async function queryAndProcessActivityIdsInBatches(
  tinybird: TinybirdClient,
  postgres: QueryExecutor,
  dryRun: boolean,
  filters: Filters,
  onBatchProcessed: () => void,
): Promise<number> {
  log.info('Querying activity IDs from Tinybird for Gerrit cleanup...')

  const BATCH_SIZE = 10000
  let offset = 0
  let hasMore = true
  let totalProcessed = 0
  let batchNumber = 0
  const whereClause = buildGerritFilterClause(filters)

  try {
    while (hasMore) {
      const query = `SELECT DISTINCT activityId FROM activityRelations WHERE ${whereClause} ORDER BY activityId LIMIT ${BATCH_SIZE} OFFSET ${offset} FORMAT JSON`
      log.info(`Querying batch: offset=${offset}, limit=${BATCH_SIZE}`)

      const result = await tinybird.executeSql<{ data: Array<{ activityId: string }> }>(query)
      const batchActivityIds = result.data.map((row) => row.activityId)

      if (batchActivityIds.length === 0) {
        hasMore = false
      } else {
        batchNumber++
        log.info(
          `Processing batch ${batchNumber} (${batchActivityIds.length} activities, total processed: ${totalProcessed})...`,
        )

        const postgresStatus = await deleteActivityRelationsFromPostgres(
          postgres,
          batchActivityIds,
          dryRun,
        )

        if (!postgresStatus.success) {
          log.error(`Failed to delete batch ${batchNumber} from Postgres: ${postgresStatus.error}`)
        }

        totalProcessed += batchActivityIds.length
        onBatchProcessed()

        // If we got fewer results than the batch size, we've reached the end
        if (batchActivityIds.length < BATCH_SIZE) {
          hasMore = false
        } else {
          offset += BATCH_SIZE
        }
      }
    }

    log.info(`Found and processed ${totalProcessed} total activity ID(s) in Tinybird`)
    return totalProcessed
  } catch (error) {
    const statusCode = error?.response?.status || 'unknown'
    const responseBody = error?.response?.data
      ? JSON.stringify(error.response.data)
      : error?.response?.body || 'no body'
    log.error(
      `Failed to query activity IDs from Tinybird: ${error.message} (status: ${statusCode}, body: ${responseBody})`,
    )
    throw error
  }
}

/**
 * Delete activity relations from Postgres using activity IDs
 */
async function deleteActivityRelationsFromPostgres(
  postgres: QueryExecutor,
  activityIds: string[],
  dryRun = false,
): Promise<DeletionStatus> {
  if (activityIds.length === 0) {
    log.info(`No activity IDs to ${dryRun ? 'query' : 'delete'} from Postgres`)
    return { success: true }
  }

  try {
    if (dryRun) {
      log.info(`[DRY RUN] Querying ${activityIds.length} activity relations from Postgres...`)
      const query = `
        SELECT COUNT(*) as count
        FROM "activityRelations"
        WHERE "activityId" IN ($(activityIds:csv))
      `
      const result = (await postgres.selectOne(query, { activityIds })) as { count: string }
      const rowCount = parseInt(result.count, 10)
      log.info(`[DRY RUN] Would delete ${rowCount} activity relation(s) from Postgres`)
      return { success: true }
    }

    log.info(`Deleting ${activityIds.length} activity relations from Postgres...`)
    const query = `
      DELETE FROM "activityRelations"
      WHERE "activityId" IN ($(activityIds:csv))
    `
    const rowCount = await postgres.result(query, { activityIds })
    log.info(`✓ Deleted ${rowCount} activity relation(s) from Postgres`)
    return { success: true }
  } catch (error) {
    log.error(`Failed to delete activity relations from Postgres: ${error.message}`)
    return { success: false, error: error.message }
  }
}

/**
 * Delete activities from Tinybird using the delete API
 */
async function deleteActivitiesFromTinybird(
  tinybird: TinybirdClient,
  dryRun = false,
  filters: Filters = {},
): Promise<{
  activities: DeletionStatus
  activityRelations: DeletionStatus
  jobIds: string[]
}> {
  const results = {
    activities: { success: false } as DeletionStatus,
    activityRelations: { success: false } as DeletionStatus,
  }

  const deleteCondition = buildGerritFilterClause(filters)

  if (dryRun) {
    log.info('[DRY RUN] Would delete activities from Tinybird using Gerrit filters...')
    log.info(`[DRY RUN] Condition: ${deleteCondition}`)
    log.info(`[DRY RUN] Would delete from 'activities' datasource`)
    log.info(`[DRY RUN] Would delete from 'activityRelations' datasource`)
    return {
      activities: { success: true },
      activityRelations: { success: true },
      jobIds: [],
    }
  }

  log.info('Deleting activities from Tinybird using Gerrit filters...')

  const triggeredJobIds: string[] = []

  // Define deletion conditions
  const activitiesDeleteCondition = deleteCondition
  const activityRelationsDeleteCondition = deleteCondition

  // Delete from activities datasource
  try {
    log.info('Triggering deletion job for activities datasource...')
    const activitiesJobResponse = await tinybird.deleteDatasource(
      'activities',
      activitiesDeleteCondition,
      true,
      false, // Don't wait
    )
    log.info(`✓ Triggered deletion job for activities (job_id: ${activitiesJobResponse.job_id})`)
    triggeredJobIds.push(activitiesJobResponse.job_id)
    results.activities = {
      success: true,
      jobId: activitiesJobResponse.job_id,
    }
  } catch (error) {
    log.error(`Failed to trigger deletion job for activities datasource: ${error.message}`)
    results.activities = {
      success: false,
      error: error.message,
    }
  }

  // Delete from activityRelations datasource
  try {
    log.info('Triggering deletion job for activityRelations datasource...')
    const activityRelationsJobResponse = await tinybird.deleteDatasource(
      'activityRelations',
      activityRelationsDeleteCondition,
      true,
      false, // Don't wait
    )
    log.info(
      `✓ Triggered deletion job for activityRelations (job_id: ${activityRelationsJobResponse.job_id})`,
    )
    triggeredJobIds.push(activityRelationsJobResponse.job_id)
    results.activityRelations = {
      success: true,
      jobId: activityRelationsJobResponse.job_id,
    }
  } catch (error) {
    log.error(`Failed to trigger deletion job for activityRelations datasource: ${error.message}`)
    results.activityRelations = {
      success: false,
      error: error.message,
    }
  }

  log.info(`✓ All deletion jobs triggered (${triggeredJobIds.length} running in background)`)

  return {
    ...results,
    jobIds: triggeredJobIds,
  }
}

/**
 * Main cleanup process
 */
async function runCleanup(dryRun = false, tbToken?: string, filters: Filters = {}): Promise<void> {
  const startTime = new Date().toISOString()
  let failedBatches = 0
  let totalBatches = 0

  if (dryRun) {
    log.info(`\n${'='.repeat(80)}`)
    log.info(`[DRY RUN MODE] Gerrit Activities Cleanup`)
    log.info(`${'='.repeat(80)}`)
  } else {
    log.info(`\n${'='.repeat(80)}`)
    log.info(`Gerrit Activities Cleanup`)
    log.info(`${'='.repeat(80)}`)
  }

  log.info(`Active filters: ${buildGerritFilterClause(filters)}`)

  try {
    // Initialize database clients
    const postgres = await initPostgresClient()
    const tinybird = new TinybirdClient(tbToken)

    // Track deletion statuses
    const allDeletionStatuses = {
      postgres: { success: true } as DeletionStatus,
      tinybird: {
        activities: { success: true } as DeletionStatus,
        activityRelations: { success: true } as DeletionStatus,
      },
    }

    // Step 1: Query activity IDs from Tinybird in batches and delete from Postgres as we go
    log.info(
      'Step 1: Processing activity IDs in batches from Tinybird and deleting from Postgres...',
    )
    const totalProcessed = await queryAndProcessActivityIdsInBatches(
      tinybird,
      postgres,
      dryRun,
      filters,
      () => {
        totalBatches++
      },
    )

    if (totalProcessed === 0) {
      log.info('No activities to delete, skipping Tinybird deletion steps')
      log.info(`✓ Completed ${dryRun ? 'dry run for' : 'cleanup for'} Gerrit activities`)
      return
    }

    log.info(`✓ Completed processing ${totalProcessed} activities from Postgres`)

    // Step 2: Delete from Tinybird in a single operation per datasource
    log.info('Step 2: Triggering Tinybird deletions...')
    const tinybirdStatuses = await deleteActivitiesFromTinybird(tinybird, dryRun, filters)

    // Track failures from Tinybird
    if (!tinybirdStatuses.activities.success) {
      allDeletionStatuses.tinybird.activities = tinybirdStatuses.activities
      failedBatches++
    }
    if (!tinybirdStatuses.activityRelations.success) {
      allDeletionStatuses.tinybird.activityRelations = tinybirdStatuses.activityRelations
      failedBatches++
    }

    // Wait for all Tinybird deletion jobs to complete
    if (!dryRun && tinybirdStatuses.jobIds.length > 0) {
      log.info(
        `Waiting for ${tinybirdStatuses.jobIds.length} Tinybird deletion job(s) to complete...`,
      )
      try {
        await tinybird.waitForJobs(tinybirdStatuses.jobIds, 60000, 3600000) // 1min interval, 1h timeout
        log.info(`✓ All Tinybird deletion jobs completed`)
      } catch (error) {
        log.error(`Failed to wait for Tinybird deletion jobs: ${error.message}`)
        // Continue anyway - jobs are still running in background
      }
    }

    // Create cleanup result
    const endTime = new Date().toISOString()
    const result: CleanupResult = {
      status: failedBatches > 0 ? 'failure' : 'success',
      startTime,
      endTime,
      totalBatches,
      failedBatches,
      deletions: allDeletionStatuses,
    }

    // Save results to file
    const jsonFilePath = path.join(
      '/tmp',
      `cleanup_gerrit_activities_${new Date().toISOString().replace(/[:.]/g, '-')}.json`,
    )
    try {
      fs.writeFileSync(jsonFilePath, JSON.stringify(result, null, 2), 'utf-8')
      log.info(`✓ Cleanup results saved to: ${jsonFilePath}`)
    } catch (error) {
      log.error(`Failed to write cleanup results to ${jsonFilePath}: ${error.message}`)
    }

    // Summary
    log.info(`\n${'='.repeat(80)}`)
    log.info('Cleanup Summary')
    log.info(`${'='.repeat(80)}`)
    log.info(`✓ Activities ${dryRun ? 'found' : 'deleted'}: ${totalProcessed}`)
    log.info(`✓ Batches processed: ${totalBatches}`)
    if (failedBatches > 0) {
      log.warn(`✗ Failed batches: ${failedBatches}`)
    }

    if (tinybirdStatuses.activities.success) {
      log.info(
        `✓ Tinybird activities deletion job ${dryRun ? 'would be' : 'was'} triggered: ${tinybirdStatuses.activities.jobId || 'N/A'}`,
      )
    } else {
      log.error(`✗ Tinybird activities deletion failed: ${tinybirdStatuses.activities.error}`)
    }

    if (tinybirdStatuses.activityRelations.success) {
      log.info(
        `✓ Tinybird activityRelations deletion job ${dryRun ? 'would be' : 'was'} triggered: ${tinybirdStatuses.activityRelations.jobId || 'N/A'}`,
      )
    } else {
      log.error(
        `✗ Tinybird activityRelations deletion failed: ${tinybirdStatuses.activityRelations.error}`,
      )
    }

    if (result.status === 'failure') {
      process.exit(1)
    }
  } catch (error) {
    log.error(`Failed to run Gerrit cleanup: ${error.message}`)
    throw error
  }
}

/**
 * Main entry point
 */
async function main() {
  const args = process.argv.slice(2)

  // Parse flags
  const dryRunIndex = args.indexOf('--dry-run')
  const tbTokenIndex = args.indexOf('--tb-token')
  const beforeDateIndex = args.indexOf('--before-date')
  const typeIndex = args.indexOf('--type')
  const dryRun = dryRunIndex !== -1

  // Extract tb-token value if provided
  let tbToken: string | undefined
  if (tbTokenIndex !== -1) {
    if (tbTokenIndex + 1 >= args.length) {
      log.error('Error: --tb-token requires a value')
      process.exit(1)
    }
    tbToken = args[tbTokenIndex + 1]
  }

  // Extract and validate --before-date
  let beforeDate: string | undefined
  if (beforeDateIndex !== -1) {
    if (beforeDateIndex + 1 >= args.length) {
      log.error('Error: --before-date requires a value (YYYY-MM-DD)')
      process.exit(1)
    }
    const raw = args[beforeDateIndex + 1]
    if (!/^\d{4}-\d{2}-\d{2}$/.test(raw) || !isFinite(Date.parse(raw))) {
      log.error(`Error: --before-date value "${raw}" is not a valid date (expected YYYY-MM-DD)`)
      process.exit(1)
    }
    beforeDate = raw
  }

  // Extract and validate --type
  let types: string[] | undefined
  if (typeIndex !== -1) {
    if (typeIndex + 1 >= args.length) {
      log.error('Error: --type requires a value (comma-separated list of activity types)')
      process.exit(1)
    }
    const raw = args[typeIndex + 1]
    const parsed = raw.split(',').map((t) => t.trim()).filter(Boolean)
    if (parsed.length === 0) {
      log.error('Error: --type received an empty value')
      process.exit(1)
    }
    const invalid = parsed.filter((t) => !(VALID_GERRIT_TYPES as readonly string[]).includes(t))
    if (invalid.length > 0) {
      log.error(
        `Error: --type contains invalid value(s): ${invalid.join(', ')}. Valid values: ${VALID_GERRIT_TYPES.join(', ')}`,
      )
      process.exit(1)
    }
    types = parsed
  }

  const filters: Filters = { beforeDate, types }

  // Check for help flag or no valid arguments
  if (args.includes('--help') || args.includes('-h')) {
    log.info(`
      Usage:
        # Via package.json script (recommended):
        pnpm run cleanup-gerrit-activities -- [options]

        # Or directly with tsx:
        npx tsx src/bin/cleanup-gerrit-activities.ts [options]

      Options:
        --dry-run                  (optional) Display what would be deleted without actually deleting anything
        --tb-token <token>         (optional) Tinybird API token (overrides CROWD_TINYBIRD_ACTIVITIES_TOKEN)
        --before-date <YYYY-MM-DD> (optional) Only delete records with updatedAt before this date
        --type <types>             (optional) Comma-separated list of activity types to delete.
                                   Valid values: ${VALID_GERRIT_TYPES.join(', ')}

      WARNING: Running with no --type and no --before-date deletes ALL gerrit activities.

      Examples:
        # Delete all gerrit activities (no filters)
        pnpm run cleanup-gerrit-activities

        # Delete only activities before 2025-12-15
        pnpm run cleanup-gerrit-activities -- --before-date 2025-12-15

        # Delete only changeset-merged activities
        pnpm run cleanup-gerrit-activities -- --type changeset-merged

        # Combine filters
        pnpm run cleanup-gerrit-activities -- --type changeset-merged,changeset-abandoned --before-date 2025-06-01

        # Dry run to preview what would be deleted
        pnpm run cleanup-gerrit-activities -- --dry-run --before-date 2025-12-15
    `)
    process.exit(0)
  }

  if (dryRun) {
    log.info(`\n${'='.repeat(80)}`)
    log.info('DRY RUN MODE - No data will be deleted')
    log.info(`${'='.repeat(80)}\n`)
  }

  try {
    await runCleanup(dryRun, tbToken, filters)
  } catch (error) {
    log.error(error, 'Failed to run Gerrit cleanup script')
    log.error(`\nError: ${error.message}`)
    process.exit(1)
  }
}

main().catch((error) => {
  log.error('Unexpected error:', error)
  process.exit(1)
})
