/**
 * Activities Cleanup Script (by platform + type)
 *
 * Deletes activities from PostgreSQL (activityRelations) and Tinybird
 * (activities and activityRelations datasources) for a given platform and
 * one or more activity types, optionally bounded by a date cutoff.
 *
 * Usage:
 *   pnpm run cleanup-activities-by-platform-and-type -- \
 *     --platform <platform> \
 *     --types <type1,type2,...> \
 *     [--before <YYYY-MM-DD>] \
 *     [--dry-run] \
 *     [--tb-token <token>]
 *
 * Required:
 *   --platform   Platform name (e.g. 'gitlab', 'gerrit')
 *   --types      Comma-separated activity types (e.g. 'merge_request-closed')
 *
 * Optional:
 *   --before     Only delete rows with updatedAt < this date (YYYY-MM-DD)
 *   --dry-run    Report what would be deleted without deleting
 *   --tb-token   Override CROWD_TINYBIRD_ACTIVITIES_TOKEN
 *
 * Environment Variables Required:
 *   CROWD_DB_WRITE_HOST, CROWD_DB_PORT, CROWD_DB_USERNAME, CROWD_DB_PASSWORD,
 *   CROWD_DB_DATABASE, CROWD_TINYBIRD_BASE_URL, CROWD_TINYBIRD_ACTIVITIES_TOKEN
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

const log = getServiceChildLogger('cleanup-activities-by-platform-and-type-script')

// Identifiers passed to Tinybird filters are interpolated, not parameterized,
// so guard against injection by requiring conservative character sets.
const IDENTIFIER_PATTERN = /^[A-Za-z0-9_-]+$/
const DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/

interface DeletionStatus {
  success: boolean
  jobId?: string
  error?: string
}

interface CleanupFilters {
  platform: string
  types: string[]
  before?: string
}

interface CleanupResult {
  status: 'success' | 'failure'
  startTime: string
  endTime: string
  filters: CleanupFilters
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

function quote(value: string): string {
  return `'${value.replace(/'/g, "''")}'`
}

function buildWhereClause(filters: CleanupFilters): string {
  const clauses = [
    `platform = ${quote(filters.platform)}`,
    `type IN (${filters.types.map(quote).join(', ')})`,
  ]
  if (filters.before) {
    clauses.push(`updatedAt < ${quote(filters.before)}`)
  }
  return clauses.join(' AND ')
}

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
  filters: CleanupFilters,
  dryRun: boolean,
  onBatchProcessed: () => void,
): Promise<number> {
  log.info('Querying activity IDs from Tinybird...')

  const where = buildWhereClause(filters)
  const BATCH_SIZE = 10000
  let offset = 0
  let hasMore = true
  let totalProcessed = 0
  let batchNumber = 0

  try {
    while (hasMore) {
      const query = `SELECT DISTINCT activityId FROM activityRelations WHERE ${where} ORDER BY activityId LIMIT ${BATCH_SIZE} OFFSET ${offset} FORMAT JSON`
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

async function deleteActivitiesFromTinybird(
  tinybird: TinybirdClient,
  filters: CleanupFilters,
  dryRun = false,
): Promise<{
  activities: DeletionStatus
  activityRelations: DeletionStatus
  jobIds: string[]
}> {
  const results = {
    activities: { success: false } as DeletionStatus,
    activityRelations: { success: false } as DeletionStatus,
  }

  const deleteCondition = buildWhereClause(filters)

  if (dryRun) {
    log.info('[DRY RUN] Would delete activities from Tinybird...')
    log.info(`[DRY RUN] Condition: ${deleteCondition}`)
    log.info(`[DRY RUN] Would delete from 'activities' datasource`)
    log.info(`[DRY RUN] Would delete from 'activityRelations' datasource`)
    return {
      activities: { success: true },
      activityRelations: { success: true },
      jobIds: [],
    }
  }

  log.info(`Deleting activities from Tinybird with condition: ${deleteCondition}`)

  const triggeredJobIds: string[] = []

  try {
    log.info('Triggering deletion job for activities datasource...')
    const activitiesJobResponse = await tinybird.deleteDatasource(
      'activities',
      deleteCondition,
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

  try {
    log.info('Triggering deletion job for activityRelations datasource...')
    const activityRelationsJobResponse = await tinybird.deleteDatasource(
      'activityRelations',
      deleteCondition,
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

async function runCleanup(
  filters: CleanupFilters,
  dryRun = false,
  tbToken?: string,
): Promise<void> {
  const startTime = new Date().toISOString()
  let failedBatches = 0
  let totalBatches = 0

  log.info(`\n${'='.repeat(80)}`)
  log.info(`${dryRun ? '[DRY RUN MODE] ' : ''}Activities Cleanup`)
  log.info(`Filters: ${buildWhereClause(filters)}`)
  log.info(`${'='.repeat(80)}`)

  try {
    const postgres = await initPostgresClient()
    const tinybird = new TinybirdClient(tbToken)

    const allDeletionStatuses = {
      postgres: { success: true } as DeletionStatus,
      tinybird: {
        activities: { success: true } as DeletionStatus,
        activityRelations: { success: true } as DeletionStatus,
      },
    }

    log.info(
      'Step 1: Processing activity IDs in batches from Tinybird and deleting from Postgres...',
    )
    const totalProcessed = await queryAndProcessActivityIdsInBatches(
      tinybird,
      postgres,
      filters,
      dryRun,
      () => {
        totalBatches++
      },
    )

    if (totalProcessed === 0) {
      log.info('No activities to delete, skipping Tinybird deletion steps')
      log.info(`✓ Completed ${dryRun ? 'dry run' : 'cleanup'}`)
      return
    }

    log.info(`✓ Completed processing ${totalProcessed} activities from Postgres`)

    log.info('Step 2: Triggering Tinybird deletions...')
    const tinybirdStatuses = await deleteActivitiesFromTinybird(tinybird, filters, dryRun)

    if (!tinybirdStatuses.activities.success) {
      allDeletionStatuses.tinybird.activities = tinybirdStatuses.activities
      failedBatches++
    }
    if (!tinybirdStatuses.activityRelations.success) {
      allDeletionStatuses.tinybird.activityRelations = tinybirdStatuses.activityRelations
      failedBatches++
    }

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

    const endTime = new Date().toISOString()
    const result: CleanupResult = {
      status: failedBatches > 0 ? 'failure' : 'success',
      startTime,
      endTime,
      filters,
      totalBatches,
      failedBatches,
      deletions: allDeletionStatuses,
    }

    const safeSuffix = `${filters.platform}_${filters.types.join('-')}`.replace(
      /[^A-Za-z0-9_-]/g,
      '_',
    )
    const jsonFilePath = path.join(
      '/tmp',
      `cleanup_activities_${safeSuffix}_${new Date().toISOString().replace(/[:.]/g, '-')}.json`,
    )
    try {
      fs.writeFileSync(jsonFilePath, JSON.stringify(result, null, 2), 'utf-8')
      log.info(`✓ Cleanup results saved to: ${jsonFilePath}`)
    } catch (error) {
      log.error(`Failed to write cleanup results to ${jsonFilePath}: ${error.message}`)
    }

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
    log.error(`Failed to run cleanup: ${error.message}`)
    throw error
  }
}

function getFlagValue(args: string[], name: string): string | undefined {
  const idx = args.indexOf(name)
  if (idx === -1) return undefined
  if (idx + 1 >= args.length) {
    log.error(`Error: ${name} requires a value`)
    process.exit(1)
  }
  return args[idx + 1]
}

function printHelp(): void {
  log.info(`
    Usage:
      pnpm run cleanup-activities-by-platform-and-type -- \\
        --platform <platform> \\
        --types <type1,type2,...> \\
        [--before <YYYY-MM-DD>] \\
        [--dry-run] \\
        [--tb-token <token>]

    Required:
      --platform   Platform name (e.g. 'gitlab', 'gerrit')
      --types      Comma-separated activity types (e.g. 'merge_request-closed')

    Optional:
      --before     Only delete rows with updatedAt < this date (YYYY-MM-DD)
      --dry-run    Report what would be deleted without deleting
      --tb-token   Override CROWD_TINYBIRD_ACTIVITIES_TOKEN

    Examples:
      # Dry run: see how many gitlab merge_request-closed rows would be deleted
      pnpm run cleanup-activities-by-platform-and-type -- \\
        --platform gitlab --types merge_request-closed --dry-run

      # Actual cleanup
      pnpm run cleanup-activities-by-platform-and-type -- \\
        --platform gitlab --types merge_request-closed
  `)
}

async function main() {
  const args = process.argv.slice(2)

  if (args.includes('--help') || args.includes('-h') || args.length === 0) {
    printHelp()
    process.exit(args.length === 0 ? 1 : 0)
  }

  const dryRun = args.includes('--dry-run')
  const tbToken = getFlagValue(args, '--tb-token')
  const platform = getFlagValue(args, '--platform')
  const typesArg = getFlagValue(args, '--types')
  const before = getFlagValue(args, '--before')

  if (!platform) {
    log.error('Error: --platform is required')
    printHelp()
    process.exit(1)
  }
  if (!IDENTIFIER_PATTERN.test(platform)) {
    log.error(`Error: invalid --platform value '${platform}' (allowed: ${IDENTIFIER_PATTERN})`)
    process.exit(1)
  }

  if (!typesArg) {
    log.error('Error: --types is required (comma-separated)')
    printHelp()
    process.exit(1)
  }
  const types = typesArg
    .split(',')
    .map((t) => t.trim())
    .filter(Boolean)
  if (types.length === 0) {
    log.error('Error: --types must contain at least one value')
    process.exit(1)
  }
  for (const t of types) {
    if (!IDENTIFIER_PATTERN.test(t)) {
      log.error(`Error: invalid type '${t}' (allowed: ${IDENTIFIER_PATTERN})`)
      process.exit(1)
    }
  }

  if (before && !DATE_PATTERN.test(before)) {
    log.error(`Error: --before must match YYYY-MM-DD, got '${before}'`)
    process.exit(1)
  }

  if (dryRun) {
    log.info(`\n${'='.repeat(80)}`)
    log.info('🧪 DRY RUN MODE - No data will be deleted')
    log.info(`${'='.repeat(80)}\n`)
  }

  try {
    await runCleanup({ platform, types, before }, dryRun, tbToken)
  } catch (error) {
    log.error(error, 'Failed to run cleanup script')
    log.error(`\n❌ Error: ${error.message}`)
    process.exit(1)
  }
}

main().catch((error) => {
  log.error('Unexpected error:', error)
  process.exit(1)
})
