/**
 * Activities Cleanup Script (by platform + type)
 *
 * Deletes activities from PostgreSQL (`activityRelations`) and Tinybird
 * (`activities` and `activityRelations` datasources) for a given platform and
 * one or more activity types, optionally bounded by a date cutoff.
 *
 * Before any deletion, the script prints the affected row counts from Tinybird
 * and prompts for confirmation (skip with --yes).
 *
 * NOTE: This script only purges the raw Tinybird datasources (`activities` and
 * `activityRelations`). Derived materialized views (activities_backup,
 * activities_deduplicated_ds, activityRelations_bucket_MV_ds_*, etc.) are NOT
 * affected because Tinybird/ClickHouse MV deletes do not cascade.
 *
 * Usage:
 *   pnpm run cleanup-activities-by-platform-and-type -- \
 *     --platform <platform> \
 *     --types <type1,type2,...> \
 *     [--before <YYYY-MM-DD>] \
 *     [--dry-run] \
 *     [--yes|-y] \
 *     [--tb-token <token>]
 *
 * Required:
 *   --platform   Platform name (e.g. 'gitlab', 'gerrit')
 *   --types      Comma-separated activity types (e.g. 'merge_request-closed')
 *
 * Optional:
 *   --before     Only delete rows with "updatedAt" < this date (YYYY-MM-DD)
 *   --dry-run    Report what would be deleted without deleting
 *   --yes / -y   Skip the interactive confirmation prompt
 *   --tb-token   Override CROWD_TINYBIRD_ACTIVITIES_TOKEN
 *
 * Environment Variables Required:
 *   CROWD_DB_WRITE_HOST, CROWD_DB_PORT, CROWD_DB_USERNAME, CROWD_DB_PASSWORD,
 *   CROWD_DB_DATABASE, CROWD_TINYBIRD_BASE_URL, CROWD_TINYBIRD_ACTIVITIES_TOKEN
 */
import * as fs from 'fs'
import * as path from 'path'
import * as readline from 'readline'

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

const POSTGRES_BATCH_SIZE = 10000
const TINYBIRD_JOB_POLL_INTERVAL_MS = 60_000
const TINYBIRD_JOB_TIMEOUT_MS = 6 * 60 * 60 * 1000 // 6h: bulk deletes can be slow

interface CleanupFilters {
  platform: string
  types: string[]
  before?: string
}

interface DeletionStatus {
  success: boolean
  jobId?: string
  error?: string
}

interface CleanupResult {
  status: 'success' | 'failure' | 'pending'
  startTime: string
  endTime?: string
  filters: CleanupFilters
  counts: {
    tinybirdActivities: number
    tinybirdActivityRelations: number
  }
  postgresDeleted: number
  postgresFailedBatches: number
  tinybirdJobIds: string[]
  deletions: {
    postgres: DeletionStatus
    tinybird: {
      activities: DeletionStatus
      activityRelations: DeletionStatus
    }
  }
}

// ---------------------------------------------------------------------------
// Filter clause builders
// ---------------------------------------------------------------------------

/** Tinybird/ClickHouse WHERE clause — unquoted identifiers, single-quoted strings. */
function buildTinybirdFilterClause(filters: CleanupFilters): string {
  const parts = [`platform = '${filters.platform}'`]
  const typesList = filters.types.map((t) => `'${t}'`).join(', ')
  parts.push(`type IN (${typesList})`)
  if (filters.before) {
    parts.push(`updatedAt < '${filters.before}'`)
  }
  return parts.join(' AND ')
}

/**
 * Postgres WHERE clause + pg-promise param map.
 * `"updatedAt"` is camelCase and must be double-quoted; `platform` and `type`
 * are lowercase and unquoted-safe.
 */
function buildPostgresFilter(filters: CleanupFilters): {
  where: string
  values: Record<string, unknown>
} {
  const conditions: string[] = [`platform = $(platform)`, `type IN ($(types:csv))`]
  const values: Record<string, unknown> = {
    platform: filters.platform,
    types: filters.types,
  }
  if (filters.before) {
    conditions.push(`"updatedAt" < $(beforeDate)`)
    values.beforeDate = filters.before
  }
  return { where: conditions.join(' AND '), values }
}

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------

async function initPostgresClient(): Promise<QueryExecutor> {
  log.info('Initializing Postgres connection...')
  const dbConnection = await getDbConnection(WRITE_DB_CONFIG())
  const queryExecutor = pgpQx(dbConnection)
  log.info('Postgres connection established')
  return queryExecutor
}

// ---------------------------------------------------------------------------
// Count helpers
// ---------------------------------------------------------------------------

async function countTinybirdRows(
  tinybird: TinybirdClient,
  datasource: string,
  filters: CleanupFilters,
): Promise<number> {
  const whereClause = buildTinybirdFilterClause(filters)
  const query = `SELECT count() AS c FROM ${datasource} WHERE ${whereClause} FORMAT JSON`
  const result = await tinybird.executeSql<{ data: Array<{ c: number }> }>(query)
  return result.data[0]?.c ?? 0
}

// ---------------------------------------------------------------------------
// Confirmation prompt
// ---------------------------------------------------------------------------

async function confirmOrAbort(message: string): Promise<void> {
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout })
  return new Promise((resolve, reject) => {
    rl.question(`${message}\nType "yes" to proceed: `, (answer) => {
      rl.close()
      if (answer.trim().toLowerCase() === 'yes') {
        resolve()
      } else {
        reject(new Error('Aborted by user'))
      }
    })
  })
}

// ---------------------------------------------------------------------------
// Postgres chunked delete
// ---------------------------------------------------------------------------

/**
 * Fetch matching IDs from `activityRelations` and delete by PK in batches.
 * PK deletes are cheap index lookups; the filter scan happens once per batch.
 * Returns { deleted, failedBatches }. Stops on the first failed batch so we
 * don't re-fetch the same undeleted rows forever.
 */
async function deletePostgresInChunks(
  postgres: QueryExecutor,
  filters: CleanupFilters,
  batchSize = POSTGRES_BATCH_SIZE,
): Promise<{ deleted: number; failedBatches: number }> {
  const { where, values } = buildPostgresFilter(filters)
  const fetchQuery = `SELECT "activityId" FROM "activityRelations" WHERE ${where} LIMIT ${batchSize}`

  let total = 0
  let batch = 0
  let failedBatches = 0
  let rows: Array<{ activityId: string }>

  do {
    rows = (await postgres.select(fetchQuery, values)) as Array<{ activityId: string }>
    if (rows.length === 0) break

    const ids = rows.map((r) => r.activityId)
    batch++

    try {
      await postgres.result(`DELETE FROM "activityRelations" WHERE "activityId" IN ($(ids:csv))`, {
        ids,
      })
      total += ids.length
    } catch (error) {
      log.error(
        `  Batch ${batch} delete failed (sample IDs: ${ids.slice(0, 3).join(', ')}): ${error.message}`,
      )
      failedBatches++
      break
    }

    if (batch % 10 === 0) {
      log.info(`  … deleted ${total.toLocaleString()} rows so far (batch ${batch})`)
    }
  } while (rows.length === batchSize)

  return { deleted: total, failedBatches }
}

// ---------------------------------------------------------------------------
// Tinybird delete jobs
// ---------------------------------------------------------------------------

async function deleteActivitiesFromTinybird(
  tinybird: TinybirdClient,
  filters: CleanupFilters,
): Promise<{
  activities: DeletionStatus
  activityRelations: DeletionStatus
  jobIds: string[]
}> {
  const deleteCondition = buildTinybirdFilterClause(filters)
  const results = {
    activities: { success: false } as DeletionStatus,
    activityRelations: { success: false } as DeletionStatus,
  }
  const triggeredJobIds: string[] = []

  log.info('Triggering deletion job for Tinybird activities datasource...')
  try {
    const resp = await tinybird.deleteDatasource('activities', deleteCondition, true, false)
    log.info(`✓ Triggered activities deletion job (job_id: ${resp.job_id})`)
    triggeredJobIds.push(resp.job_id)
    results.activities = { success: true, jobId: resp.job_id }
  } catch (error) {
    log.error(`Failed to trigger deletion job for activities: ${error.message}`)
    results.activities = { success: false, error: error.message }
  }

  log.info('Triggering deletion job for Tinybird activityRelations datasource...')
  try {
    const resp = await tinybird.deleteDatasource('activityRelations', deleteCondition, true, false)
    log.info(`✓ Triggered activityRelations deletion job (job_id: ${resp.job_id})`)
    triggeredJobIds.push(resp.job_id)
    results.activityRelations = { success: true, jobId: resp.job_id }
  } catch (error) {
    log.error(`Failed to trigger deletion job for activityRelations: ${error.message}`)
    results.activityRelations = { success: false, error: error.message }
  }

  return { ...results, jobIds: triggeredJobIds }
}

// ---------------------------------------------------------------------------
// Result JSON persistence
// ---------------------------------------------------------------------------

function resultJsonPath(filters: CleanupFilters, startTime: string): string {
  const safeSuffix = `${filters.platform}_${filters.types.join('-')}`.replace(
    /[^A-Za-z0-9_-]/g,
    '_',
  )
  return path.join(
    '/tmp',
    `cleanup_activities_${safeSuffix}_${startTime.replace(/[:.]/g, '-')}.json`,
  )
}

function writeResult(filePath: string, result: CleanupResult): void {
  try {
    fs.writeFileSync(filePath, JSON.stringify(result, null, 2), 'utf-8')
    log.info(`✓ Cleanup results saved to: ${filePath}`)
  } catch (error) {
    log.error(`Failed to write cleanup results to ${filePath}: ${error.message}`)
  }
}

// ---------------------------------------------------------------------------
// Main orchestration
// ---------------------------------------------------------------------------

async function runCleanup(
  filters: CleanupFilters,
  dryRun: boolean,
  skipConfirm: boolean,
  tbToken?: string,
): Promise<void> {
  const startTime = new Date().toISOString()

  log.info(`\n${'='.repeat(80)}`)
  log.info(`${dryRun ? '[DRY RUN MODE] ' : ''}Activities Cleanup`)
  log.info(`Filters: ${buildTinybirdFilterClause(filters)}`)
  log.info(`${'='.repeat(80)}`)

  const postgres = await initPostgresClient()
  const tinybird = new TinybirdClient(tbToken)

  log.info('Counting affected rows in Tinybird...')
  const [tbActivitiesCount, tbRelationsCount] = await Promise.all([
    countTinybirdRows(tinybird, 'activities', filters),
    countTinybirdRows(tinybird, 'activityRelations', filters),
  ])

  log.info(`  Tinybird   activities        : ${tbActivitiesCount.toLocaleString()} rows`)
  log.info(`  Tinybird   activityRelations : ${tbRelationsCount.toLocaleString()} rows`)
  log.info(`  PostgreSQL activityRelations : will be deleted by streaming batches (no pre-count)`)

  if (dryRun) {
    log.info(`\n[DRY RUN] Would delete:`)
    log.info(
      `  PostgreSQL activityRelations matching filter (actual count reported during real execution)`,
    )
    log.info(`  ${tbActivitiesCount.toLocaleString()} rows from Tinybird activities`)
    log.info(`  ${tbRelationsCount.toLocaleString()} rows from Tinybird activityRelations`)
    log.info('[DRY RUN] No data was deleted.')
    return
  }

  if (tbActivitiesCount === 0 && tbRelationsCount === 0) {
    log.info('No matching rows found in Tinybird. Nothing to delete.')
    return
  }

  if (!skipConfirm) {
    await confirmOrAbort(
      `\nAbout to permanently delete PG rows matching filter, ${tbActivitiesCount.toLocaleString()} TB activities, ${tbRelationsCount.toLocaleString()} TB activityRelations.`,
    )
  }

  const result: CleanupResult = {
    status: 'pending',
    startTime,
    filters,
    counts: {
      tinybirdActivities: tbActivitiesCount,
      tinybirdActivityRelations: tbRelationsCount,
    },
    postgresDeleted: 0,
    postgresFailedBatches: 0,
    tinybirdJobIds: [],
    deletions: {
      postgres: { success: true },
      tinybird: {
        activities: { success: true },
        activityRelations: { success: true },
      },
    },
  }
  const jsonFilePath = resultJsonPath(filters, startTime)

  log.info(`\nStep 1: Deleting matching rows from PostgreSQL in ${POSTGRES_BATCH_SIZE} batches...`)
  try {
    const pgResult = await deletePostgresInChunks(postgres, filters)
    result.postgresDeleted = pgResult.deleted
    result.postgresFailedBatches = pgResult.failedBatches
    if (pgResult.failedBatches > 0) {
      log.warn(
        `  ${pgResult.failedBatches} batch(es) failed — ${pgResult.deleted.toLocaleString()} rows deleted successfully`,
      )
      result.deletions.postgres = {
        success: false,
        error: `${pgResult.failedBatches} batch(es) failed`,
      }
    } else {
      log.info(`✓ Deleted ${pgResult.deleted.toLocaleString()} row(s) from PostgreSQL`)
    }
  } catch (error) {
    log.error(`Postgres deletion failed: ${error.message}`)
    result.deletions.postgres = { success: false, error: error.message }
  }

  log.info('\nStep 2: Triggering Tinybird deletions...')
  const tinybirdStatuses = await deleteActivitiesFromTinybird(tinybird, filters)
  result.deletions.tinybird.activities = tinybirdStatuses.activities
  result.deletions.tinybird.activityRelations = tinybirdStatuses.activityRelations
  result.tinybirdJobIds = tinybirdStatuses.jobIds

  // Persist result BEFORE waiting on TB jobs so the job IDs survive a timeout.
  writeResult(jsonFilePath, result)

  if (tinybirdStatuses.jobIds.length > 0) {
    log.info(
      `Waiting for ${tinybirdStatuses.jobIds.length} Tinybird deletion job(s) to complete (timeout: ${TINYBIRD_JOB_TIMEOUT_MS / 1000 / 60} min)...`,
    )
    try {
      await tinybird.waitForJobs(
        tinybirdStatuses.jobIds,
        TINYBIRD_JOB_POLL_INTERVAL_MS,
        TINYBIRD_JOB_TIMEOUT_MS,
      )
      log.info(`✓ All Tinybird deletion jobs completed`)
    } catch (error) {
      log.error(
        `Failed to wait for Tinybird deletion jobs: ${error.message} (jobs are still running in background; IDs persisted to ${jsonFilePath})`,
      )
    }
  }

  result.endTime = new Date().toISOString()
  const anyFailure =
    !result.deletions.postgres.success ||
    !result.deletions.tinybird.activities.success ||
    !result.deletions.tinybird.activityRelations.success
  result.status = anyFailure ? 'failure' : 'success'
  writeResult(jsonFilePath, result)

  log.info(`\n${'='.repeat(80)}`)
  log.info('Cleanup Summary')
  log.info(`${'='.repeat(80)}`)
  log.info(`✓ PostgreSQL rows deleted: ${result.postgresDeleted.toLocaleString()}`)
  if (result.postgresFailedBatches > 0) {
    log.warn(`✗ PostgreSQL failed batches: ${result.postgresFailedBatches}`)
  }
  if (tinybirdStatuses.activities.success) {
    log.info(`✓ Tinybird activities deletion job: ${tinybirdStatuses.activities.jobId || 'N/A'}`)
  } else {
    log.error(`✗ Tinybird activities deletion failed: ${tinybirdStatuses.activities.error}`)
  }
  if (tinybirdStatuses.activityRelations.success) {
    log.info(
      `✓ Tinybird activityRelations deletion job: ${tinybirdStatuses.activityRelations.jobId || 'N/A'}`,
    )
  } else {
    log.error(
      `✗ Tinybird activityRelations deletion failed: ${tinybirdStatuses.activityRelations.error}`,
    )
  }

  if (result.status === 'failure') {
    process.exit(1)
  }
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

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
        [--yes|-y] \\
        [--tb-token <token>]

    Required:
      --platform   Platform name (e.g. 'gitlab', 'gerrit')
      --types      Comma-separated activity types (e.g. 'merge_request-closed')

    Optional:
      --before     Only delete rows with "updatedAt" < this date (YYYY-MM-DD)
      --dry-run    Report what would be deleted without deleting
      --yes / -y   Skip the interactive confirmation prompt
      --tb-token   Override CROWD_TINYBIRD_ACTIVITIES_TOKEN

    Examples:
      # Dry run: see how many gitlab merge_request-closed rows would be deleted
      pnpm run cleanup-activities-by-platform-and-type -- \\
        --platform gitlab --types merge_request-closed --dry-run

      # Actual cleanup (will prompt for confirmation)
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
  const skipConfirm = args.includes('--yes') || args.includes('-y')
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

  if (before) {
    if (!DATE_PATTERN.test(before) || Number.isNaN(Date.parse(before))) {
      log.error(`Error: --before must be a valid YYYY-MM-DD date, got '${before}'`)
      process.exit(1)
    }
  }

  if (dryRun) {
    log.info(`\n${'='.repeat(80)}`)
    log.info('🧪 DRY RUN MODE - No data will be deleted')
    log.info(`${'='.repeat(80)}\n`)
  }

  try {
    await runCleanup({ platform, types, before }, dryRun, skipConfirm, tbToken)
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
