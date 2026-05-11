/**
 * Gerrit Activities Cleanup Script
 *
 * PROBLEM:
 * Gerrit activities need to be cleaned up from both PostgreSQL and Tinybird.
 *
 * SOLUTION:
 * This script deletes activities from the Gerrit platform across:
 * - PostgreSQL (activityRelations table only, chunked in 10k batches)
 * - Tinybird (activities and activityRelations datasources, one delete job each)
 *
 * Before any deletion, the script prints the affected row counts from all three
 * stores and prompts for confirmation (skip with --yes).
 *
 * Filters (all optional):
 * - platform = 'gerrit'  (always applied)
 * - --type <types>       (optional) restrict to specific activity type(s)
 * - --before-date <date> (optional) restrict to records with updatedAt < date
 *
 * WARNING: Running with no --type and no --before-date will delete ALL gerrit
 * activities regardless of type or age.
 *
 * NOTE: This script only purges the raw Tinybird datasources (`activities` and
 * `activityRelations`). Derived materialized views (activities_backup,
 * activities_deduplicated_ds, activityRelations_bucket_MV_ds_*, etc.) are NOT
 * affected because Tinybird/ClickHouse MV deletes do not cascade.
 *
 * Usage:
 *   # Via package.json script (recommended):
 *   pnpm run cleanup-gerrit-activities -- [options]
 *
 *   # Or directly with tsx:
 *   npx tsx src/bin/cleanup-gerrit-activities.ts [options]
 *
 * Options:
 *   --dry-run                  Display row counts and what would be deleted, without deleting anything
 *   --yes / -y                 Skip confirmation prompt (for non-interactive use)
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
import * as readline from 'readline'

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

interface DeletionStatus {
  success: boolean
  jobId?: string
  error?: string
}

interface CleanupResult {
  status: 'success' | 'failure'
  startTime: string
  endTime: string
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

/** Tinybird/ClickHouse WHERE clause (unquoted identifiers, single-quoted strings) */
function buildTinybirdFilterClause(filters: Filters): string {
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

/**
 * Postgres WHERE clause + pg-promise param map.
 * updatedAt is camelCase and must be double-quoted.
 */
function buildPostgresFilter(filters: Filters): { where: string; values: Record<string, unknown> } {
  const conditions: string[] = [`platform = 'gerrit'`]
  const values: Record<string, unknown> = {}

  if (filters.types?.length) {
    conditions.push(`type IN ($(types:csv))`)
    values.types = filters.types
  }
  if (filters.beforeDate) {
    conditions.push(`"updatedAt" < $(beforeDate)`)
    values.beforeDate = filters.beforeDate
  }

  return { where: conditions.join(' AND '), values }
}

// ---------------------------------------------------------------------------
// Count helpers
// ---------------------------------------------------------------------------

async function countTinybirdRows(
  tinybird: TinybirdClient,
  datasource: string,
  filters: Filters,
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
 * Delete matching rows from activityRelations in batches by fetching IDs first.
 * Each iteration: fetch up to batchSize IDs matching the filter, then delete by PK.
 * PK deletes are cheap index lookups; the filter scan happens once per batch (not twice).
 * Returns { deleted, failedBatches }.
 */
async function deletePostgresInChunks(
  postgres: QueryExecutor,
  filters: Filters,
  batchSize = 10000,
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
      // Stop to prevent re-fetching the same undeleted rows on the next iteration
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
  filters: Filters,
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
// Main cleanup orchestration
// ---------------------------------------------------------------------------

async function runCleanup(
  dryRun: boolean,
  skipConfirm: boolean,
  tbToken: string | undefined,
  filters: Filters,
): Promise<void> {
  const startTime = new Date().toISOString()

  log.info(`\n${'='.repeat(80)}`)
  log.info(dryRun ? '[DRY RUN MODE] Gerrit Activities Cleanup' : 'Gerrit Activities Cleanup')
  log.info(`${'='.repeat(80)}`)
  log.info(`Active filters: ${buildTinybirdFilterClause(filters)}`)

  const postgres = await initPostgresClient()
  const tinybird = new TinybirdClient(tbToken)

  // Pre-flight counts — Tinybird only (ClickHouse COUNT is cheap; PG COUNT over 1M+ rows is not)
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
      `  PostgreSQL activityRelations matching filter: ${buildTinybirdFilterClause(filters).replace(/'/g, '"')} — actual count reported during real execution`,
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

  // Step 1: Delete from Postgres in chunks (fetch IDs then delete by PK, 10k at a time)
  log.info(`\nStep 1: Deleting matching rows from PostgreSQL in 10k batches...`)
  const postgresStatus: DeletionStatus = { success: true }
  let postgresDeleted = 0
  let postgresFailedBatches = 0
  try {
    const pgResult = await deletePostgresInChunks(postgres, filters)
    postgresDeleted = pgResult.deleted
    postgresFailedBatches = pgResult.failedBatches
    if (postgresFailedBatches > 0) {
      log.warn(
        `  ${postgresFailedBatches} batch(es) failed — ${postgresDeleted.toLocaleString()} rows deleted successfully`,
      )
      postgresStatus.success = false
      postgresStatus.error = `${postgresFailedBatches} batch(es) failed`
    } else {
      log.info(`✓ Deleted ${postgresDeleted.toLocaleString()} row(s) from PostgreSQL`)
    }
  } catch (error) {
    log.error(`Failed to delete from PostgreSQL: ${error.message}`)
    postgresStatus.success = false
    postgresStatus.error = error.message
  }

  // Step 2: Trigger Tinybird delete jobs
  log.info('\nStep 2: Triggering Tinybird deletion jobs...')
  const tinybirdStatuses = await deleteActivitiesFromTinybird(tinybird, filters)

  // Persist result JSON immediately (before waiting) so job IDs are recoverable on timeout
  const endTime = new Date().toISOString()
  const result: CleanupResult = {
    status:
      postgresStatus.success &&
      tinybirdStatuses.activities.success &&
      tinybirdStatuses.activityRelations.success
        ? 'success'
        : 'failure',
    startTime,
    endTime,
    postgresDeleted,
    postgresFailedBatches,
    tinybirdJobIds: tinybirdStatuses.jobIds,
    deletions: {
      postgres: postgresStatus,
      tinybird: {
        activities: tinybirdStatuses.activities,
        activityRelations: tinybirdStatuses.activityRelations,
      },
    },
  }

  const jsonFilePath = path.join(
    '/tmp',
    `cleanup_gerrit_activities_${new Date().toISOString().replace(/[:.]/g, '-')}.json`,
  )
  try {
    fs.writeFileSync(jsonFilePath, JSON.stringify(result, null, 2), 'utf-8')
    log.info(`✓ Job IDs and status saved to: ${jsonFilePath}`)
  } catch (error) {
    log.error(`Failed to write result file ${jsonFilePath}: ${error.message}`)
  }

  // Step 3: Wait for Tinybird jobs (up to 6 hours)
  if (tinybirdStatuses.jobIds.length > 0) {
    log.info(
      `\nStep 3: Waiting for ${tinybirdStatuses.jobIds.length} Tinybird job(s) to complete (up to 6h)...`,
    )
    try {
      await tinybird.waitForJobs(tinybirdStatuses.jobIds, 60_000, 6 * 60 * 60 * 1000)
      log.info('✓ All Tinybird deletion jobs completed')
    } catch (error) {
      log.error(`Tinybird wait failed: ${error.message}`)
      log.error(
        `Tinybird jobs may still be running. Job IDs: ${tinybirdStatuses.jobIds.join(', ')}`,
      )
      log.error(`Check the result file for details: ${jsonFilePath}`)
      process.exit(1)
    }
  }

  // Summary
  log.info(`\n${'='.repeat(80)}`)
  log.info('Cleanup Summary')
  log.info(`${'='.repeat(80)}`)
  if (postgresStatus.success) {
    log.info(`✓ PostgreSQL rows deleted : ${postgresDeleted.toLocaleString()}`)
  } else {
    log.warn(
      `⚠ PostgreSQL rows deleted : ${postgresDeleted.toLocaleString()} (${postgresFailedBatches} batch(es) failed)`,
    )
  }
  if (tinybirdStatuses.activities.success) {
    log.info(`✓ Tinybird activities job  : ${tinybirdStatuses.activities.jobId}`)
  } else {
    log.error(`✗ Tinybird activities failed: ${tinybirdStatuses.activities.error}`)
  }
  if (tinybirdStatuses.activityRelations.success) {
    log.info(`✓ Tinybird activityRelations job: ${tinybirdStatuses.activityRelations.jobId}`)
  } else {
    log.error(`✗ Tinybird activityRelations failed: ${tinybirdStatuses.activityRelations.error}`)
  }
  log.info(`Result file: ${jsonFilePath}`)

  if (result.status === 'failure') {
    process.exit(1)
  }
}

async function initPostgresClient(): Promise<QueryExecutor> {
  log.info('Initializing Postgres connection...')
  const dbConnection = await getDbConnection(WRITE_DB_CONFIG())
  const queryExecutor = pgpQx(dbConnection)
  log.info('Postgres connection established')
  return queryExecutor
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

async function main() {
  const args = process.argv.slice(2)

  if (args.includes('--help') || args.includes('-h')) {
    log.info(`
      Usage:
        pnpm run cleanup-gerrit-activities -- [options]

      Options:
        --dry-run                  Display row counts without deleting anything
        --yes / -y                 Skip confirmation prompt (non-interactive)
        --tb-token <token>         Tinybird API token (overrides env var)
        --before-date <YYYY-MM-DD> Only delete records with updatedAt before this date
        --type <types>             Comma-separated activity types to delete.
                                   Valid: ${VALID_GERRIT_TYPES.join(', ')}

      WARNING: Running with no --type and no --before-date deletes ALL gerrit activities.

      Examples:
        pnpm run cleanup-gerrit-activities -- --dry-run --type patchset_approval-created
        pnpm run cleanup-gerrit-activities -- --type patchset_approval-created --before-date 2025-12-15
        pnpm run cleanup-gerrit-activities -- --type changeset-merged --yes
    `)
    process.exit(0)
  }

  const dryRun = args.includes('--dry-run')
  const skipConfirm = args.includes('--yes') || args.includes('-y')

  let tbToken: string | undefined
  const tbTokenIndex = args.indexOf('--tb-token')
  if (tbTokenIndex !== -1) {
    if (tbTokenIndex + 1 >= args.length) {
      log.error('Error: --tb-token requires a value')
      process.exit(1)
    }
    tbToken = args[tbTokenIndex + 1]
  }

  let beforeDate: string | undefined
  const beforeDateIndex = args.indexOf('--before-date')
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

  let types: string[] | undefined
  const typeIndex = args.indexOf('--type')
  if (typeIndex !== -1) {
    if (typeIndex + 1 >= args.length) {
      log.error('Error: --type requires a value (comma-separated list of activity types)')
      process.exit(1)
    }
    const raw = args[typeIndex + 1]
    const parsed = raw
      .split(',')
      .map((t) => t.trim())
      .filter(Boolean)
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

  if (!types && !beforeDate) {
    log.warn(
      'WARNING: No --type or --before-date provided — this will target ALL gerrit activities.',
    )
  }

  const filters: Filters = { beforeDate, types }

  try {
    await runCleanup(dryRun, skipConfirm, tbToken, filters)
  } catch (error) {
    if (error.message === 'Aborted by user') {
      log.info('Cleanup aborted.')
      process.exit(0)
    }
    log.error(error, 'Failed to run Gerrit cleanup script')
    log.error(`\nError: ${error.message}`)
    process.exit(1)
  }
}

main().catch((error) => {
  log.error('Unexpected error:', error)
  process.exit(1)
})
