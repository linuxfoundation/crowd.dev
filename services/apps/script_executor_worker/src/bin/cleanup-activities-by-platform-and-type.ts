/**
 * Activities Cleanup Script (by platform + type)
 *
 * Sequin sends each deleted row on to Tinybird, so run it without --tinybird-only first, then
 * with it once Kafka Connect has caught up.
 *
 * Usage:
 *   pnpm run cleanup-activities-by-platform-and-type -- \
 *     --platform <platform> \
 *     --types <type1,type2,...> \
 *     [--segment-id <uuid>] \
 *     [--before <YYYY-MM-DD>] \
 *     [--tinybird-only] \
 *     [--dry-run] \
 *     [--yes|-y] \
 *     [--tb-token <token>]
 *
 * Required:
 *   --platform   Platform name (e.g. 'gitlab', 'gerrit')
 *   --types      Comma-separated activity types (e.g. 'merge_request-closed')
 *
 * Optional:
 *   --segment-id Restrict deletion to a single segment (UUID)
 *   --before     Only delete rows with "updatedAt" < this date (YYYY-MM-DD)
 *   --tinybird-only  Second run: delete from Tinybird only
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
const UUID_PATTERN = /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/

const POSTGRES_BATCH_SIZE = 10000
const TINYBIRD_JOB_POLL_INTERVAL_MS = 60_000
const TINYBIRD_JOB_TIMEOUT_MS = 6 * 60 * 60 * 1000 // 6h: bulk deletes can be slow

interface CleanupFilters {
  platform: string
  types: string[]
  segmentId?: string
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
    tinybirdActivities: number | null
    tinybirdActivityRelations: number | null
  }
  postgresDeleted: number
  postgresFailedBatches: number
  deletions: {
    postgres: DeletionStatus
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
  if (filters.segmentId) {
    parts.push(`segmentId = '${filters.segmentId}'`)
  }
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
  if (filters.segmentId) {
    conditions.push(`"segmentId" = $(segmentId)`)
    values.segmentId = filters.segmentId
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
  return Number(result.data[0]?.c ?? 0)
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

async function postgresHasMatchingRows(
  postgres: QueryExecutor,
  filters: CleanupFilters,
): Promise<boolean> {
  const { where, values } = buildPostgresFilter(filters)
  const rows = await postgres.select(
    `SELECT 1 FROM "activityRelations" WHERE ${where} LIMIT 1`,
    values,
  )
  return rows.length > 0
}

// ---------------------------------------------------------------------------
// Tinybird delete jobs
// ---------------------------------------------------------------------------

const TINYBIRD_RAW_SOURCES = ['activities', 'activityRelations']

interface SkippedDatasource {
  name: string
  reason: string
}

function isSharedFromAnotherWorkspace(datasource: string): boolean {
  return datasource.includes('.')
}

function isRelationCopy(columns: Set<string>): boolean {
  return columns.has('activityId')
}

function isActivityCopy(columns: Set<string>): boolean {
  return columns.has('id') && columns.has('sourceId') && columns.has('body')
}

// copies are filled from these: raw sources, then MVs, then copies, and copies of copies last
function deletionRank(datasource: string): number {
  if (TINYBIRD_RAW_SOURCES.includes(datasource)) return 0
  if (datasource.includes('_MV_')) return 1
  if (datasource.includes('_by_member')) return 3
  return 2
}

// Deletes on a raw datasource don't reach its MVs and copies, so every copy is deleted from:
// https://www.tinybird.co/docs/classic/get-data-in/data-operations/replace-and-delete-data#delete-data-selectively
async function planTinybirdDatasources(
  tinybird: TinybirdClient,
  filters: CleanupFilters,
): Promise<{ datasources: string[]; skipped: SkippedDatasource[] }> {
  const filterColumns = ['platform', 'type']
  if (filters.segmentId) filterColumns.push('segmentId')
  if (filters.before) filterColumns.push('updatedAt')

  const listed = await tinybird.listDatasources()
  const datasources: string[] = []
  const skipped: SkippedDatasource[] = []

  for (const { name, columns } of listed) {
    if (isSharedFromAnotherWorkspace(name)) continue

    const columnNames = new Set(columns.map((c) => c.name))
    const relationCopy = isRelationCopy(columnNames)
    if (!relationCopy && !isActivityCopy(columnNames)) continue

    const missing = filterColumns.filter((column) => !columnNames.has(column))
    if (missing.length > 0) {
      skipped.push({ name, reason: `no ${missing.join(', ')} column to filter by` })
    } else if (filters.before && !relationCopy) {
      skipped.push({ name, reason: "its updatedAt is the activity's, not the relation's" })
    } else {
      datasources.push(name)
    }
  }

  const unplannedSources = TINYBIRD_RAW_SOURCES.filter(
    (source) => !datasources.includes(source) && !skipped.some(({ name }) => name === source),
  )
  if (unplannedSources.length > 0) {
    throw new Error(
      `Tinybird listed no usable columns for ${unplannedSources.join(', ')}: ` +
        'the token must read every datasource',
    )
  }

  datasources.sort((a, b) => deletionRank(a) - deletionRank(b) || a.localeCompare(b))
  return { datasources, skipped }
}

function formatCount(count: number | null | undefined): string {
  return count === null || count === undefined ? 'unknown' : count.toLocaleString()
}

// large datasources can hit the Query API time limit
async function countTinybirdRowsOrNull(
  tinybird: TinybirdClient,
  datasource: string,
  filters: CleanupFilters,
): Promise<number | null> {
  try {
    return await countTinybirdRows(tinybird, datasource, filters)
  } catch (error) {
    log.warn(`  Counting ${datasource} failed: ${error.message}`)
    return null
  }
}

async function countAllTinybird(
  tinybird: TinybirdClient,
  datasources: string[],
  filters: CleanupFilters,
): Promise<Map<string, number | null>> {
  const counts = new Map<string, number | null>()
  for (const datasource of datasources) {
    counts.set(datasource, await countTinybirdRowsOrNull(tinybird, datasource, filters))
  }
  return counts
}

async function deleteFromTinybird(
  tinybird: TinybirdClient,
  datasource: string,
  filters: CleanupFilters,
): Promise<void> {
  log.info(`  Deleting from ${datasource}...`)
  const resp = await tinybird.deleteDatasource(
    datasource,
    buildTinybirdFilterClause(filters),
    true,
    false,
  )
  if (!resp.job_id) {
    throw new Error(`Delete on ${datasource} returned no job`)
  }
  await tinybird.waitForJobs([resp.job_id], TINYBIRD_JOB_POLL_INTERVAL_MS, TINYBIRD_JOB_TIMEOUT_MS)
}

function tinybirdOnlyCommand(filters: CleanupFilters): string {
  const parts = [
    'pnpm run cleanup-activities-by-platform-and-type --',
    '--tinybird-only',
    `--platform ${filters.platform}`,
    `--types ${filters.types.join(',')}`,
  ]
  if (filters.segmentId) parts.push(`--segment-id ${filters.segmentId}`)
  if (filters.before) parts.push(`--before ${filters.before}`)
  return parts.join(' ')
}

async function runTinybirdCleanup(
  filters: CleanupFilters,
  dryRun: boolean,
  skipConfirm: boolean,
  tbToken?: string,
): Promise<void> {
  const postgres = await initPostgresClient()
  if (await postgresHasMatchingRows(postgres, filters)) {
    const message =
      'PostgreSQL still has matching rows: run the cleanup without --tinybird-only first, ' +
      'with --before if new matching activities keep arriving'
    if (!dryRun) throw new Error(message)
    log.warn(message)
  }

  const tinybird = new TinybirdClient(tbToken)
  const { datasources, skipped } = await planTinybirdDatasources(tinybird, filters)

  for (const { name, reason } of skipped) {
    log.warn(`  Skipping ${name}: ${reason}, its matching rows stay`)
  }

  log.info('Counting affected rows in Tinybird...')
  const counts = await countAllTinybird(tinybird, datasources, filters)
  for (const datasource of datasources) {
    const count = counts.get(datasource)
    log.info(`  ${datasource}: ${formatCount(count)} rows`)
  }

  const toDelete = datasources.filter((datasource) => counts.get(datasource) !== 0)
  if (dryRun) {
    log.info(`[DRY RUN] Would delete from ${toDelete.length} datasources. No data was deleted.`)
    return
  }
  if (toDelete.length === 0) {
    log.info('No matching rows found in Tinybird. Nothing to delete.')
    if (skipped.length > 0) process.exit(2)
    return
  }

  if (!skipConfirm) {
    await confirmOrAbort(
      `\nAbout to permanently delete the matching rows from ${toDelete.length} Tinybird datasources.`,
    )
  }

  for (const datasource of toDelete) {
    await deleteFromTinybird(tinybird, datasource, filters)
  }

  const afterFirstPass = await countAllTinybird(tinybird, datasources, filters)
  for (const datasource of datasources.filter((ds) => afterFirstPass.get(ds) !== 0)) {
    log.info(
      `  ${datasource}: deleting once more (count ${afterFirstPass.get(datasource) ?? 'unknown'})`,
    )
    await deleteFromTinybird(tinybird, datasource, filters)
  }

  const final = await countAllTinybird(tinybird, datasources, filters)
  const left = datasources.filter((ds) => (final.get(ds) ?? 0) > 0)
  const unverified = datasources.filter((ds) => final.get(ds) === null)

  for (const datasource of left) {
    log.error(`✗ ${datasource} still holds ${final.get(datasource)} matching rows`)
  }
  if (left.length > 0) {
    process.exit(1)
  }
  if (unverified.length > 0) {
    log.warn(`Deleted twice but could not count afterwards: ${unverified.join(', ')}`)
  }
  if (skipped.length > 0) {
    log.warn(`Skipped, matching rows may remain: ${skipped.map(({ name }) => name).join(', ')}`)
  }
  log.info(
    'Not adjusted: segment aggregates, and pull_requests_analyzed until ' +
      'pull_request_analysis_initial_snapshot runs again',
  )
  if (unverified.length > 0 || skipped.length > 0) {
    process.exit(2)
  }

  log.info(`✓ No matching rows left in ${datasources.length} Tinybird datasources`)
  log.info(
    'A copy job that was already running can still write rows back: run it again after the next hourly copies to confirm',
  )
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

async function runPostgresCleanup(
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
    countTinybirdRowsOrNull(tinybird, 'activities', filters),
    countTinybirdRowsOrNull(tinybird, 'activityRelations', filters),
  ])

  log.info(`  Tinybird   activities        : ${formatCount(tbActivitiesCount)} rows`)
  log.info(`  Tinybird   activityRelations : ${formatCount(tbRelationsCount)} rows`)
  log.info(`  PostgreSQL activityRelations : will be deleted by streaming batches (no pre-count)`)

  if (dryRun) {
    log.info(`\n[DRY RUN] Would delete:`)
    log.info(
      `  PostgreSQL activityRelations matching filter (actual count reported during real execution)`,
    )
    log.info(`  Tinybird: nothing in this run, see --tinybird-only`)
    log.info('[DRY RUN] No data was deleted.')
    return
  }

  if (!skipConfirm) {
    await confirmOrAbort(
      `\nAbout to permanently delete the matching PostgreSQL rows. Tinybird (${formatCount(tbActivitiesCount)} activities, ${formatCount(tbRelationsCount)} activityRelations) is cleaned by the --tinybird-only run afterwards.`,
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
    deletions: {
      postgres: { success: true },
    },
  }
  const jsonFilePath = resultJsonPath(filters, startTime)

  log.info(`\nDeleting matching rows from PostgreSQL in ${POSTGRES_BATCH_SIZE} batches...`)
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

  result.endTime = new Date().toISOString()
  result.status = result.deletions.postgres.success ? 'success' : 'failure'
  writeResult(jsonFilePath, result)

  log.info(`\n${'='.repeat(80)}`)
  log.info('Cleanup Summary')
  log.info(`${'='.repeat(80)}`)
  log.info(`✓ PostgreSQL rows deleted: ${result.postgresDeleted.toLocaleString()}`)
  if (result.postgresFailedBatches > 0) {
    log.warn(`✗ PostgreSQL failed batches: ${result.postgresFailedBatches}`)
  }

  if (result.status === 'failure') {
    process.exit(1)
  }

  log.info('\nTinybird is not cleaned yet. Once Kafka Connect has caught up, run:')
  log.info(`  ${tinybirdOnlyCommand(filters)}`)
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
        [--segment-id <uuid>] \\
        [--before <YYYY-MM-DD>] \\
        [--tinybird-only] \\
        [--dry-run] \\
        [--yes|-y] \\
        [--tb-token <token>]

    Required:
      --platform   Platform name (e.g. 'gitlab', 'gerrit')
      --types      Comma-separated activity types (e.g. 'merge_request-closed')

    Optional:
      --segment-id Restrict deletion to a single segment (UUID)
      --before     Only delete rows with "updatedAt" < this date (YYYY-MM-DD)
      --tinybird-only  Second run, once Kafka Connect has caught up: delete
                   from every Tinybird copy of activities and relations
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
  const tinybirdOnly = args.includes('--tinybird-only')
  const skipConfirm = args.includes('--yes') || args.includes('-y')
  const tbToken = getFlagValue(args, '--tb-token')
  const platform = getFlagValue(args, '--platform')
  const typesArg = getFlagValue(args, '--types')
  const segmentId = getFlagValue(args, '--segment-id')
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

  if (segmentId && !UUID_PATTERN.test(segmentId)) {
    log.error(`Error: --segment-id must be a UUID, got '${segmentId}'`)
    process.exit(1)
  }

  if (dryRun) {
    log.info(`\n${'='.repeat(80)}`)
    log.info('🧪 DRY RUN MODE - No data will be deleted')
    log.info(`${'='.repeat(80)}\n`)
  }

  try {
    const filters = { platform, types, segmentId, before }
    if (tinybirdOnly) {
      await runTinybirdCleanup(filters, dryRun, skipConfirm, tbToken)
    } else {
      await runPostgresCleanup(filters, dryRun, skipConfirm, tbToken)
    }
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
