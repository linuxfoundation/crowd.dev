/**
 * NULL-channel duplicate cleanup for activityRelations (linuxfoundation/lfx-cdp-insights-ops#852)
 *
 * Before #850, activities without a channel got a new activityRelations row on every re-send,
 * so one dedup key ("timestamp", platform, type, "sourceId", "segmentId" with a NULL channel)
 * can have many copies. This script keeps the newest copy of each key and moves the others to
 * a dated backup table, in short batches that can be stopped and resumed.
 *
 * Commands:
 *   collect  Walk the dedup index in time windows and record every key with more than one
 *            NULL-channel copy in the work table. Resumes from the last finished window.
 *   status   Print the work table's progress per platform.
 *
 * Usage:
 *   pnpm run cleanup-null-channel-duplicates -- <command> --tag <DD_MM_YYYY> [options]
 *
 * Common options:
 *   --tag         Date tag shared by every run of one cleanup (DD_MM_YYYY). Names the tables:
 *                 "activityRelations_<tag>" (backup), "activityRelationsNullChannelKeys_<tag>"
 *                 (work) and "activityRelationsNullChannelProgress_<tag>" (progress).
 *   --platforms   Comma-separated platforms (default: meetings,linkedin,tnc,cvent,slack,stackoverflow)
 *   --dry-run     Report what would happen without writing
 *
 * collect options:
 *   --from        First timestamp to scan (YYYY-MM-DD, default: oldest activity or the last
 *                 finished window)
 *   --to          Scan up to this timestamp, exclusive (YYYY-MM-DD, default: now)
 *   --window-days Days per scan window (default 7)
 *
 * Environment Variables Required:
 *   CROWD_DB_WRITE_HOST, CROWD_DB_PORT, CROWD_DB_USERNAME, CROWD_DB_PASSWORD, CROWD_DB_DATABASE
 */
import {
  DbConnection,
  WRITE_DB_CONFIG,
  getDbConnection,
} from '@crowd/data-access-layer/src/database'
import { getServiceChildLogger } from '@crowd/logging'

const log = getServiceChildLogger('cleanup-null-channel-duplicates')

const DEFAULT_PLATFORMS = ['meetings', 'linkedin', 'tnc', 'cvent', 'slack', 'stackoverflow']
const TAG_PATTERN = /^(0[1-9]|[12][0-9]|3[01])_(0[1-9]|1[0-2])_20[0-9]{2}$/
const IDENTIFIER_PATTERN = /^[A-Za-z0-9_-]+$/
const DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/
const DAY_MS = 24 * 60 * 60 * 1000
const COLLECT_STATEMENT_TIMEOUT = '5min'

interface ITables {
  tag: string
  backup: string
  keys: string
  progress: string
}

interface IKeyCount {
  keys: number
  copies: number
}

interface ICommonOptions {
  tag: string
  platforms: string[]
  dryRun: boolean
}

interface ICollectOptions extends ICommonOptions {
  from?: Date
  to: Date
  windowDays: number
}

function tablesFor(tag: string): ITables {
  return {
    tag,
    backup: `"activityRelations_${tag}"`,
    keys: `"activityRelationsNullChannelKeys_${tag}"`,
    progress: `"activityRelationsNullChannelProgress_${tag}"`,
  }
}

async function ensureTables(db: DbConnection, tables: ITables): Promise<void> {
  await db.tx(async (t) => {
    await t.none(`
      create table if not exists ${tables.backup} (
        like "activityRelations",
        "keptActivityId" uuid not null,
        "batchId" uuid not null,
        "deletedAt" timestamptz not null default now()
      )
    `)
    await t.none(`
      create table if not exists ${tables.keys} (
        id bigserial primary key,
        "timestamp" timestamptz not null,
        platform text not null,
        type text not null,
        "sourceId" text not null,
        "segmentId" uuid not null,
        copies integer not null,
        status text not null default 'pending',
        "batchId" uuid,
        "doneAt" timestamptz,
        unique ("timestamp", platform, type, "sourceId", "segmentId")
      )
    `)
    await t.none(`
      create index if not exists "ix_activityRelationsNullChannelKeys_${tables.tag}_pending"
      on ${tables.keys} (platform, "segmentId", id) where status = 'pending'
    `)
    await t.none(`
      create table if not exists ${tables.progress} (
        name text primary key,
        "windowEnd" timestamptz not null,
        "updatedAt" timestamptz not null default now()
      )
    `)
  })
}

async function collectStart(
  db: DbConnection,
  tables: ITables,
  options: ICollectOptions,
): Promise<Date> {
  if (options.from) {
    return options.from
  }

  const progressExists = await db.one<{ exists: boolean }>(
    `select to_regclass($(name)) is not null as exists`,
    { name: tables.progress },
  )
  if (progressExists.exists) {
    const progress = await db.oneOrNone<{ windowEnd: Date }>(
      `select "windowEnd" from ${tables.progress} where name = 'collect'`,
    )
    if (progress) {
      return new Date(progress.windowEnd)
    }
  }

  const oldest = await db.one<{ min: Date | null }>(
    `select min("timestamp") as min from "activityRelations"`,
  )
  if (!oldest.min) {
    throw new Error('activityRelations is empty')
  }
  const oldestMs = new Date(oldest.min).getTime()
  return new Date(oldestMs - (oldestMs % DAY_MS))
}

async function collect(db: DbConnection, options: ICollectOptions): Promise<void> {
  const tables = tablesFor(options.tag)
  if (!options.dryRun) {
    await ensureTables(db, tables)
  }

  const start = await collectStart(db, tables, options)

  log.info(
    { from: start.toISOString(), to: options.to.toISOString(), platforms: options.platforms },
    'Collecting NULL-channel duplicate keys...',
  )

  const totals = { keys: 0, copies: 0 }
  let windowStart = start
  while (windowStart < options.to) {
    const windowEnd = new Date(
      Math.min(windowStart.getTime() + options.windowDays * DAY_MS, options.to.getTime()),
    )
    const started = Date.now()

    const found: IKeyCount = await db.tx(async (t) => {
      await t.none(`set local statement_timeout = '${COLLECT_STATEMENT_TIMEOUT}'`)
      const duplicateKeys = `
        select "timestamp", platform, type, "sourceId", "segmentId", count(*)::int as copies
        from "activityRelations"
        where "timestamp" >= $(windowStart)
          and "timestamp" < $(windowEnd)
          and channel is null
          and platform in ($(platforms:csv))
        group by "timestamp", platform, type, "sourceId", "segmentId"
        having count(*) > 1
      `
      const params = { windowStart, windowEnd, platforms: options.platforms }

      if (options.dryRun) {
        return t.one(
          `select count(*)::int as keys, coalesce(sum(copies), 0)::int as copies from (${duplicateKeys}) k`,
          params,
        )
      }

      const inserted: IKeyCount = await t.one(
        `
          with inserted as (
            insert into ${tables.keys} ("timestamp", platform, type, "sourceId", "segmentId", copies)
            ${duplicateKeys}
            on conflict ("timestamp", platform, type, "sourceId", "segmentId") do nothing
            returning copies
          )
          select count(*)::int as keys, coalesce(sum(copies), 0)::int as copies from inserted
        `,
        params,
      )
      await t.none(
        `
          insert into ${tables.progress} (name, "windowEnd") values ('collect', $(windowEnd))
          on conflict (name) do update set "windowEnd" = excluded."windowEnd", "updatedAt" = now()
        `,
        { windowEnd },
      )
      return inserted
    })

    totals.keys += found.keys
    totals.copies += found.copies
    log.info(
      {
        windowStart: windowStart.toISOString(),
        windowEnd: windowEnd.toISOString(),
        keys: found.keys,
        copies: found.copies,
        totalKeys: totals.keys,
        totalCopies: totals.copies,
        ms: Date.now() - started,
      },
      options.dryRun ? 'Window scanned (dry run)' : 'Window collected',
    )
    windowStart = windowEnd
  }

  log.info(totals, options.dryRun ? 'Collect dry run done' : 'Collect done')
}

async function status(db: DbConnection, options: ICommonOptions): Promise<void> {
  const tables = tablesFor(options.tag)
  const rows = await db.any<{
    platform: string
    status: string
    keys: number
    copies: number
    duplicates: number
  }>(
    `
      select platform, status, count(*)::int as keys, sum(copies)::int as copies,
        sum(copies - 1)::int as duplicates
      from ${tables.keys}
      group by platform, status
      order by platform, status
    `,
  )
  const progress = await db.oneOrNone<{ windowEnd: Date }>(
    `select "windowEnd" from ${tables.progress} where name = 'collect'`,
  )
  const backup = await db.one<{ rows: number }>(
    `select count(*)::int as rows from ${tables.backup}`,
  )

  log.info({ collectedUntil: progress?.windowEnd ?? null, backupRows: backup.rows }, 'Progress')
  for (const row of rows) {
    log.info(row, 'Keys')
  }
}

function getFlagValue(args: string[], flag: string): string | undefined {
  const index = args.indexOf(flag)
  if (index === -1) {
    return undefined
  }
  const value = args[index + 1]
  if (value === undefined || value.startsWith('--')) {
    throw new Error(`${flag} needs a value`)
  }
  return value
}

function parseDate(flag: string, value: string | undefined): Date | undefined {
  if (value === undefined) {
    return undefined
  }
  if (!DATE_PATTERN.test(value) || Number.isNaN(Date.parse(value))) {
    throw new Error(`${flag} must be a valid YYYY-MM-DD date, got '${value}'`)
  }
  return new Date(`${value}T00:00:00Z`)
}

function parseCommon(args: string[]): ICommonOptions {
  const tag = getFlagValue(args, '--tag')
  if (!tag || !TAG_PATTERN.test(tag)) {
    throw new Error(`--tag is required as DD_MM_YYYY, got '${tag ?? ''}'`)
  }

  const platformsArg = getFlagValue(args, '--platforms')
  const platforms = platformsArg
    ? platformsArg
        .split(',')
        .map((p) => p.trim())
        .filter(Boolean)
    : DEFAULT_PLATFORMS
  if (platforms.length === 0 || platforms.some((p) => !IDENTIFIER_PATTERN.test(p))) {
    throw new Error(`--platforms must be comma-separated platform names, got '${platformsArg}'`)
  }

  return { tag, platforms, dryRun: args.includes('--dry-run') }
}

function parseCollect(args: string[]): ICollectOptions {
  const from = parseDate('--from', getFlagValue(args, '--from'))
  const to = parseDate('--to', getFlagValue(args, '--to')) ?? new Date()
  const windowDaysArg = getFlagValue(args, '--window-days')
  const windowDays = windowDaysArg === undefined ? 7 : Number(windowDaysArg)
  if (!Number.isInteger(windowDays) || windowDays < 1 || windowDays > 366) {
    throw new Error(`--window-days must be an integer from 1 to 366, got '${windowDaysArg}'`)
  }
  if (from && from >= to) {
    throw new Error('--from must be before --to')
  }

  return { ...parseCommon(args), from, to, windowDays }
}

function printHelp(): void {
  log.info(`
    Usage: pnpm run cleanup-null-channel-duplicates -- <collect|status> --tag <DD_MM_YYYY> [options]
    See the header of src/bin/cleanup-null-channel-duplicates.ts for the options.
  `)
}

async function main(): Promise<void> {
  const [command, ...args] = process.argv.slice(2)
  if (!command || command === '--help' || command === '-h') {
    printHelp()
    process.exit(command ? 0 : 1)
  }

  const db = await getDbConnection(WRITE_DB_CONFIG(), 1)

  switch (command) {
    case 'collect':
      await collect(db, parseCollect(args))
      break
    case 'status':
      await status(db, parseCommon(args))
      break
    default:
      printHelp()
      throw new Error(`Unknown command '${command}'`)
  }
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    log.error(error, 'Cleanup failed')
    process.exit(1)
  })
