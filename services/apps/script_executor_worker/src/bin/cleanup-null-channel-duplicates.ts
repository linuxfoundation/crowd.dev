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
 *   clean    Take pending keys platform by platform, in --platforms order, in transactions of
 *            whole keys up to --batch-rows rows. In each: lock the key's NULL-channel copies,
 *            keep the newest (updatedAt, createdAt, activityId, as data-sink-worker reads it),
 *            carry parentId, conversationId and sourceParentId over to it where it has none,
 *            set its updatedAt past every deleted copy's, and move the other copies to the
 *            backup table with the kept activityId.
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
 * clean options:
 *   --segments    Only keys of these segment ids (comma-separated)
 *   --batch-rows  Rows per transaction, whole keys only (default 20000)
 *   --max-rows    Stop after deleting this many rows
 *
 * Environment Variables Required:
 *   CROWD_DB_WRITE_HOST, CROWD_DB_PORT, CROWD_DB_USERNAME, CROWD_DB_PASSWORD, CROWD_DB_DATABASE
 */
import { randomUUID } from 'crypto'

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
const UUID_PATTERN = /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/
const DAY_MS = 24 * 60 * 60 * 1000
const COLLECT_STATEMENT_TIMEOUT = '5min'
const CLEAN_LOCK_TIMEOUT = '5s'
const CLEAN_STATEMENT_TIMEOUT = '60s'
const KEYS_PER_FETCH = 1000
const LOCK_RETRIES = 3
const LOCK_RETRY_DELAY_MS = 5000
const LOCK_NOT_AVAILABLE = '55P03'

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

interface ICleanOptions extends ICommonOptions {
  segmentIds: string[]
  batchRows: number
  maxRows?: number
}

interface IKey {
  id: string
  timestamp: Date
  platform: string
  type: string
  sourceId: string
  segmentId: string
  copies: number
}

interface IBatchResult {
  deleted: number
  kept: number
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
          and type is not null
          and "sourceId" is not null
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

async function activityRelationColumns(db: DbConnection): Promise<string[]> {
  const rows: { name: string }[] = await db.any(
    `
      select column_name as name from information_schema.columns
      where table_schema = 'public' and table_name = 'activityRelations'
      order by ordinal_position
    `,
  )
  return rows.map((r) => r.name)
}

async function assertBackupMatches(
  db: DbConnection,
  tables: ITables,
  columns: string[],
): Promise<void> {
  const rows: { name: string }[] = await db.any(
    `
      select column_name as name from information_schema.columns
      where table_schema = 'public' and table_name = $(table)
    `,
    { table: tables.backup.replace(/"/g, '') },
  )
  const backupColumns = new Set(rows.map((r) => r.name))
  const missing = columns.filter((c) => !backupColumns.has(c))
  if (missing.length > 0) {
    throw new Error(`Backup table ${tables.backup} is missing columns: ${missing.join(', ')}`)
  }
}

async function pendingKeys(
  db: DbConnection,
  tables: ITables,
  platform: string,
  options: ICleanOptions,
): Promise<IKey[]> {
  const keys: IKey[] = await db.any(
    `
      select id, "timestamp", platform, type, "sourceId", "segmentId", copies
      from ${tables.keys}
      where status = 'pending'
        and platform = $(platform)
        ${options.segmentIds.length > 0 ? `and "segmentId" in ($(segmentIds:csv))` : ''}
      order by id
      limit ${KEYS_PER_FETCH}
    `,
    { platform, segmentIds: options.segmentIds },
  )

  const batch: IKey[] = []
  let rows = 0
  for (const key of keys) {
    if (batch.length > 0 && rows + key.copies > options.batchRows) {
      break
    }
    batch.push(key)
    rows += key.copies
  }
  return batch
}

function keyParams(keys: IKey[]) {
  return {
    timestamps: keys.map((k) => k.timestamp),
    platforms: keys.map((k) => k.platform),
    types: keys.map((k) => k.type),
    sourceIds: keys.map((k) => k.sourceId),
    segmentIds: keys.map((k) => k.segmentId),
  }
}

const BATCH_KEYS = `
  select *
  from unnest(
    $(timestamps)::timestamptz[],
    $(platforms)::text[],
    $(types)::text[],
    $(sourceIds)::text[],
    $(segmentIds)::uuid[]
  ) as k("timestamp", platform, type, "sourceId", "segmentId")
`

const KEY_JOIN = `
  ar."timestamp" = k."timestamp"
  and ar.platform = k.platform
  and ar.type = k.type
  and ar."sourceId" = k."sourceId"
  and ar."segmentId" = k."segmentId"
  and ar.channel is null
`

// Same order as data-sink-worker's duplicate lookup (findActivityRelationsByDedupKeys), so the
// copy kept is the one the worker reads and writes.
const RANKED = `
  select ar.*,
    first_value(ar."activityId") over w as "keptActivityId",
    row_number() over w as rank
  from "activityRelations" ar
  join k on ${KEY_JOIN}
  window w as (
    partition by ar."timestamp", ar.platform, ar.type, ar."sourceId", ar."segmentId"
    order by ar."updatedAt" desc, ar."createdAt" desc, ar."activityId" desc
  )
`

async function cleanBatch(
  db: DbConnection,
  tables: ITables,
  columns: string[],
  keys: IKey[],
  dryRun: boolean,
): Promise<IBatchResult> {
  const params = { ...keyParams(keys), batchId: randomUUID(), keyIds: keys.map((k) => k.id) }

  if (dryRun) {
    return db.tx(async (t) => {
      await t.none(`set transaction read only`)
      await t.none(`set local statement_timeout = '${CLEAN_STATEMENT_TIMEOUT}'`)
      const result: IBatchResult = await t.one(
        `
          with k as (${BATCH_KEYS}), ranked as (${RANKED})
          select count(*) filter (where rank > 1)::int as deleted,
            count(distinct "keptActivityId") filter (where rank > 1)::int as kept
          from ranked r2 where r2."keptActivityId" = ranked."activityId" and r2.rank > 1
            ))::int as kept
          from ranked
        `,
        params,
      )
      return result
    })
  }

  const columnList = columns.map((c) => `"${c}"`).join(', ')
  return db.tx(async (t) => {
    await t.none(`set local lock_timeout = '${CLEAN_LOCK_TIMEOUT}'`)
    await t.none(`set local statement_timeout = '${CLEAN_STATEMENT_TIMEOUT}'`)

    // FOR UPDATE can't be combined with window functions, so lock first and rank after
    await t.one(
      `
        with k as (${BATCH_KEYS})
        select count(*) as locked from (
          select ar."activityId" from "activityRelations" ar join k on ${KEY_JOIN}
          for update of ar
        ) l
      `,
      params,
    )

    const result: IBatchResult = await t.one(
      `
        with k as (${BATCH_KEYS}),
        ranked as (${RANKED}),
        dropped as (select * from ranked where rank > 1),
        carried as (
          select "keptActivityId",
            max("updatedAt") as "maxUpdatedAt",
            (array_agg("parentId" order by rank) filter (where "parentId" is not null))[1] as "parentId",
            (array_agg("conversationId" order by rank) filter (where "conversationId" is not null))[1] as "conversationId",
            (array_agg("sourceParentId" order by rank) filter (where "sourceParentId" is not null))[1] as "sourceParentId"
          from dropped
          group by "keptActivityId"
        ),
        kept as (
          update "activityRelations" ar set
            "parentId" = coalesce(ar."parentId", c."parentId"),
            "conversationId" = coalesce(ar."conversationId", c."conversationId"),
            "sourceParentId" = coalesce(ar."sourceParentId", c."sourceParentId"),
            "updatedAt" = greatest(clock_timestamp(), c."maxUpdatedAt" + interval '1 millisecond')
          from carried c
          where ar."activityId" = c."keptActivityId"
          returning ar."activityId"
        ),
        deleted as (
          delete from "activityRelations" ar
          using dropped d
          where ar."activityId" = d."activityId"
          returning ar.*, d."keptActivityId"
        ),
        backed_up as (
          insert into ${tables.backup} (${columnList}, "keptActivityId", "batchId")
          select ${columnList}, "keptActivityId", $(batchId) from deleted
          returning 1
        )
        select (select count(*) from backed_up)::int as deleted,
          (select count(*) from kept)::int as kept
      `,
      params,
    )

    await t.none(
      `
        update ${tables.keys} set status = 'done', "batchId" = $(batchId), "doneAt" = now()
        where id in ($(keyIds:csv))
      `,
      params,
    )
    return result
  })
}

async function withLockRetries<T>(run: () => Promise<T>): Promise<T> {
  for (let attempt = 1; ; attempt++) {
    try {
      return await run()
    } catch (err) {
      if (err?.code !== LOCK_NOT_AVAILABLE || attempt >= LOCK_RETRIES) {
        throw err
      }
      log.warn({ attempt }, 'Rows of the batch are locked by another transaction, retrying...')
      await new Promise((resolve) => setTimeout(resolve, LOCK_RETRY_DELAY_MS))
    }
  }
}

async function clean(db: DbConnection, options: ICleanOptions): Promise<void> {
  const tables = tablesFor(options.tag)
  const keysExist: { exists: boolean } = await db.one(
    `select to_regclass($(name)) is not null as exists`,
    { name: tables.keys },
  )
  if (!keysExist.exists) {
    throw new Error(`Work table ${tables.keys} doesn't exist: run collect first`)
  }

  const columns = await activityRelationColumns(db)
  await assertBackupMatches(db, tables, columns)

  log.info(
    {
      platforms: options.platforms,
      segmentIds: options.segmentIds,
      batchRows: options.batchRows,
      maxRows: options.maxRows ?? null,
    },
    options.dryRun ? 'Cleaning (dry run)...' : 'Cleaning...',
  )

  const totals = { batches: 0, keys: 0, deleted: 0, kept: 0 }
  for (const platform of options.platforms) {
    for (;;) {
      if (options.maxRows !== undefined && totals.deleted >= options.maxRows) {
        log.info(totals, 'Reached --max-rows, stopping')
        return
      }

      const keys = await pendingKeys(db, tables, platform, options)
      if (keys.length === 0) {
        break
      }

      const started = Date.now()
      const result = await withLockRetries(() =>
        cleanBatch(db, tables, columns, keys, options.dryRun),
      )
      totals.batches += 1
      totals.keys += keys.length
      totals.deleted += result.deleted
      totals.kept += result.kept
      log.info(
        { platform, keys: keys.length, ...result, ms: Date.now() - started, totals },
        options.dryRun ? 'Batch checked (dry run)' : 'Batch cleaned',
      )

      if (options.dryRun) {
        break
      }
    }
  }

  log.info(totals, options.dryRun ? 'Clean dry run done' : 'Clean done')
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

function parsePositiveInt(flag: string, value: string | undefined): number | undefined {
  if (value === undefined) {
    return undefined
  }
  const parsed = Number(value)
  if (!Number.isInteger(parsed) || parsed < 1) {
    throw new Error(`${flag} must be a positive integer, got '${value}'`)
  }
  return parsed
}

function parseClean(args: string[]): ICleanOptions {
  const segmentsArg = getFlagValue(args, '--segments')
  const segmentIds = segmentsArg
    ? segmentsArg
        .split(',')
        .map((s) => s.trim())
        .filter(Boolean)
    : []
  if (segmentIds.some((id) => !UUID_PATTERN.test(id))) {
    throw new Error(`--segments must be comma-separated segment ids, got '${segmentsArg}'`)
  }

  return {
    ...parseCommon(args),
    segmentIds,
    batchRows: parsePositiveInt('--batch-rows', getFlagValue(args, '--batch-rows')) ?? 20000,
    maxRows: parsePositiveInt('--max-rows', getFlagValue(args, '--max-rows')),
  }
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
    Usage: pnpm run cleanup-null-channel-duplicates -- <collect|clean|status> --tag <DD_MM_YYYY> [options]
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
    case 'clean':
      await clean(db, parseClean(args))
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
