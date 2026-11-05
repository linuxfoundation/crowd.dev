/**
 * NULL-channel duplicate cleanup for activityRelations (linuxfoundation/lfx-cdp-insights-ops#852)
 *
 * Before #850, activities without a channel got a new activityRelations row on every re-send, so
 * one dedup key ("timestamp", platform, type, "sourceId", "segmentId" with a NULL channel) can have
 * many copies. This script keeps one copy per key, the one data-sink-worker reads (newest by
 * "updatedAt", "createdAt", "activityId"), and moves the other copies to a dated backup table, in
 * short transactions that can be stopped and resumed.
 *
 * Commands:
 *   collect  Walk the dedup index in time windows and record every key with more than one
 *            NULL-channel copy in the work table. Resumes from the last finished window of the
 *            same platform list.
 *   clean    Take pending keys platform by platform (--platforms order), whole keys up to
 *            --batch-rows rows per transaction. Each transaction:
 *              1. locks the key's NULL-channel copies (NOWAIT: never waits on app transactions),
 *              2. ranks only the locked copies and keeps rank 1,
 *              3. carries parentId, conversationId and sourceParentId to the kept copy where it
 *                 has none, and moves its updatedAt at least 2 ms past every deleted copy's
 *                 (Tinybird stores milliseconds) only when it isn't already,
 *              4. moves the other copies to the backup table with the kept activityId,
 *              5. checks that every key still has exactly one locked copy and that every deleted
 *                 row is in the backup, and rolls back otherwise,
 *              6. marks the keys and records the batch's metrics in the batches table.
 *            Keys whose batch keeps failing on locks, deadlocks or timeouts are marked deferred
 *            and skipped; --retry-deferred takes them again.
 *   status   Print progress from the work, batches and progress tables.
 *
 * Usage:
 *   npx tsx src/bin/cleanup-null-channel-duplicates.ts <command> --tag <DD_MM_YYYY> [options]
 *   (or pnpm run cleanup-null-channel-duplicates -- <command> ...)
 *
 * Common options:
 *   --tag DD_MM_YYYY   Shared by every run of one cleanup. Tables: "activityRelations_<tag>"
 *                      (backup), "activityRelationsNullChannelKeys_<tag>" (work),
 *                      "activityRelationsNullChannelBatches_<tag>" (batch metrics),
 *                      "activityRelationsNullChannelProgress_<tag>" (collect progress).
 *   --platforms a,b    Default: meetings,linkedin,tnc,cvent,slack,stackoverflow
 *   --dry-run          Read only: report what would happen
 *   --yes              Skip the confirmation prompt (required when stdin isn't a terminal)
 *
 * collect options:
 *   --from YYYY-MM-DD  First day to scan (default: the last finished window, else the oldest row)
 *   --to YYYY-MM-DD    Scan up to this day, exclusive (default: the day after the newest row)
 *   --window-days N    Days per window (default 7, halved automatically when a window times out)
 *
 * clean options:
 *   --segments a,b     Only keys of these segment ids
 *   --batch-rows N     Rows per transaction, whole keys only (default 20000, max 100000)
 *   --max-rows N       Stop after deleting this many rows
 *   --until ISO        Stop at this time, e.g. 2026-11-06T22:00:00Z (at most 48 h ahead, 7 days
 *                      with --wait-nightly)
 *                      A real run needs --max-rows or --until.
 *   --max-slot-lag-mb N  Wait before a batch while sequin_slot or sequin_slot_2 is further behind
 *                      (default 1024). A missing slot stops the run.
 *   --skip-slot-guard  Run without the slot check (for environments without Sequin)
 *   --allow-nightly    Also run 00:30-05:30 UTC (nightly meetings export); needs --until
 *   --wait-nightly     Pause during 00:30-05:30 UTC and continue afterwards instead of stopping;
 *                      allows --until up to 7 days ahead
 *   --allow-partial-collect  Clean although collect hasn't caught up for these platforms
 *   --retry-deferred   Also take keys deferred by earlier runs
 *   --sample-batches N Dry run: batches to check per platform (default 3)
 *
 * Stopping: Ctrl-C, SIGTERM or SIGHUP stops after the current batch; a second one exits at once
 * (the open transaction rolls back).
 *
 * Exit codes: 0 nothing left to do, 2 stopped by --max-rows/--until/nightly window/signal,
 * 3 finished but some keys were deferred (rerun with --retry-deferred), 4 database connection
 * lost (safe to restart), 1 error incl. a failed safety check (look before restarting), 64 usage.
 *
 * Environment: CROWD_DB_WRITE_HOST, CROWD_DB_PORT, CROWD_DB_USERNAME, CROWD_DB_PASSWORD,
 * CROWD_DB_DATABASE. Logs are JSON lines with a constant "msg" per event (run.config, run.plan,
 * collect.window, batch.done, batch.retry, batch.deferred, slot.wait, run.progress, run.summary,
 * ...); pipe through jq.
 */
import { randomUUID } from 'crypto'
import { hostname } from 'os'
import * as readline from 'readline'

import {
  DbConnection,
  WRITE_DB_CONFIG,
  getDbConnection,
} from '@crowd/data-access-layer/src/database'
import { getServiceChildLogger } from '@crowd/logging'

const baseLog = getServiceChildLogger('cleanup-null-channel-duplicates')

const DEFAULT_PLATFORMS = ['meetings', 'linkedin', 'tnc', 'cvent', 'slack', 'stackoverflow']
const TAG_PATTERN = /^(0[1-9]|[12][0-9]|3[01])_(0[1-9]|1[0-2])_20[0-9]{2}$/
const IDENTIFIER_PATTERN = /^[A-Za-z0-9_-]+$/
const DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/
const UUID_PATTERN = /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/
const DAY_MS = 24 * 60 * 60 * 1000
const MAX_UNTIL_MS = 48 * 60 * 60 * 1000
const MAX_UNTIL_WAIT_NIGHTLY_MS = 7 * DAY_MS
const MAX_BATCH_ROWS = 100_000

const COLLECT_STATEMENT_TIMEOUT_MS = 5 * 60_000
const CLEAN_STATEMENT_TIMEOUT_MS = 30_000
// the lock step reads every copy's heap page, cold in prod: ~15 s for a 20k-row batch
const CLEAN_LOCK_STATEMENT_TIMEOUT_MS = 60_000
const RUN_LOCK_WAIT_MS = 2 * 60_000
const SESSION_QUERY_TIMEOUT_MS = 10 * 60_000
const CLEAN_LOCK_TIMEOUT = '5s'
const IDLE_IN_TRANSACTION_TIMEOUT = '60s'

const RETRY_DELAYS_MS = [5_000, 15_000, 45_000]
const RETRYABLE_CODES: Record<string, string> = {
  '55P03': 'lock not available',
  '40P01': 'deadlock',
  '40001': 'serialization failure',
  '57014': 'statement timeout',
}

const NIGHTLY_EXPORT_START_MINUTES = 30
const NIGHTLY_EXPORT_END_MINUTES = 5 * 60 + 30
const SLOTS = ['sequin_slot', 'sequin_slot_2']
const SLOT_POLL_MS = 30_000
const PROGRESS_EVERY_MS = 5 * 60_000
const PROGRESS_EVERY_BATCHES = 50

const EXIT_DONE = 0
const EXIT_ERROR = 1
const EXIT_STOPPED = 2
const EXIT_DEFERRED = 3
const EXIT_CONNECTION = 4
const EXIT_USAGE = 64
const EXIT_SIGNAL = 130

const COMMAND_FLAGS: Record<string, { values: string[]; switches: string[] }> = {
  collect: {
    values: ['--tag', '--platforms', '--from', '--to', '--window-days'],
    switches: ['--dry-run', '--yes'],
  },
  clean: {
    values: [
      '--tag',
      '--platforms',
      '--segments',
      '--batch-rows',
      '--max-rows',
      '--until',
      '--max-slot-lag-mb',
      '--sample-batches',
    ],
    switches: [
      '--dry-run',
      '--yes',
      '--skip-slot-guard',
      '--allow-nightly',
      '--wait-nightly',
      '--allow-partial-collect',
      '--retry-deferred',
    ],
  },
  status: { values: ['--tag', '--platforms'], switches: [] },
}

type Session = Awaited<ReturnType<DbConnection['connect']>>
type Log = ReturnType<typeof getServiceChildLogger>

class UsageError extends Error {}
class InvariantError extends Error {}

interface ITables {
  tag: string
  backup: string
  keys: string
  batches: string
  progress: string
  raw: { backup: string; keys: string; batches: string; progress: string }
}

interface ICommonOptions {
  tag: string
  platforms: string[]
  dryRun: boolean
  yes: boolean
}

interface ICollectOptions extends ICommonOptions {
  from?: Date
  to?: Date
  windowDays: number
}

interface ICleanOptions extends ICommonOptions {
  segmentIds: string[]
  batchRows: number
  maxRows?: number
  until?: Date
  maxSlotLagBytes: number
  skipSlotGuard: boolean
  allowNightly: boolean
  waitNightly: boolean
  allowPartialCollect: boolean
  retryDeferred: boolean
  sampleBatches: number
}

interface IKey {
  id: string
  copies: number
}

interface IBatchMetrics {
  lockedRows: number
  foundRows: number
  keysFound: number
  keysWithDuplicates: number
  dropped: number
  deleted: number
  backedUp: number
  keptUpdated: number
  maxCopies: number
  crossYear: number
  deletedByYear: Record<string, number>
  carriedParentId: number
  carriedConversationId: number
  carriedSourceParentId: number
  parentConflicts: number
  segments: number
  perKey: Record<string, number>
  walBytes: number
  ms: { lock: number; clean: number; verify: number; mark: number }
}

interface IRunTotals {
  batches: number
  keys: number
  deleted: number
  keptUpdated: number
  keysNoDuplicates: number
  crossYear: number
  carried: number
  parentConflicts: number
  walBytes: number
  retries: number
  deferredKeys: number
  slotWaitMs: number
  maxSlotLagBytes: number
}

const stopState = { requested: false, reason: '' }

function tablesFor(tag: string): ITables {
  const raw = {
    backup: `activityRelations_${tag}`,
    keys: `activityRelationsNullChannelKeys_${tag}`,
    batches: `activityRelationsNullChannelBatches_${tag}`,
    progress: `activityRelationsNullChannelProgress_${tag}`,
  }
  return {
    tag,
    backup: `"${raw.backup}"`,
    keys: `"${raw.keys}"`,
    batches: `"${raw.batches}"`,
    progress: `"${raw.progress}"`,
    raw,
  }
}

function sleep(ms: number, stop: () => boolean): Promise<void> {
  return new Promise((resolve) => {
    const started = Date.now()
    const tick = () => {
      if (stop() || Date.now() - started >= ms) {
        resolve()
      } else {
        setTimeout(tick, Math.min(1000, ms - (Date.now() - started)))
      }
    }
    tick()
  })
}

// pg error fields the bunyan serializer drops
function pgFields(err: unknown): Record<string, unknown> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const e = err as any
  return {
    err,
    pgCode: e?.code,
    pgDetail: e?.detail,
    pgHint: e?.hint,
    pgTable: e?.table,
    pgConstraint: e?.constraint,
    pgWhere: e?.where,
  }
}

function isConnectionError(err: unknown): boolean {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const e = err as any
  const code = typeof e?.code === 'string' ? e.code : ''
  return (
    code.startsWith('08') ||
    ['57P01', '57P02', '57P03', 'ECONNRESET', 'ECONNREFUSED', 'ETIMEDOUT', 'EPIPE'].includes(
      code,
    ) ||
    /Connection terminated|connection error|Query read timeout|not queryable/i.test(
      e?.message ?? '',
    )
  )
}

async function tableExists(db: Session, rawName: string): Promise<boolean> {
  const row: { exists: boolean } = await db.one(
    `select to_regclass($(name)) is not null as exists`,
    { name: `public."${rawName}"` },
  )
  return row.exists
}

async function ensureTables(db: Session, tables: ITables): Promise<void> {
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
        deleted integer,
        attempts integer not null default 0,
        error text,
        "batchId" uuid,
        "collectedAt" timestamptz not null default now(),
        "doneAt" timestamptz,
        unique ("timestamp", platform, type, "sourceId", "segmentId")
      )
    `)
    await t.none(`
      create index if not exists "ix_${tables.raw.keys}_open"
      on ${tables.keys} (platform, id) where status in ('pending', 'deferred')
    `)
    await t.none(`
      create index if not exists "ix_${tables.raw.keys}_open_segment"
      on ${tables.keys} (platform, "segmentId", id) where status in ('pending', 'deferred')
    `)
    await t.none(`
      create table if not exists ${tables.batches} (
        "batchId" uuid primary key,
        "runId" uuid not null,
        platform text not null,
        "keyIdMin" bigint not null,
        "keyIdMax" bigint not null,
        keys integer not null,
        "expectedRows" integer not null,
        metrics jsonb not null,
        attempts integer not null,
        "startedAt" timestamptz not null,
        "finishedAt" timestamptz not null default now()
      )
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

async function acquireRunLock(db: Session, log: Log, tables: ITables): Promise<void> {
  const name = `cleanup-null-channel-duplicates:${tables.tag}`
  const started = Date.now()
  for (;;) {
    const row: { locked: boolean } = await db.one(
      `select pg_try_advisory_lock(hashtext($(name))) as locked`,
      { name },
    )
    if (row.locked) {
      return
    }
    // an aborted run's backend keeps the lock until its last statement ends (up to a minute)
    const holders = await runLockHolders(db, tables)
    if (Date.now() - started >= RUN_LOCK_WAIT_MS || stopState.requested) {
      throw new UsageError(
        `Another collect or clean run for tag ${tables.tag} holds the run lock: ${JSON.stringify(holders)}`,
      )
    }
    log.warn({ holders }, 'run.lock-wait')
    await sleep(10_000, () => stopState.requested)
  }
}

async function runLockHolders(db: Session, tables: ITables) {
  return db.any(
    `
      select a.pid, a.application_name as "applicationName", a.client_addr::text as "clientAddr",
        a.state, a.backend_start::text as "backendStart", left(a.query, 120) as query
      from pg_locks l
      join pg_stat_activity a on a.pid = l.pid
      where l.locktype = 'advisory' and l.granted
        and l.objid = (hashtext($(name)) & 4294967295)::oid
    `,
    { name: `cleanup-null-channel-duplicates:${tables.tag}` },
  )
}

function collectProgressName(platforms: string[]): string {
  return `collect:${[...platforms].sort().join(',')}`
}

async function epochMs(db: Session, sql: string, params = {}): Promise<number | null> {
  const row: { ms: string | null } = await db.one(sql, params)
  return row.ms === null ? null : Number(row.ms)
}

// ---------------------------------------------------------------------------------------------
// collect
// ---------------------------------------------------------------------------------------------

async function collect(db: Session, log: Log, options: ICollectOptions): Promise<number> {
  const tables = tablesFor(options.tag)
  const progressName = collectProgressName(options.platforms)

  const storedEnd = (await tableExists(db, tables.raw.progress))
    ? await epochMs(
        db,
        `select (extract(epoch from "windowEnd") * 1000)::bigint::text as ms from ${tables.progress} where name = $(name)`,
        { name: progressName },
      )
    : null
  const oldest = await epochMs(
    db,
    `select (extract(epoch from min("timestamp")) * 1000)::bigint::text as ms from "activityRelations"`,
  )
  const newest = await epochMs(
    db,
    `select (extract(epoch from max("timestamp")) * 1000)::bigint::text as ms from "activityRelations"`,
  )
  if (oldest === null || newest === null) {
    throw new UsageError('activityRelations has no rows with a timestamp')
  }

  let startMs: number
  if (options.from) {
    startMs = options.from.getTime()
    if (storedEnd !== null && startMs > storedEnd) {
      log.warn(
        { storedEnd: new Date(storedEnd).toISOString(), from: options.from.toISOString() },
        'collect.gap',
      )
    }
  } else {
    startMs = storedEnd ?? oldest - (oldest % DAY_MS)
  }
  const endMs = options.to ? options.to.getTime() : newest - (newest % DAY_MS) + DAY_MS

  log.info(
    {
      platforms: options.platforms,
      progressName,
      storedEnd: storedEnd === null ? null : new Date(storedEnd).toISOString(),
      oldest: new Date(oldest).toISOString(),
      newest: new Date(newest).toISOString(),
      from: new Date(startMs).toISOString(),
      to: new Date(endMs).toISOString(),
      windowDays: options.windowDays,
      dryRun: options.dryRun,
    },
    'collect.plan',
  )
  if (startMs >= endMs) {
    log.info('collect.uptodate')
    return EXIT_DONE
  }

  if (!options.dryRun) {
    await confirm(
      options,
      `Collect duplicate keys for ${options.platforms.join(', ')} from ${new Date(startMs).toISOString()} to ${new Date(endMs).toISOString()} into ${tables.keys}.`,
    )
    await ensureTables(db, tables)
  }

  const totals = {
    windows: 0,
    skippedDays: 0,
    found: 0,
    inserted: 0,
    reopened: 0,
    copies: 0,
    ms: 0,
  }
  let windowStart = startMs
  let windowDays = options.windowDays
  while (windowStart < endMs) {
    if (stopState.requested) {
      log.info({ reason: stopState.reason, ...totals }, 'collect.stopped')
      return EXIT_STOPPED
    }

    // years without rows (timestamps go from 0026 to 2120): jump to the next day that has any
    const next = await epochMs(
      db,
      `select (extract(epoch from min("timestamp")) * 1000)::bigint::text as ms
       from "activityRelations" where "timestamp" >= $(from)::timestamptz`,
      { from: new Date(windowStart).toISOString() },
    )
    if (next === null || next >= endMs) {
      totals.skippedDays += Math.round((endMs - windowStart) / DAY_MS)
      windowStart = endMs
      break
    }
    const nextDay = next - (next % DAY_MS)
    if (nextDay > windowStart) {
      totals.skippedDays += Math.round((nextDay - windowStart) / DAY_MS)
      windowStart = nextDay
    }

    const windowEnd = Math.min(windowStart + windowDays * DAY_MS, endMs)
    const started = Date.now()
    let result: {
      found: number
      inserted: number
      reopened: number
      copies: number
      maxCopies: number
      byPlatform: Record<string, number>
    }
    try {
      result = await collectWindow(db, tables, options, progressName, windowStart, windowEnd)
    } catch (err) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      if ((err as any)?.code === '57014' && windowDays > 1) {
        windowDays = Math.max(1, Math.floor(windowDays / 2))
        log.warn(
          { ...pgFields(err), windowStart: new Date(windowStart).toISOString(), windowDays },
          'collect.window.timeout',
        )
        continue
      }
      log.error(
        {
          ...pgFields(err),
          windowStart: new Date(windowStart).toISOString(),
          windowEnd: new Date(windowEnd).toISOString(),
        },
        'collect.window.failed',
      )
      throw err
    }

    const ms = Date.now() - started
    totals.windows += 1
    totals.found += result.found
    totals.inserted += result.inserted
    totals.reopened += result.reopened
    totals.copies += result.copies
    totals.ms += ms
    log.info(
      {
        windowStart: new Date(windowStart).toISOString(),
        windowEnd: new Date(windowEnd).toISOString(),
        windowDays,
        ...result,
        ms,
        // share of the time range done; days without rows are skipped, so no ETA
        rangeDonePct: Math.round(((windowEnd - startMs) / (endMs - startMs)) * 1000) / 10,
        totals,
        dryRun: options.dryRun,
      },
      'collect.window',
    )
    windowStart = windowEnd
  }

  log.info({ ...totals, dryRun: options.dryRun }, 'collect.summary')
  return EXIT_DONE
}

async function collectWindow(
  db: Session,
  tables: ITables,
  options: ICollectOptions,
  progressName: string,
  windowStartMs: number,
  windowEndMs: number,
) {
  const params = {
    windowStart: new Date(windowStartMs).toISOString(),
    windowEnd: new Date(windowEndMs).toISOString(),
    platforms: options.platforms,
    progressName,
  }
  const found = `
    select "timestamp", platform, type, "sourceId", "segmentId", count(*)::int as copies
    from "activityRelations"
    where "timestamp" >= $(windowStart)::timestamptz
      and "timestamp" < $(windowEnd)::timestamptz
      and channel is null
      and platform in ($(platforms:csv))
      and type is not null
      and "sourceId" is not null
    group by "timestamp", platform, type, "sourceId", "segmentId"
    having count(*) > 1
  `
  const summary = `
    (select count(*) from found)::int as found,
    (select coalesce(sum(copies), 0) from found)::int as copies,
    (select coalesce(max(copies), 0) from found)::int as "maxCopies",
    (select coalesce(jsonb_object_agg(platform, n), '{}') from (
      select platform, count(*)::int as n from found group by platform) p) as "byPlatform"
  `

  return db.tx(async (t) => {
    if (options.dryRun) {
      await t.none(`set transaction read only`)
    }
    await t.none(`set local statement_timeout = ${COLLECT_STATEMENT_TIMEOUT_MS}`)

    if (options.dryRun) {
      const row = await t.one(
        `with found as (${found}) select 0 as inserted, 0 as reopened, ${summary}`,
        params,
      )
      return row
    }

    const row = await t.one(
      `
        with found as (${found}),
        upserted as (
          insert into ${tables.keys} ("timestamp", platform, type, "sourceId", "segmentId", copies)
          select "timestamp", platform, type, "sourceId", "segmentId", copies from found
          on conflict ("timestamp", platform, type, "sourceId", "segmentId") do update set
            status = 'pending', copies = excluded.copies, deleted = null, error = null,
            "collectedAt" = now()
          -- a finished key that has duplicates again (e.g. a re-send raced the cleanup)
          where ${tables.keys}.status in ('done', 'no_duplicates')
          returning (xmax = 0) as inserted
        )
        select (select count(*) from upserted where inserted)::int as inserted,
          (select count(*) from upserted where not inserted)::int as reopened, ${summary}
      `,
      params,
    )
    // an explicit --to is a re-scan of part of the range, not progress
    if (!options.to) {
      await t.none(
        `
          insert into ${tables.progress} (name, "windowEnd") values ($(progressName), $(windowEnd)::timestamptz)
          on conflict (name) do update set
            "windowEnd" = greatest(${tables.progress}."windowEnd", excluded."windowEnd"),
            "updatedAt" = now()
        `,
        params,
      )
    }
    return row
  })
}

// ---------------------------------------------------------------------------------------------
// clean
// ---------------------------------------------------------------------------------------------

async function activityRelationColumns(db: Session): Promise<string[]> {
  const rows: { name: string }[] = await db.any(
    `
      select column_name as name from information_schema.columns
      where table_schema = 'public' and table_name = 'activityRelations'
      order by ordinal_position
    `,
  )
  return rows.map((r) => r.name)
}

async function assertBackupMatches(db: Session, tables: ITables, columns: string[]) {
  const rows: { name: string }[] = await db.any(
    `
      select column_name as name from information_schema.columns
      where table_schema = 'public' and table_name = $(table)
    `,
    { table: tables.raw.backup },
  )
  const backupColumns = new Set(rows.map((r) => r.name))
  const missing = columns.filter((c) => !backupColumns.has(c))
  if (missing.length > 0) {
    throw new UsageError(
      `Backup table ${tables.backup} is missing columns of activityRelations: ${missing.join(', ')}`,
    )
  }
}

async function slotLags(db: Session): Promise<Record<string, number>> {
  const rows: { slot: string; active: boolean; lag: string | null }[] = await db.any(
    `
      select slot_name as slot, active,
        pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint::text as lag
      from pg_replication_slots
      where slot_name in ($(slots:csv))
    `,
    { slots: SLOTS },
  )
  const missing = SLOTS.filter((s) => !rows.some((r) => r.slot === s && r.lag !== null))
  if (missing.length > 0) {
    throw new UsageError(
      `Replication slot(s) missing or without a confirmed flush position: ${missing.join(', ')}. ` +
        "The lag guard can't work; use --skip-slot-guard only where there is no Sequin.",
    )
  }
  return Object.fromEntries(rows.map((r) => [r.slot, Number(r.lag)]))
}

function inNightlyExport(now: Date): boolean {
  const minutes = now.getUTCHours() * 60 + now.getUTCMinutes()
  return minutes >= NIGHTLY_EXPORT_START_MINUTES && minutes < NIGHTLY_EXPORT_END_MINUTES
}

function stopReason(options: ICleanOptions, deleted: number): string | null {
  const now = new Date()
  if (stopState.requested) {
    return stopState.reason
  }
  if (options.maxRows !== undefined && deleted >= options.maxRows) {
    return 'max-rows'
  }
  if (options.until && now >= options.until) {
    return 'until'
  }
  if (!options.dryRun && !options.allowNightly && inNightlyExport(now)) {
    return 'nightly-window'
  }
  return null
}

const KEYS_CTE = (tables: ITables) => `
  select id as "keyId", "timestamp", platform, type, "sourceId", "segmentId"
  from ${tables.keys}
  where id in ($(keyIds:csv)) and status in ($(statuses:csv))
`

const KEY_JOIN = `
  ar."timestamp" = k."timestamp"
  and ar.platform = k.platform
  and ar.type = k.type
  and ar."sourceId" = k."sourceId"
  and ar."segmentId" = k."segmentId"
  and ar.channel is null
`

// Rank order = data-sink-worker's duplicate lookup (findActivityRelationsByDedupKeys), so the
// copy kept is the one the worker reads and writes.
const RANKED = (onlyLocked: boolean) => `
  select ar.*, k."keyId",
    first_value(ar."activityId") over w as "keptActivityId",
    first_value(ar."createdAt") over w as "keptCreatedAt",
    row_number() over w as rank
  from "activityRelations" ar
  join k on ${KEY_JOIN}
  ${onlyLocked ? `where ar."activityId" = any($(lockedIds)::uuid[])` : ''}
  window w as (
    partition by k."keyId"
    order by ar."updatedAt" desc, ar."createdAt" desc, ar."activityId" desc
  )
`

const STATS = `
  (select count(*) from ranked)::int as "foundRows",
  (select count(distinct "keyId") from ranked)::int as "keysFound",
  (select count(distinct "keyId") from dropped)::int as "keysWithDuplicates",
  (select count(*) from dropped)::int as dropped,
  (select coalesce(max(rank), 0) from ranked)::int as "maxCopies",
  (select count(*) from dropped
    where date_part('year', "createdAt") <> date_part('year', "keptCreatedAt"))::int as "crossYear",
  (select coalesce(jsonb_object_agg(y, n), '{}') from (
    select date_part('year', "createdAt")::int as y, count(*)::int as n from dropped group by 1) x)
    as "deletedByYear",
  (select count(distinct "segmentId") from dropped)::int as segments,
  (select count(*) from plan where "carryParentId")::int as "carriedParentId",
  (select count(*) from plan where "carryConversationId")::int as "carriedConversationId",
  (select count(*) from plan where "carrySourceParentId")::int as "carriedSourceParentId",
  (select count(*) from plan where "parentConflict")::int as "parentConflicts"
`

const PLAN = `
  carried as (
    select "keptActivityId",
      max("updatedAt") as "maxDroppedUpdatedAt",
      (array_agg("parentId" order by rank) filter (where "parentId" is not null))[1] as "parentId",
      count(distinct "parentId") as "parentIds",
      (array_agg("conversationId" order by rank) filter (where "conversationId" is not null))[1] as "conversationId",
      (array_agg("sourceParentId" order by rank) filter (where "sourceParentId" is not null))[1] as "sourceParentId"
    from dropped
    group by "keptActivityId"
  ),
  plan as (
    select kr."activityId",
      greatest(
        kr."updatedAt",
        -- 2 ms: a deleted copy's updatedAt rounded up instead of truncated still can't tie
        date_trunc('milliseconds', c."maxDroppedUpdatedAt") + interval '2 milliseconds'
      ) as "newUpdatedAt",
      kr."updatedAt" as "oldUpdatedAt",
      kr."parentId" is null and c."parentId" is not null as "carryParentId",
      kr."conversationId" is null and c."conversationId" is not null as "carryConversationId",
      kr."sourceParentId" is null and c."sourceParentId" is not null as "carrySourceParentId",
      c."parentIds" > 1
        or (kr."parentId" is not null and c."parentId" is not null and kr."parentId" <> c."parentId")
        as "parentConflict",
      c."parentId", c."conversationId", c."sourceParentId"
    from carried c
    join ranked kr on kr."activityId" = c."keptActivityId"
  )
`

async function cleanBatch(
  db: Session,
  tables: ITables,
  columns: string[],
  keys: IKey[],
  batchId: string,
  statuses: string[],
  dryRun: boolean,
  record: { runId: string; platform: string; attempts: number; startedAt: Date },
): Promise<IBatchMetrics> {
  const params = { keyIds: keys.map((k) => k.id), statuses, batchId }
  const ms = { lock: 0, clean: 0, verify: 0, mark: 0 }

  return db.tx(async (t) => {
    if (dryRun) {
      await t.none(`set transaction read only`)
    }
    await t.none(`set local lock_timeout = '${CLEAN_LOCK_TIMEOUT}'`)
    await t.none(`set local statement_timeout = ${CLEAN_STATEMENT_TIMEOUT_MS}`)
    await t.none(`set local idle_in_transaction_session_timeout = '${IDLE_IN_TRANSACTION_TIMEOUT}'`)

    if (dryRun) {
      let started = Date.now()
      const stats = await t.one(
        `
          with k as (${KEYS_CTE(tables)}),
          ranked as (${RANKED(false)}),
          dropped as (select * from ranked where rank > 1),
          ${PLAN}
          select (select count(*) from ranked)::int as "lockedRows", ${STATS},
            (select count(*) from plan
              where "newUpdatedAt" <> "oldUpdatedAt" or "carryParentId" or "carryConversationId"
                or "carrySourceParentId")::int as "keptUpdated"
        `,
        params,
      )
      ms.clean = Date.now() - started
      started = Date.now()
      return {
        ...stats,
        deleted: stats.dropped,
        backedUp: 0,
        perKey: {},
        walBytes: 0,
        ms,
      }
    }

    const walStart: { lsn: string } = await t.one(`select pg_current_wal_lsn()::text as lsn`)

    await t.none(`set local statement_timeout = ${CLEAN_LOCK_STATEMENT_TIMEOUT_MS}`)
    let started = Date.now()
    // NOWAIT: an app transaction holding a copy fails this batch instead of making it wait, so
    // the script never blocks app writes and never takes part in a deadlock
    const locked: { ids: string[] | null; n: number } = await t.one(
      `
        with k as (${KEYS_CTE(tables)})
        select array_agg(l."activityId") as ids, count(*)::int as n from (
          select ar."activityId" from "activityRelations" ar join k on ${KEY_JOIN}
          order by ar."activityId"
          for update of ar nowait
        ) l
      `,
      params,
    )
    ms.lock = Date.now() - started
    await t.none(`set local statement_timeout = ${CLEAN_STATEMENT_TIMEOUT_MS}`)
    const lockedIds = locked.ids ?? []

    started = Date.now()
    const result = await t.one(
      `
        with k as (${KEYS_CTE(tables)}),
        ranked as (${RANKED(true)}),
        dropped as (select * from ranked where rank > 1),
        ${PLAN},
        kept as (
          update "activityRelations" ar set
            "parentId" = case when p."carryParentId" then p."parentId" else ar."parentId" end,
            "conversationId" = case when p."carryConversationId" then p."conversationId" else ar."conversationId" end,
            "sourceParentId" = case when p."carrySourceParentId" then p."sourceParentId" else ar."sourceParentId" end,
            "updatedAt" = p."newUpdatedAt"
          from plan p
          where ar."activityId" = p."activityId"
            and ar.channel is null
            and (p."newUpdatedAt" <> p."oldUpdatedAt" or p."carryParentId"
              or p."carryConversationId" or p."carrySourceParentId")
          returning ar."activityId"
        ),
        deleted as (
          delete from "activityRelations" ar
          using dropped d
          where ar."activityId" = d."activityId" and ar.channel is null
          returning ar.*, d."keptActivityId", d."keyId"
        ),
        backed_up as (
          insert into ${tables.backup} (${columns.map((c) => `"${c}"`).join(', ')}, "keptActivityId", "batchId")
          select ${columns.map((c) => `"${c}"`).join(', ')}, "keptActivityId", $(batchId) from deleted
          returning 1
        )
        select ${STATS},
          (select count(*) from deleted)::int as deleted,
          (select count(*) from backed_up)::int as "backedUp",
          (select count(*) from kept)::int as "keptUpdated",
          (select coalesce(jsonb_object_agg("keyId", n), '{}') from (
            select "keyId", count(*)::int as n from deleted group by "keyId") d) as "perKey"
      `,
      { ...params, lockedIds },
    )
    ms.clean = Date.now() - started

    started = Date.now()
    const context = { batchId, lockedRows: locked.n, ...result }
    if (result.deleted !== result.dropped || result.backedUp !== result.deleted) {
      throw new InvariantError(
        `Deleted ${result.deleted} rows, expected ${result.dropped}, backed up ${result.backedUp}: ${JSON.stringify(context)}`,
      )
    }
    if (result.foundRows !== locked.n) {
      throw new InvariantError(
        `Ranked ${result.foundRows} rows but locked ${locked.n}: ${JSON.stringify(context)}`,
      )
    }
    // every key that had locked copies must keep exactly one of them
    const remaining: { rows: number; keys: number } = await t.one(
      `
        with k as (${KEYS_CTE(tables)})
        select count(*)::int as rows, count(distinct k."keyId")::int as keys
        from "activityRelations" ar join k on ${KEY_JOIN}
        where ar."activityId" = any($(lockedIds)::uuid[])
      `,
      { ...params, lockedIds },
    )
    if (remaining.rows !== result.keysFound || remaining.keys !== result.keysFound) {
      throw new InvariantError(
        `After the batch ${remaining.rows} locked rows of ${remaining.keys} keys remain, expected one row for each of ${result.keysFound} keys: ${JSON.stringify(context)}`,
      )
    }
    ms.verify = Date.now() - started

    started = Date.now()
    const marked = await t.result(
      `
        update ${tables.keys} k set
          status = case when coalesce((d.counts ->> k.id::text)::int, 0) > 0 then 'done' else 'no_duplicates' end,
          deleted = coalesce((d.counts ->> k.id::text)::int, 0),
          attempts = k.attempts + 1,
          error = null,
          "batchId" = $(batchId),
          "doneAt" = now()
        from (select $(perKey)::jsonb as counts) d
        where k.id in ($(keyIds:csv)) and k.status in ($(statuses:csv))
      `,
      { ...params, perKey: JSON.stringify(result.perKey) },
    )
    if (marked.rowCount !== keys.length) {
      throw new InvariantError(
        `Marked ${marked.rowCount} keys, expected ${keys.length} (another run took them?): batch ${batchId}`,
      )
    }
    ms.mark = Date.now() - started

    const walEnd: { bytes: string } = await t.one(
      `select pg_wal_lsn_diff(pg_current_wal_lsn(), $(lsn)::pg_lsn)::bigint::text as bytes`,
      { lsn: walStart.lsn },
    )

    const metrics: IBatchMetrics = {
      ...result,
      lockedRows: locked.n,
      walBytes: Number(walEnd.bytes),
      ms,
    }
    await recordBatch(
      t,
      tables,
      record.runId,
      record.platform,
      keys,
      batchId,
      record.attempts,
      record.startedAt,
      metrics,
    )
    return metrics
  })
}

async function recordBatch(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  db: { none: (query: string, params?: any) => Promise<null> },
  tables: ITables,
  runId: string,
  platform: string,
  keys: IKey[],
  batchId: string,
  attempts: number,
  startedAt: Date,
  metrics: IBatchMetrics,
): Promise<void> {
  const { perKey, ...rest } = metrics
  await db.none(
    `
      insert into ${tables.batches}
        ("batchId", "runId", platform, "keyIdMin", "keyIdMax", keys, "expectedRows", metrics, attempts, "startedAt")
      values ($(batchId), $(runId), $(platform), $(keyIdMin), $(keyIdMax), $(keys), $(expectedRows),
        $(metrics)::jsonb, $(attempts), $(startedAt))
    `,
    {
      batchId,
      runId,
      platform,
      keyIdMin: keys[0].id,
      keyIdMax: keys[keys.length - 1].id,
      keys: keys.length,
      expectedRows: keys.reduce((sum, k) => sum + k.copies, 0),
      metrics: JSON.stringify({ ...rest, perKeyCount: Object.keys(perKey).length }),
      attempts,
      startedAt: startedAt.toISOString(),
    },
  )
}

async function deferKeys(db: Session, tables: ITables, keys: IKey[], error: string) {
  await db.none(
    `
      update ${tables.keys} set status = 'deferred', attempts = attempts + 1, error = $(error)
      where id in ($(keyIds:csv)) and status in ('pending', 'deferred')
    `,
    { keyIds: keys.map((k) => k.id), error: error.slice(0, 1000) },
  )
}

async function nextKeys(
  db: Session,
  tables: ITables,
  platform: string,
  options: ICleanOptions,
  statuses: string[],
  afterId: string,
): Promise<IKey[]> {
  const keys: IKey[] = await db.any(
    `
      select id, copies
      from ${tables.keys}
      where status in ($(statuses:csv))
        and platform = $(platform)
        and id > $(afterId)
        ${options.segmentIds.length > 0 ? `and "segmentId" in ($(segmentIds:csv))` : ''}
      order by id
      limit $(limit)
    `,
    {
      statuses,
      platform,
      afterId,
      segmentIds: options.segmentIds,
      limit: Math.max(1, Math.ceil(options.batchRows / 2)),
    },
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

interface IPlatformPlan {
  platform: string
  keys: number
  copies: number
  duplicates: number
  maxCopies: number
  keysOverBatch: number
}

async function cleanPlan(
  db: Session,
  tables: ITables,
  options: ICleanOptions,
  statuses: string[],
): Promise<IPlatformPlan[]> {
  return db.any(
    `
      select platform, count(*)::int as keys, coalesce(sum(copies), 0)::bigint::int as copies,
        coalesce(sum(copies - 1), 0)::bigint::int as duplicates,
        coalesce(max(copies), 0)::int as "maxCopies",
        count(*) filter (where copies > $(batchRows))::int as "keysOverBatch"
      from ${tables.keys}
      where status in ($(statuses:csv)) and platform in ($(platforms:csv))
        ${options.segmentIds.length > 0 ? `and "segmentId" in ($(segmentIds:csv))` : ''}
      group by platform
    `,
    {
      statuses,
      platforms: options.platforms,
      segmentIds: options.segmentIds,
      batchRows: options.batchRows,
    },
  )
}

async function clean(db: Session, log: Log, options: ICleanOptions, runId: string) {
  const tables = tablesFor(options.tag)
  for (const name of [tables.raw.keys, tables.raw.backup, tables.raw.batches]) {
    if (!(await tableExists(db, name))) {
      throw new UsageError(
        `Table "${name}" doesn't exist: run collect for tag ${options.tag} first`,
      )
    }
  }

  const columns = await activityRelationColumns(db)
  await assertBackupMatches(db, tables, columns)

  const statuses = options.retryDeferred ? ['pending', 'deferred'] : ['pending']
  const plan = await cleanPlan(db, tables, options, statuses)
  const unknownPlatforms = options.platforms.filter((p) => !plan.some((r) => r.platform === p))
  const progress: { name: string; windowEndMs: string }[] = (await tableExists(
    db,
    tables.raw.progress,
  ))
    ? await db.any(
        `select name, (extract(epoch from "windowEnd") * 1000)::bigint::text as "windowEndMs" from ${tables.progress}`,
      )
    : []
  const covered = progress.filter((p) =>
    options.platforms.every((platform) =>
      p.name
        .replace(/^collect:/, '')
        .split(',')
        .includes(platform),
    ),
  )
  const collectedUntil = covered
    .map((p) => Number(p.windowEndMs))
    .reduce((max, ms) => Math.max(max, ms), 0)
  const slots = options.skipSlotGuard ? null : await slotLags(db)
  // collect's default end: the day after the newest row (future-dated rows included)
  const newest = await epochMs(
    db,
    `select (extract(epoch from max("timestamp")) * 1000)::bigint::text as ms from "activityRelations"`,
  )
  const collectTarget = newest === null ? 0 : newest - (newest % DAY_MS) + DAY_MS

  log.info(
    {
      plan,
      platformsWithoutPendingKeys: unknownPlatforms,
      collectProgress: progress.map((p) => ({
        name: p.name,
        windowEnd: new Date(Number(p.windowEndMs)).toISOString(),
      })),
      collectedUntil: collectedUntil ? new Date(collectedUntil).toISOString() : null,
      collectTarget: new Date(collectTarget).toISOString(),
      slotLagMb:
        slots &&
        Object.fromEntries(Object.entries(slots).map(([s, b]) => [s, Math.round(b / 1048576)])),
    },
    'run.plan',
  )

  if (!collectedUntil || collectedUntil < collectTarget) {
    const message = `collect hasn't caught up for ${options.platforms.join(', ')} (collected until ${collectedUntil ? new Date(collectedUntil).toISOString() : 'never'})`
    if (!options.allowPartialCollect && !options.dryRun) {
      throw new UsageError(`${message}; run collect first or pass --allow-partial-collect`)
    }
    log.warn({ collectedUntil }, 'run.partial-collect')
  }

  if (!options.dryRun) {
    const nowReason = stopReason(options, 0)
    if (nowReason === 'nightly-window' && !options.waitNightly) {
      throw new UsageError(
        'Refusing to start during the nightly meetings export (00:30-05:30 UTC); pass --allow-nightly with --until to override',
      )
    }
    const duplicates = plan.reduce((sum, p) => sum + p.duplicates, 0)
    await confirm(
      options,
      `Clean ${duplicates} duplicate rows (${plan.map((p) => `${p.platform}: ${p.duplicates}`).join(', ')}) from activityRelations into ${tables.backup}` +
        `${options.maxRows ? `, at most ${options.maxRows} rows` : ''}${options.until ? `, until ${options.until.toISOString()}` : ''}.`,
    )
  }

  const totals: IRunTotals = {
    batches: 0,
    keys: 0,
    deleted: 0,
    keptUpdated: 0,
    keysNoDuplicates: 0,
    crossYear: 0,
    carried: 0,
    parentConflicts: 0,
    walBytes: 0,
    retries: 0,
    deferredKeys: 0,
    slotWaitMs: 0,
    maxSlotLagBytes: 0,
  }
  const runStarted = Date.now()
  let lastProgress = Date.now()
  let lastProgressBatches = 0
  let lastBatchId: string | null = null
  let lastSlots: Record<string, number> | null = slots
  let exitCode = EXIT_DONE
  let reason = 'done'

  const progressReport = async () => {
    const remaining = await cleanPlan(db, tables, options, statuses)
    const backupSize: { bytes: string } = await db.one(
      `select pg_total_relation_size($(name)::regclass)::text as bytes`,
      { name: `public.${tables.backup}` },
    )
    const elapsedMin = (Date.now() - runStarted) / 60_000
    const rate = elapsedMin > 0 ? totals.deleted / elapsedMin : 0
    const left = remaining.reduce((sum, p) => sum + p.duplicates, 0)
    log.info(
      {
        totals,
        remaining,
        rowsPerMin: Math.round(rate),
        etaMin: rate > 0 ? Math.round(left / rate) : null,
        backupMb: Math.round(Number(backupSize.bytes) / 1048576),
        slotLagMb:
          lastSlots &&
          Object.fromEntries(
            Object.entries(lastSlots).map(([s, b]) => [s, Math.round(b / 1048576)]),
          ),
        lastBatchId,
      },
      'run.progress',
    )
  }

  const processKeys = async (
    platform: string,
    keys: IKey[],
    platformTotals: { batches: number; keys: number; deleted: number },
  ): Promise<void> => {
    const batchId = randomUUID()
    const startedAt = new Date()
    const batchContext = {
      platform,
      batchId,
      keyIdMin: keys[0].id,
      keyIdMax: keys[keys.length - 1].id,
      keys: keys.length,
      expectedRows: keys.reduce((sum, k) => sum + k.copies, 0),
    }
    log.info(batchContext, 'batch.start')

    let metrics: IBatchMetrics | null = null
    let attempts = 0
    for (;;) {
      attempts += 1
      try {
        metrics = await cleanBatch(db, tables, columns, keys, batchId, statuses, options.dryRun, {
          runId,
          platform,
          attempts,
          startedAt,
        })
        break
      } catch (err) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const code = (err as any)?.code
        if (!RETRYABLE_CODES[code]) {
          log.error({ ...pgFields(err), ...batchContext, attempts }, 'batch.failed')
          throw err
        }
        // a batch too slow for the timeout is retried as two smaller ones
        if (code === '57014' && keys.length > 1) {
          const half = Math.ceil(keys.length / 2)
          log.warn(
            { pgCode: code, ...batchContext, attempts, splitInto: [half, keys.length - half] },
            'batch.split',
          )
          totals.retries += 1
          await processKeys(platform, keys.slice(0, half), platformTotals)
          if (!stopReason(options, totals.deleted)) {
            await processKeys(platform, keys.slice(half), platformTotals)
          }
          return
        }
        if (attempts > RETRY_DELAYS_MS.length) {
          log.warn(
            { ...pgFields(err), ...batchContext, attempts, cause: RETRYABLE_CODES[code] },
            'batch.deferred',
          )
          if (!options.dryRun) {
            await deferKeys(db, tables, keys, `${RETRYABLE_CODES[code]}: ${err.message}`)
          }
          totals.deferredKeys += keys.length
          return
        }
        totals.retries += 1
        const delay = RETRY_DELAYS_MS[attempts - 1]
        log.warn(
          { pgCode: code, cause: RETRYABLE_CODES[code], ...batchContext, attempts, delayMs: delay },
          'batch.retry',
        )
        await sleep(delay, () => stopState.requested)
        if (stopState.requested) {
          return
        }
      }
    }

    totals.batches += 1
    totals.keys += keys.length
    totals.deleted += metrics.deleted
    totals.keptUpdated += metrics.keptUpdated
    totals.keysNoDuplicates += metrics.keysFound - metrics.keysWithDuplicates
    totals.crossYear += metrics.crossYear
    totals.carried +=
      metrics.carriedParentId + metrics.carriedConversationId + metrics.carriedSourceParentId
    totals.parentConflicts += metrics.parentConflicts
    totals.walBytes += metrics.walBytes
    platformTotals.batches += 1
    platformTotals.keys += keys.length
    platformTotals.deleted += metrics.deleted
    lastBatchId = batchId

    const totalMs = Date.now() - startedAt.getTime()
    const { perKey, ...metricsToLog } = metrics
    log.info(
      {
        ...batchContext,
        attempts,
        ...metricsToLog,
        keysMissing: keys.length - metrics.keysFound,
        keysNoDuplicates: metrics.keysFound - metrics.keysWithDuplicates,
        drift: batchContext.expectedRows - metrics.lockedRows,
        perKeyCount: Object.keys(perKey).length,
        totalMs,
        rowsPerSec: totalMs > 0 ? Math.round((metrics.deleted * 1000) / totalMs) : null,
        slotLagMb:
          lastSlots &&
          Object.fromEntries(
            Object.entries(lastSlots).map(([s, b]) => [s, Math.round(b / 1048576)]),
          ),
        totals,
        dryRun: options.dryRun,
      },
      'batch.done',
    )
  }

  try {
    platforms: for (const platform of options.platforms) {
      let afterId = '0'
      const platformTotals = { batches: 0, keys: 0, deleted: 0 }
      for (;;) {
        const stop = stopReason(options, totals.deleted)
        if (stop === 'nightly-window' && options.waitNightly) {
          const now = new Date()
          const resume = new Date(now)
          resume.setUTCHours(
            Math.floor(NIGHTLY_EXPORT_END_MINUTES / 60),
            NIGHTLY_EXPORT_END_MINUTES % 60,
            0,
            0,
          )
          log.info({ resumeAt: resume.toISOString() }, 'nightly.wait')
          await sleep(resume.getTime() - now.getTime(), () => {
            const other = stopReason({ ...options, allowNightly: true }, totals.deleted)
            return !!other
          })
          continue
        }
        if (stop) {
          reason = stop
          exitCode = EXIT_STOPPED
          break platforms
        }

        if (!options.dryRun && !options.skipSlotGuard) {
          const waitStarted = Date.now()
          for (;;) {
            lastSlots = await slotLags(db)
            const lag = Math.max(...Object.values(lastSlots))
            totals.maxSlotLagBytes = Math.max(totals.maxSlotLagBytes, lag)
            if (lag <= options.maxSlotLagBytes || stopReason(options, totals.deleted)) {
              break
            }
            log.warn(
              {
                slotLagMb: Object.fromEntries(
                  Object.entries(lastSlots).map(([s, b]) => [s, Math.round(b / 1048576)]),
                ),
                limitMb: options.maxSlotLagBytes / 1048576,
                waitedS: Math.round((Date.now() - waitStarted) / 1000),
              },
              'slot.wait',
            )
            await sleep(SLOT_POLL_MS, () => !!stopReason(options, totals.deleted))
          }
          const waited = Date.now() - waitStarted
          if (waited > 1000) {
            totals.slotWaitMs += waited
            log.info({ waitedS: Math.round(waited / 1000) }, 'slot.resumed')
            continue
          }
        }

        const keys = await nextKeys(db, tables, platform, options, statuses, afterId)
        if (keys.length === 0) {
          break
        }
        afterId = keys[keys.length - 1].id

        await processKeys(platform, keys, platformTotals)

        if (options.dryRun && platformTotals.batches >= options.sampleBatches) {
          break
        }

        if (
          Date.now() - lastProgress >= PROGRESS_EVERY_MS ||
          totals.batches - lastProgressBatches >= PROGRESS_EVERY_BATCHES
        ) {
          await progressReport()
          lastProgress = Date.now()
          lastProgressBatches = totals.batches
        }
      }
      log.info({ platform, ...platformTotals, dryRun: options.dryRun }, 'platform.done')
    }
    if (exitCode === EXIT_DONE && totals.deferredKeys > 0) {
      reason = 'done-with-deferred-keys'
      exitCode = EXIT_DEFERRED
    }
  } catch (err) {
    if (err instanceof InvariantError) {
      reason = 'invariant-violation'
      exitCode = EXIT_ERROR
    } else if (isConnectionError(err)) {
      reason = 'connection-lost'
      exitCode = EXIT_CONNECTION
    } else {
      reason = 'error'
      exitCode = EXIT_ERROR
    }
    throw err
  } finally {
    if (totals.batches > 0 && !options.dryRun) {
      await progressReport().catch((err) => log.warn(pgFields(err), 'run.progress.failed'))
    }
    log.info(
      {
        reason,
        exitCode,
        totals,
        durationMin: Math.round((Date.now() - runStarted) / 60_000),
        lastBatchId,
        dryRun: options.dryRun,
      },
      'run.summary',
    )
  }

  return exitCode
}

// ---------------------------------------------------------------------------------------------
// status
// ---------------------------------------------------------------------------------------------

async function status(db: Session, log: Log, options: ICommonOptions): Promise<number> {
  const tables = tablesFor(options.tag)
  if (!(await tableExists(db, tables.raw.keys))) {
    throw new UsageError(`No cleanup tables for tag ${options.tag}`)
  }

  const keys = await db.any(
    `
      select platform, status, count(*)::int as keys, sum(copies)::bigint::text as copies,
        sum(coalesce(deleted, 0))::bigint::text as deleted, max(copies)::int as "maxCopies"
      from ${tables.keys}
      group by platform, status
      order by platform, status
    `,
  )
  const progress = (await tableExists(db, tables.raw.progress))
    ? await db.any(`select name, "windowEnd"::text, "updatedAt"::text from ${tables.progress}`)
    : []
  const batches = (await tableExists(db, tables.raw.batches))
    ? await db.one(
        `
          select count(*)::int as batches, count(distinct "runId")::int as runs,
            coalesce(sum((metrics ->> 'deleted')::bigint), 0)::text as deleted,
            coalesce(sum((metrics ->> 'crossYear')::bigint), 0)::text as "crossYear",
            coalesce(sum((metrics ->> 'keptUpdated')::bigint), 0)::text as "keptUpdated",
            coalesce(sum((metrics ->> 'parentConflicts')::bigint), 0)::text as "parentConflicts",
            coalesce(sum((metrics ->> 'walBytes')::bigint), 0)::text as "walBytes",
            max("finishedAt")::text as "lastBatchAt"
          from ${tables.batches}
        `,
      )
    : null
  const deferred = await db.any(
    `select id, platform, copies, attempts, error from ${tables.keys} where status = 'deferred' order by id limit 20`,
  )
  const backupSize = (await tableExists(db, tables.raw.backup))
    ? await db.one(`select pg_total_relation_size($(name)::regclass)::text as bytes`, {
        name: `public.${tables.backup}`,
      })
    : null

  const perPlatform: Record<string, { copies: number; finished: number }> = {}
  for (const row of keys) {
    const p = (perPlatform[row.platform] ??= { copies: 0, finished: 0 })
    p.copies += Number(row.copies)
    if (row.status === 'done' || row.status === 'no_duplicates') {
      p.finished += Number(row.copies)
    }
  }
  const percentDone = Object.fromEntries(
    Object.entries(perPlatform).map(([platform, p]) => [
      platform,
      p.copies > 0 ? Math.round((p.finished / p.copies) * 1000) / 10 : null,
    ]),
  )

  log.info(
    {
      tag: options.tag,
      activeRun: await runLockHolders(db, tables),
      percentDone,
      progress,
      batches,
      backupBytes: backupSize?.bytes ?? null,
    },
    'status.summary',
  )
  for (const row of keys) {
    log.info(row, 'status.keys')
  }
  for (const row of deferred) {
    log.info(row, 'status.deferred')
  }
  return EXIT_DONE
}

// ---------------------------------------------------------------------------------------------
// arguments, confirmation, main
// ---------------------------------------------------------------------------------------------

function parseFlags(command: string, args: string[]): Map<string, string | true> {
  const allowed = COMMAND_FLAGS[command]
  const flags = new Map<string, string | true>()
  for (let i = 0; i < args.length; i++) {
    const arg = args[i]
    if (allowed.values.includes(arg)) {
      const value = args[i + 1]
      if (value === undefined || value.startsWith('--')) {
        throw new UsageError(`${arg} needs a value`)
      }
      if (flags.has(arg)) {
        throw new UsageError(`${arg} given twice`)
      }
      flags.set(arg, value)
      i += 1
    } else if (allowed.switches.includes(arg)) {
      if (flags.has(arg)) {
        throw new UsageError(`${arg} given twice`)
      }
      flags.set(arg, true)
    } else {
      throw new UsageError(`Unknown option '${arg}' for ${command}`)
    }
  }
  return flags
}

function value(flags: Map<string, string | true>, flag: string): string | undefined {
  const v = flags.get(flag)
  return typeof v === 'string' ? v : undefined
}

function parseDate(flag: string, input: string | undefined): Date | undefined {
  if (input === undefined) {
    return undefined
  }
  if (!DATE_PATTERN.test(input) || Number.isNaN(Date.parse(`${input}T00:00:00Z`))) {
    throw new UsageError(`${flag} must be a valid YYYY-MM-DD date, got '${input}'`)
  }
  return new Date(`${input}T00:00:00Z`)
}

function parseInt(flag: string, input: string | undefined, min: number, max: number) {
  if (input === undefined) {
    return undefined
  }
  const parsed = Number(input)
  if (!Number.isInteger(parsed) || parsed < min || parsed > max) {
    throw new UsageError(`${flag} must be an integer from ${min} to ${max}, got '${input}'`)
  }
  return parsed
}

function parseList(flag: string, input: string | undefined, pattern: RegExp): string[] | undefined {
  if (input === undefined) {
    return undefined
  }
  const items = input
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean)
  if (
    items.length === 0 ||
    items.some((s) => !pattern.test(s)) ||
    new Set(items).size !== items.length
  ) {
    throw new UsageError(
      `${flag} must be a comma-separated list without duplicates, got '${input}'`,
    )
  }
  return items
}

function parseCommon(flags: Map<string, string | true>): ICommonOptions {
  const tag = value(flags, '--tag')
  if (!tag || !TAG_PATTERN.test(tag)) {
    throw new UsageError(`--tag is required as DD_MM_YYYY, got '${tag ?? ''}'`)
  }
  return {
    tag,
    platforms:
      parseList('--platforms', value(flags, '--platforms'), IDENTIFIER_PATTERN) ??
      DEFAULT_PLATFORMS,
    dryRun: flags.has('--dry-run'),
    yes: flags.has('--yes'),
  }
}

function parseCollect(flags: Map<string, string | true>): ICollectOptions {
  const from = parseDate('--from', value(flags, '--from'))
  const to = parseDate('--to', value(flags, '--to'))
  if (from && to && from >= to) {
    throw new UsageError('--from must be before --to')
  }
  return {
    ...parseCommon(flags),
    from,
    to,
    windowDays: parseInt('--window-days', value(flags, '--window-days'), 1, 366) ?? 7,
  }
}

function parseClean(flags: Map<string, string | true>): ICleanOptions {
  const common = parseCommon(flags)
  const untilArg = value(flags, '--until')
  let until: Date | undefined
  if (untilArg !== undefined) {
    until = new Date(untilArg)
    if (Number.isNaN(until.getTime()) || !/(Z|[+-]\d{2}:\d{2})$/.test(untilArg)) {
      throw new UsageError(`--until must be an ISO timestamp with a time zone, got '${untilArg}'`)
    }
    if (until.getTime() <= Date.now()) {
      throw new UsageError(`--until is in the past: ${untilArg}`)
    }
    const maxUntil = flags.has('--wait-nightly') ? MAX_UNTIL_WAIT_NIGHTLY_MS : MAX_UNTIL_MS
    if (until.getTime() - Date.now() > maxUntil) {
      throw new UsageError(
        `--until must be at most ${maxUntil / 3_600_000} hours ahead (7 days with --wait-nightly)`,
      )
    }
  }

  const options: ICleanOptions = {
    ...common,
    segmentIds: parseList('--segments', value(flags, '--segments'), UUID_PATTERN) ?? [],
    batchRows: parseInt('--batch-rows', value(flags, '--batch-rows'), 2, MAX_BATCH_ROWS) ?? 20000,
    maxRows: parseInt('--max-rows', value(flags, '--max-rows'), 1, 200_000_000),
    until,
    maxSlotLagBytes:
      (parseInt('--max-slot-lag-mb', value(flags, '--max-slot-lag-mb'), 1, 1_000_000) ?? 1024) *
      1048576,
    skipSlotGuard: flags.has('--skip-slot-guard'),
    allowNightly: flags.has('--allow-nightly'),
    waitNightly: flags.has('--wait-nightly'),
    allowPartialCollect: flags.has('--allow-partial-collect'),
    retryDeferred: flags.has('--retry-deferred'),
    sampleBatches: parseInt('--sample-batches', value(flags, '--sample-batches'), 1, 1000) ?? 3,
  }
  if (!options.dryRun && options.maxRows === undefined && !options.until) {
    throw new UsageError('A real clean needs --max-rows or --until, so the run has a known end')
  }
  if (options.allowNightly && !options.until) {
    throw new UsageError('--allow-nightly needs --until')
  }
  if (options.allowNightly && options.waitNightly) {
    throw new UsageError('--allow-nightly and --wait-nightly contradict each other')
  }
  return options
}

async function confirm(options: ICommonOptions, plan: string): Promise<void> {
  if (options.yes) {
    return
  }
  if (!process.stdin.isTTY) {
    throw new UsageError('stdin is not a terminal: pass --yes to run without the confirmation')
  }
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout })
  const answer = await new Promise<string>((resolve) =>
    rl.question(`\n${plan}\nType the tag (${options.tag}) to proceed: `, resolve),
  )
  rl.close()
  if (answer.trim() !== options.tag) {
    throw new UsageError('Aborted by the operator')
  }
}

function printHelp(): void {
  process.stdout.write(
    'Usage: npx tsx src/bin/cleanup-null-channel-duplicates.ts <collect|clean|status> --tag <DD_MM_YYYY> [options]\n' +
      'See the header of services/apps/script_executor_worker/src/bin/cleanup-null-channel-duplicates.ts\n',
  )
}

function installSignalHandlers(log: Log): void {
  const onSignal = (signal: string) => {
    if (stopState.requested) {
      log.warn({ signal }, 'run.abort')
      process.exit(EXIT_SIGNAL)
    }
    stopState.requested = true
    stopState.reason = `signal ${signal}`
    log.warn({ signal }, 'run.stopping')
  }
  process.on('SIGINT', () => onSignal('SIGINT'))
  process.on('SIGTERM', () => onSignal('SIGTERM'))
  process.on('SIGHUP', () => onSignal('SIGHUP'))
  process.stdout.on('error', () => {
    stopState.requested = true
    stopState.reason = 'stdout closed'
  })
}

async function main(): Promise<number> {
  const [command, ...args] = process.argv.slice(2)
  if (!command || command === '--help' || command === '-h' || args.includes('--help')) {
    printHelp()
    return command ? EXIT_DONE : EXIT_USAGE
  }
  if (!COMMAND_FLAGS[command]) {
    printHelp()
    throw new UsageError(`Unknown command '${command}'`)
  }

  const flags = parseFlags(command, args)
  const options =
    command === 'collect'
      ? parseCollect(flags)
      : command === 'clean'
        ? parseClean(flags)
        : parseCommon(flags)

  const runId = randomUUID()
  const log = baseLog.child({ runId, tag: options.tag, command })
  installSignalHandlers(log)

  // application_name in pg_stat_activity, so a DBA can tell this session from the worker's
  process.env.SERVICE = `cleanup-null-channel-${options.tag}`
  const timeoutMs =
    command === 'collect' ? COLLECT_STATEMENT_TIMEOUT_MS + 60_000 : SESSION_QUERY_TIMEOUT_MS
  const db = await getDbConnection(
    { ...WRITE_DB_CONFIG(), queryTimeoutMs: timeoutMs, statementTimeoutMs: timeoutMs },
    1,
    0,
  )
  // one dedicated session for the whole run: the advisory run lock lives as long as it does
  const session = await db.connect()

  const config = WRITE_DB_CONFIG()
  log.info(
    {
      options: { ...options, until: (options as Partial<ICleanOptions>).until?.toISOString() },
      tables: tablesFor(options.tag).raw,
      db: { host: config.host, database: config.database },
      backendPid: session.client.processID,
      host: hostname(),
      pid: process.pid,
    },
    'run.config',
  )

  try {
    if (command !== 'status' && !options.dryRun) {
      await acquireRunLock(session, log, tablesFor(options.tag))
    }
    if (command === 'collect') {
      return await collect(session, log, options as ICollectOptions)
    }
    if (command === 'clean') {
      return await clean(session, log, options as ICleanOptions, runId)
    }
    return await status(session, log, options)
  } finally {
    session.done()
  }
}

process.exitCode = EXIT_ERROR // a bare process.exit() from @crowd/database must not look like success
main()
  .then((code) => process.exit(code))
  .catch((error) => {
    if (error instanceof UsageError) {
      baseLog.error({ message: error.message }, 'run.usage-error')
      process.exit(EXIT_USAGE)
    }
    baseLog.error(pgFields(error), 'run.failed')
    process.exit(isConnectionError(error) ? EXIT_CONNECTION : EXIT_ERROR)
  })
