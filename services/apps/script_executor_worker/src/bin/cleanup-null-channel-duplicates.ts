import { randomUUID } from 'crypto'
import { readFileSync, writeFileSync } from 'fs'
import { hostname } from 'os'
import * as readline from 'readline'

import {
  DbConnection,
  WRITE_DB_CONFIG,
  getDbConnection,
} from '@crowd/data-access-layer/src/database'
import { getServiceChildLogger } from '@crowd/logging'

const baseLog = getServiceChildLogger('cleanup-null-channel-duplicates')

const USAGE = `NULL-channel duplicate cleanup for activityRelations (linuxfoundation/lfx-cdp-insights-ops#852)

Before #850, activities without a channel got a new activityRelations row on every re-send, so
one dedup key ("timestamp", platform, type, "sourceId", "segmentId" with a NULL channel) can have
many copies. This script keeps one copy per key, the one data-sink-worker reads (newest by
"updatedAt", "createdAt", "activityId"), and moves the other copies to a dated backup table, in
short transactions that can be stopped and resumed.

Commands:
  collect  Walk the dedup index in time windows and record every key with more than one
           NULL-channel copy in the work table. Resumes from the last finished window of the
           same platform list.
  clean    Take pending keys platform by platform (--platforms order), whole keys up to
           --batch-rows rows per transaction. Each transaction:
             1. locks the key's NULL-channel copies with NOWAIT: a copy an app transaction holds
                fails the batch (retried); app writes to locked copies wait until it commits,
             2. ranks only the locked copies and keeps rank 1,
             3. carries parentId, conversationId and sourceParentId to the kept copy where it
                has none. When it changes the kept copy, or the kept copy isn't 2 ms past every
                deleted copy's updatedAt (Tinybird stores milliseconds), it sets the kept copy's
                updatedAt to now() (or later, if it already is), so Tinybird's daily merge
                picks it up,
             4. moves the other copies to the backup table with the kept activityId,
             5. checks that every key still has exactly one locked copy and that every deleted
                row is in the backup, and rolls back otherwise,
             6. marks the keys and records the batch's metrics in the batches table.
           Keys whose batch keeps failing on locks, deadlocks or timeouts are marked deferred
           and skipped; --retry-deferred takes them again.
  status   Print progress from the work, batches and progress tables, per platform, for
           every platform of the tag (no --platforms).
  snapshot Read only. Save every row (any channel) of the next --keys pending keys of one
           platform (and --segments), in the order clean takes them, to --out (JSON).
  verify   Read only. Compare the keys of a --snapshot file with what is there now, the backup
           rows written since the snapshot, the keys table and the batches table. Exit 0 and
           verify.pass when clean did exactly what it should, exit 1 and verify.fail with every
           problem otherwise, including keys clean took that the snapshot doesn't cover.
  rollback Put the deleted rows of one --batch back into activityRelations, set its keys back
           to pending and remove the restored rows from the backup table. Follows the table's
           foreign keys: a row whose member or segment is gone stays in the backup, a removed
           organization, conversation or object member is restored as NULL. Kept rows keep
           their carried fields and bumped updatedAt.

Usage:
  npx tsx src/bin/cleanup-null-channel-duplicates.ts <command> --tag <DD_MM_YYYY> [options]
  (or pnpm run cleanup-null-channel-duplicates -- <command> ...)

Common options:
  --tag DD_MM_YYYY   Shared by every run of one cleanup. Tables: "activityRelations_<tag>"
                     (backup), "activityRelationsNullChannelKeys_<tag>" (work),
                     "activityRelationsNullChannelBatches_<tag>" (batch metrics),
                     "activityRelationsNullChannelProgress_<tag>" (collect progress).
  --platforms a,b    Default: meetings,linkedin,tnc,cvent,slack,stackoverflow
  --dry-run          Read only: report what would happen. collect --dry-run scans the same range
                     as a real collect (hours on production): pass --from/--to to sample
  --yes              Skip the confirmation prompt (required when stdin isn't a terminal)

collect options:
  --from YYYY-MM-DD  First day to scan (default: the last finished window, else the oldest row)
  --to YYYY-MM-DD    Scan up to this day, exclusive (default: the day after the newest row)
  --window-days N    Days per window (default 7, halved down to an hour when a window times out)
  --rescan           Scan from the oldest row again although earlier windows are finished: the
                     final sweep after clean, which re-opens keys that got duplicates again. An
                     interrupted rescan continues where it stopped when run with --rescan again

snapshot options:
  --platforms p      Exactly one platform, the one the gated clean run uses
  --segments a,b     Only keys of these segment ids, as in clean
  --keys N           How many pending keys to save, more than the clean run will take (max 20000)
  --retry-deferred   Also take deferred keys, for a clean run with --retry-deferred
  --out FILE         Where to write the snapshot

verify options:
  --snapshot FILE    A file from snapshot, taken before the clean run

rollback options:
  --batch UUID       The batchId from batch.done or the batches table

clean options:
  --segments a,b     Only keys of these segment ids
  --batch-rows N     Rows per transaction, whole keys only (default 20000, max 100000)
  --max-rows N       Delete at most this many rows. A key that doesn't fit what is left isn't
                     started, and a batch that would still go over (copies added since collect)
                     rolls back; the run then stops with exit 2
  --until ISO        Stop at this time, e.g. 2026-11-06T22:00:00Z (at most 48 h ahead, 7 days
                     with --wait-nightly)
                     A real run needs --max-rows or --until.
  --max-slot-lag-mb N  Wait before a batch while sequin_slot or sequin_slot_2 keeps more WAL than
                     this, measured from restart_lsn (default 1024). A missing slot, or one whose
                     wal_status is unreserved or lost, stops the run.
  --skip-slot-guard  Run without the slot check (for environments without Sequin)
  --allow-nightly    Also run 00:30-05:30 UTC (nightly meetings export); needs --until
  --wait-nightly     Pause during 00:30-05:30 UTC and continue afterwards instead of stopping;
                     allows --until up to 7 days ahead
  --allow-partial-collect  Clean although collect hasn't caught up for these platforms
  --retry-deferred   Also take keys deferred by earlier runs
  --sample-batches N Dry run: batches to check per platform (default 3)

Stopping: Ctrl-C, SIGTERM or SIGHUP stops after the current batch; a second one exits at once
(the open transaction rolls back).

Exit codes: 0 nothing left to do, 2 stopped by --max-rows/--until/nightly window/signal, or not
started (another run still holds the run lock after 2 minutes, or inside the nightly window),
3 finished but some keys were deferred (rerun with --retry-deferred), 4 database connection
lost (safe to restart), 1 error incl. a failed safety check (look before restarting), 64 usage.
serverWalBytes in the batch metrics is all WAL the server wrote during the batch, not only ours.

Environment: CROWD_DB_WRITE_HOST, CROWD_DB_PORT, CROWD_DB_USERNAME, CROWD_DB_PASSWORD,
CROWD_DB_DATABASE. Logs are JSON lines with a constant "msg" per event (run.config, run.plan,
collect.window, batch.done, batch.retry, batch.deferred, slot.wait, run.progress, run.summary,
...); pipe through jq.
`

const CARRIED_COLUMNS = ['parentId', 'conversationId', 'sourceParentId']
const DEFAULT_PLATFORMS = ['meetings', 'linkedin', 'tnc', 'cvent', 'slack', 'stackoverflow']
const TAG_PATTERN = /^(0[1-9]|[12][0-9]|3[01])_(0[1-9]|1[0-2])_20[0-9]{2}$/
const IDENTIFIER_PATTERN = /^[A-Za-z0-9_-]+$/
const DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/
const UUID_PATTERN = /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/
const HOUR_MS = 60 * 60 * 1000
const DAY_MS = 24 * HOUR_MS
const MAX_UNTIL_MS = 48 * 60 * 60 * 1000
const MAX_UNTIL_WAIT_NIGHTLY_MS = 7 * DAY_MS
const MAX_BATCH_ROWS = 100_000
// session temp table, so the locked ids never travel as query params (the shared pg-promise
// error hook logs every failed query's params: 20k uuids per retry)
const LOCKED_IDS = 'pg_temp."activityRelationsNullChannelLocked"'

const COLLECT_STATEMENT_TIMEOUT_MS = 5 * 60_000
const CLEAN_STATEMENT_TIMEOUT_MS = 30_000
// the lock step reads every copy's heap page, cold in prod: ~15 s for a 20k-row batch
const CLEAN_LOCK_STATEMENT_TIMEOUT_MS = 60_000
const RUN_LOCK_WAIT_MS = 2 * 60_000
const SESSION_QUERY_TIMEOUT_MS = 10 * 60_000
// rollback's first run indexes the whole backup table (25-35 GB at the end of the cleanup)
const ROLLBACK_QUERY_TIMEOUT_MS = 60 * 60_000
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
    switches: ['--dry-run', '--yes', '--rescan'],
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
  status: { values: ['--tag'], switches: [] },
  snapshot: {
    values: ['--tag', '--platforms', '--segments', '--keys', '--out'],
    switches: ['--retry-deferred'],
  },
  verify: { values: ['--tag', '--snapshot'], switches: [] },
  rollback: { values: ['--tag', '--batch'], switches: ['--yes'] },
}

type Session = Awaited<ReturnType<DbConnection['connect']>>
type Log = ReturnType<typeof getServiceChildLogger>

class UsageError extends Error {}
class InvariantError extends Error {}
class GuardError extends Error {}
class NotStartedError extends Error {}
class MaxRowsError extends Error {}

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

interface ISnapshotOptions extends ICommonOptions {
  segmentIds: string[]
  keys: number
  out: string
  retryDeferred: boolean
}

interface IVerifyOptions extends ICommonOptions {
  snapshot: string
}

interface IRollbackOptions extends ICommonOptions {
  batchId: string
}

interface IKeyRow {
  keyId: string
  updatedUs: string
  createdUs: string
  row: Record<string, unknown>
}

interface ISnapshotFile {
  tag: string
  takenAt: string
  takenAtUs: string
  platform: string
  segmentIds: string[]
  keyIds: string[]
  keyDeleted: Record<string, number>
  rows: IKeyRow[]
}

interface ICollectOptions extends ICommonOptions {
  from?: Date
  to?: Date
  windowDays: number
  rescan: boolean
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
  serverWalBytes: number
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
  serverWalBytes: number
  retries: number
  deferredKeys: number
  slotWaitMs: number
  nightlyWaitMs: number
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

// floor, not ms - ms % DAY_MS: that rounds pre-1970 timestamps (back to year 0026) up
function startOfUtcDay(ms: number): number {
  return Math.floor(ms / DAY_MS) * DAY_MS
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

interface IPgError {
  code?: string
  message?: string
  detail?: string
  hint?: string
  table?: string
  constraint?: string
  where?: string
}

function asPgError(err: unknown): IPgError {
  return typeof err === 'object' && err !== null ? (err as IPgError) : {}
}

// pg error fields the bunyan serializer drops
function pgFields(err: unknown): Record<string, unknown> {
  const e = asPgError(err)
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
  const e = asPgError(err)
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
      throw new NotStartedError(
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
        and l.database = (select oid from pg_database where datname = current_database())
        and l.objid = (hashtext($(name)) & 4294967295)::oid
    `,
    { name: `cleanup-null-channel-duplicates:${tables.tag}` },
  )
}

function collectProgressName(platforms: string[]): string {
  return `collect:${[...platforms].sort().join(',')}`
}

function rescanProgressName(platforms: string[]): string {
  return `rescan:${[...platforms].sort().join(',')}`
}

async function epochMs(db: Session, sql: string, params = {}): Promise<number | null> {
  const row: { ms: string | null } = await db.one(sql, params)
  return row.ms === null ? null : Number(row.ms)
}

async function collect(db: Session, log: Log, options: ICollectOptions): Promise<number> {
  const tables = tablesFor(options.tag)
  const progressName = collectProgressName(options.platforms)
  const rescanName = rescanProgressName(options.platforms)

  let progressTable = await tableExists(db, tables.raw.progress)
  const readProgress = async (name: string) =>
    progressTable
      ? epochMs(
          db,
          `select floor(extract(epoch from max("windowEnd")) * 1000)::bigint::text as ms from ${tables.progress} where name = $(name)`,
          { name },
        )
      : null
  const storedEnd = await readProgress(progressName)
  // an interrupted --rescan continues where it stopped; the row goes away when a rescan ends
  const rescanEnd = options.rescan ? await readProgress(rescanName) : null
  const oldest = await epochMs(
    db,
    `select floor(extract(epoch from min("timestamp")) * 1000)::bigint::text as ms from "activityRelations"`,
  )
  const newest = await epochMs(
    db,
    `select floor(extract(epoch from max("timestamp")) * 1000)::bigint::text as ms from "activityRelations"`,
  )
  if (oldest === null || newest === null) {
    throw new UsageError('activityRelations has no rows with a timestamp')
  }

  const endMs = options.to ? options.to.getTime() : startOfUtcDay(newest) + DAY_MS
  // a rescan cursor at or past the end belongs to a rescan that finished (or died right after
  // its last window): the next rescan starts from the oldest row again
  const rescanResume = rescanEnd !== null && rescanEnd < endMs ? rescanEnd : null
  let startMs: number
  if (options.from) {
    startMs = options.from.getTime()
    if (storedEnd !== null && startMs > storedEnd) {
      log.warn(
        { storedEnd: new Date(storedEnd).toISOString(), from: options.from.toISOString() },
        'collect.gap: windows scanned now are not recorded as progress',
      )
    }
  } else if (options.rescan) {
    startMs = rescanResume ?? startOfUtcDay(oldest)
  } else {
    startMs = storedEnd ?? startOfUtcDay(oldest)
  }
  // progress means "scanned from the oldest row up to here": a partial scan (--to) or one that
  // starts past what was scanned (--from with a gap) must not move it
  const recordsProgress = !options.to && startMs <= (storedEnd ?? startOfUtcDay(oldest))

  log.info(
    {
      platforms: options.platforms,
      progressName,
      recordsProgress,
      storedEnd: storedEnd === null ? null : new Date(storedEnd).toISOString(),
      rescanEnd: rescanEnd === null ? null : new Date(rescanEnd).toISOString(),
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
    log.info(
      'collect.uptodate: nothing scanned; pass --rescan (or --from) to scan finished windows again',
    )
    return EXIT_DONE
  }

  if (!options.dryRun) {
    await confirm(
      options,
      `Collect duplicate keys for ${options.platforms.join(', ')} from ${new Date(startMs).toISOString()} to ${new Date(endMs).toISOString()} into ${tables.keys}.`,
    )
    // after the prompt, so a run left waiting for an answer holds no lock
    await acquireRunLock(db, log, tables)
    // another run may have moved progress while this one waited for the answer: its plan would
    // be stale, so it stops (exit 2) and the next start plans again
    progressTable = await tableExists(db, tables.raw.progress)
    const storedEndNow = await readProgress(progressName)
    const rescanEndNow = options.rescan ? await readProgress(rescanName) : null
    if (storedEndNow !== storedEnd || rescanEndNow !== rescanEnd) {
      throw new NotStartedError(
        'Collect progress changed while this run waited for the confirmation; start it again to plan from the current progress',
      )
    }
    await ensureTables(db, tables)
    // a stale cursor would stay ahead of this rescan (progress only moves forward) and make an
    // interrupted run look finished
    if (options.rescan && !options.to && rescanEnd !== null && rescanResume === null) {
      await db.none(`delete from ${tables.progress} where name = $(name)`, { name: rescanName })
    }
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
  const progressNamesForRun = [
    ...(recordsProgress ? [progressName] : []),
    ...(options.rescan && recordsProgress ? [rescanName] : []),
  ]
  let windowStart = startMs
  let windowMs = options.windowDays * DAY_MS
  while (windowStart < endMs) {
    if (stopState.requested) {
      log.info({ reason: stopState.reason, ...totals }, 'collect.stopped')
      return EXIT_STOPPED
    }

    // years without rows (timestamps go from 0026 to 2120): jump to the next day that has any
    const next = await epochMs(
      db,
      `select floor(extract(epoch from min("timestamp")) * 1000)::bigint::text as ms
       from "activityRelations" where "timestamp" >= $(from)::timestamptz`,
      { from: new Date(windowStart).toISOString() },
    )
    if (next === null || next >= endMs) {
      totals.skippedDays += Math.round((endMs - windowStart) / DAY_MS)
      windowStart = endMs
      // no rows left before the end: that range is scanned too
      if (!options.dryRun) {
        await recordProgress(db, tables, progressNamesForRun, endMs)
      }
      break
    }
    const nextDay = startOfUtcDay(next)
    if (nextDay > windowStart) {
      totals.skippedDays += Math.round((nextDay - windowStart) / DAY_MS)
      windowStart = nextDay
    }

    const windowEnd = Math.min(windowStart + windowMs, endMs)
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
      result = await collectWindow(db, tables, options, progressNamesForRun, windowStart, windowEnd)
    } catch (err) {
      // halves down to an hour, so one dense day can't stop the collect for good
      if (asPgError(err).code === '57014' && windowMs > HOUR_MS) {
        windowMs = Math.max(HOUR_MS, Math.floor(windowMs / 2 / HOUR_MS) * HOUR_MS)
        log.warn(
          {
            ...pgFields(err),
            windowStart: new Date(windowStart).toISOString(),
            windowHours: windowMs / HOUR_MS,
          },
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
        windowHours: windowMs / HOUR_MS,
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

  if (options.rescan && !options.dryRun && !options.to) {
    await db.none(`delete from ${tables.progress} where name = $(name)`, { name: rescanName })
  }
  log.info({ ...totals, dryRun: options.dryRun }, 'collect.summary')
  return EXIT_DONE
}

async function recordProgress(
  db: { none: (query: string, params?: Record<string, unknown>) => Promise<null> },
  tables: ITables,
  names: string[],
  windowEndMs: number,
): Promise<void> {
  if (names.length === 0) {
    return
  }
  await db.none(
    `
      insert into ${tables.progress} (name, "windowEnd")
      select unnest($(names)::text[]), $(windowEnd)::timestamptz
      on conflict (name) do update set
        "windowEnd" = greatest(${tables.progress}."windowEnd", excluded."windowEnd"),
        "updatedAt" = now()
    `,
    { names, windowEnd: new Date(windowEndMs).toISOString() },
  )
}

async function collectWindow(
  db: Session,
  tables: ITables,
  options: ICollectOptions,
  progressNames: string[],
  windowStartMs: number,
  windowEndMs: number,
) {
  const params = {
    windowStart: new Date(windowStartMs).toISOString(),
    windowEnd: new Date(windowEndMs).toISOString(),
    platforms: options.platforms,
    progressNames,
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
            status = 'pending', copies = excluded.copies, error = null,
            "collectedAt" = now()
          where ${tables.keys}.status in ('done', 'no_duplicates')
          returning (xmax = 0) as inserted
        )
        select (select count(*) from upserted where inserted)::int as inserted,
          (select count(*) from upserted where not inserted)::int as reopened, ${summary}
      `,
      params,
    )
    await recordProgress(t, tables, progressNames, windowEndMs)
    return row
  })
}

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

// WAL each slot keeps on disk, from restart_lsn: a long transaction holds restart_lsn back
// while confirmed_flush_lsn moves on, so the confirmed lag alone would miss it
async function slotLags(db: Session): Promise<Record<string, number>> {
  const rows: { slot: string; walStatus: string | null; lag: string | null }[] = await db.any(
    `
      select slot_name as slot, wal_status as "walStatus",
        pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint::text as lag
      from pg_replication_slots
      where slot_name in ($(slots:csv))
    `,
    { slots: SLOTS },
  )
  const missing = SLOTS.filter((s) => !rows.some((r) => r.slot === s && r.lag !== null))
  if (missing.length > 0) {
    throw new GuardError(
      `Replication slot(s) missing or without a restart position: ${missing.join(', ')}. ` +
        "The lag guard can't work; use --skip-slot-guard only where there is no Sequin.",
    )
  }
  // unreserved: past max_wal_size and about to lose WAL; lost: already gone
  const endangered = rows.filter((r) => r.walStatus === 'unreserved' || r.walStatus === 'lost')
  if (endangered.length > 0) {
    throw new GuardError(
      `Replication slot(s) close to losing WAL: ${endangered.map((r) => `${r.slot}=${r.walStatus}`).join(', ')}`,
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
    first_value(ar."updatedAt") over w as "keptUpdatedAt",
    first_value(ar."parentId") over w as "keptParentId",
    first_value(ar."conversationId") over w as "keptConversationId",
    first_value(ar."sourceParentId") over w as "keptSourceParentId",
    row_number() over w as rank
  from "activityRelations" ar
  join k on ${KEY_JOIN}
  ${onlyLocked ? `where ar."activityId" in (select "activityId" from ${LOCKED_IDS})` : ''}
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

// the kept row's columns come from window functions in ranked: joining ranked back to itself
// made the planner (which estimates the CTE at 1-2 rows) scan the whole batch per kept row
const PLAN = `
  carried as (
    select "keptActivityId", "keptUpdatedAt", "keptParentId", "keptConversationId", "keptSourceParentId",
      max("updatedAt") as "maxDroppedUpdatedAt",
      (array_agg("parentId" order by rank) filter (where "parentId" is not null))[1] as "parentId",
      count(distinct "parentId") as "parentIds",
      (array_agg("conversationId" order by rank) filter (where "conversationId" is not null))[1] as "conversationId",
      (array_agg("sourceParentId" order by rank) filter (where "sourceParentId" is not null))[1] as "sourceParentId"
    from dropped
    group by "keptActivityId", "keptUpdatedAt", "keptParentId", "keptConversationId", "keptSourceParentId"
  ),
  plan as (
    select c."keptActivityId" as "activityId",
      greatest(
        c."keptUpdatedAt",
        -- 2 ms: a deleted copy's updatedAt rounded up instead of truncated still can't tie
        date_trunc('milliseconds', c."maxDroppedUpdatedAt") + interval '2 milliseconds'
      ) as "newUpdatedAt",
      c."keptUpdatedAt" as "oldUpdatedAt",
      c."keptParentId" is null and c."parentId" is not null as "carryParentId",
      c."keptConversationId" is null and c."conversationId" is not null as "carryConversationId",
      c."keptSourceParentId" is null and c."sourceParentId" is not null as "carrySourceParentId",
      c."parentIds" > 1
        or (c."keptParentId" is not null and c."parentId" is not null and c."keptParentId" <> c."parentId")
        as "parentConflict",
      c."parentId", c."conversationId", c."sourceParentId"
    from carried c
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
  maxDeletes: number | undefined,
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
        serverWalBytes: 0,
        ms,
      }
    }

    const walStart: { lsn: string } = await t.one(`select pg_current_wal_lsn()::text as lsn`)

    await t.none(`set local statement_timeout = ${CLEAN_LOCK_STATEMENT_TIMEOUT_MS}`)
    let started = Date.now()
    // NOWAIT: a copy an app transaction holds fails the batch (retried) instead of waiting; app
    // writes to copies locked here wait for this transaction, bounded by its statement timeouts
    const lockedResult = await t.result(
      `
        with k as (${KEYS_CTE(tables)})
        insert into ${LOCKED_IDS} ("activityId")
        select l."activityId" from (
          select ar."activityId" from "activityRelations" ar join k on ${KEY_JOIN}
          order by ar."activityId"
          for update of ar nowait
        ) l
      `,
      params,
    )
    const locked = { n: lockedResult.rowCount }
    ms.lock = Date.now() - started
    await t.none(`set local statement_timeout = ${CLEAN_STATEMENT_TIMEOUT_MS}`)

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
            -- now(): Tinybird's daily merge only takes rows whose updatedAt day is at or after its
            -- last run, so a kept row left with an old updatedAt would never reach the served layer
            "updatedAt" = greatest(p."newUpdatedAt", now())
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
      params,
    )
    ms.clean = Date.now() - started

    started = Date.now()
    const context = { batchId, lockedRows: locked.n, ...result }
    if (maxDeletes !== undefined && result.deleted > maxDeletes) {
      throw new MaxRowsError(
        `Batch ${batchId} would delete ${result.deleted} rows, ${maxDeletes} left under --max-rows`,
      )
    }
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
    const survivorsPerKey: { rows: number; keys: number } = await t.one(
      `
        with k as (${KEYS_CTE(tables)})
        select count(*)::int as rows, count(distinct k."keyId")::int as keys
        from "activityRelations" ar join k on ${KEY_JOIN}
        where ar."activityId" in (select "activityId" from ${LOCKED_IDS})
      `,
      params,
    )
    if (survivorsPerKey.rows !== result.keysFound || survivorsPerKey.keys !== result.keysFound) {
      throw new InvariantError(
        `After the batch ${survivorsPerKey.rows} locked rows of ${survivorsPerKey.keys} keys remain, expected one row for each of ${result.keysFound} keys: ${JSON.stringify(context)}`,
      )
    }
    ms.verify = Date.now() - started

    started = Date.now()
    const marked = await t.result(
      `
        update ${tables.keys} k set
          status = case when coalesce((d.counts ->> k.id::text)::int, 0) > 0 then 'done' else 'no_duplicates' end,
          deleted = coalesce(k.deleted, 0) + coalesce((d.counts ->> k.id::text)::int, 0),
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
      serverWalBytes: Number(walEnd.bytes),
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
  db: { none: (query: string, params?: Record<string, unknown>) => Promise<null> },
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
  maxDeletes: number | undefined,
): Promise<{ batch: IKey[]; limited: boolean }> {
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

  // whole keys only; a key bigger than --batch-rows still goes alone, one past --max-rows doesn't
  const batch: IKey[] = []
  let rows = 0
  let deletes = 0
  for (const key of keys) {
    if (maxDeletes !== undefined && deletes + key.copies - 1 > maxDeletes) {
      return { batch, limited: true }
    }
    if (batch.length > 0 && rows + key.copies > options.batchRows) {
      break
    }
    batch.push(key)
    rows += key.copies
    deletes += key.copies - 1
  }
  return { batch, limited: false }
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
  if (!options.dryRun) {
    // emptied by every commit and rollback, so it only ever holds the open batch's locked ids
    await db.none(
      `create temp table if not exists ${LOCKED_IDS} ("activityId" uuid primary key) on commit delete rows`,
    )
  }

  const statuses = options.retryDeferred ? ['pending', 'deferred'] : ['pending']
  const plan = await cleanPlan(db, tables, options, statuses)
  const unknownPlatforms = options.platforms.filter((p) => !plan.some((r) => r.platform === p))
  const progress: { name: string; windowEndMs: string }[] = (await tableExists(
    db,
    tables.raw.progress,
  ))
    ? await db.any(
        `select name, floor(extract(epoch from "windowEnd") * 1000)::bigint::text as "windowEndMs" from ${tables.progress}`,
      )
    : []
  // per platform: the furthest end of any collect run that included it, so collects done in
  // separate platform runs count; the run as a whole is covered up to the least covered one
  const collectedUntilByPlatform = Object.fromEntries(
    options.platforms.map((platform) => [
      platform,
      progress
        .filter(
          (p) =>
            p.name.startsWith('collect:') &&
            p.name
              .replace(/^collect:/, '')
              .split(',')
              .includes(platform),
        )
        .reduce((max, p) => Math.max(max, Number(p.windowEndMs)), 0),
    ]),
  )
  const collectedUntil = Math.min(...Object.values(collectedUntilByPlatform))
  const slots = options.skipSlotGuard ? null : await slotLags(db)
  const newest = await epochMs(
    db,
    `select floor(extract(epoch from max("timestamp")) * 1000)::bigint::text as ms from "activityRelations"`,
  )
  const collectTarget = newest === null ? 0 : startOfUtcDay(newest) + DAY_MS

  log.info(
    {
      plan,
      platformsWithoutPendingKeys: unknownPlatforms,
      collectProgress: progress.map((p) => ({
        name: p.name,
        windowEnd: new Date(Number(p.windowEndMs)).toISOString(),
      })),
      collectedUntil: collectedUntil ? new Date(collectedUntil).toISOString() : null,
      collectedUntilByPlatform: Object.fromEntries(
        Object.entries(collectedUntilByPlatform).map(([p, ms]) => [
          p,
          ms ? new Date(ms).toISOString() : null,
        ]),
      ),
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
      throw new NotStartedError(
        'Not starting during the nightly meetings export (00:30-05:30 UTC): start again after 05:30, or pass --wait-nightly, or --allow-nightly with --until',
      )
    }
    const duplicates = plan.reduce((sum, p) => sum + p.duplicates, 0)
    await confirm(
      options,
      `Clean ${duplicates} duplicate rows (${plan.map((p) => `${p.platform}: ${p.duplicates}`).join(', ')}) from activityRelations into ${tables.backup}` +
        `${options.maxRows ? `, at most ${options.maxRows} rows` : ''}${options.until ? `, until ${options.until.toISOString()}` : ''}.`,
    )
    await acquireRunLock(db, log, tables)
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
    serverWalBytes: 0,
    retries: 0,
    deferredKeys: 0,
    slotWaitMs: 0,
    nightlyWaitMs: 0,
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
    // waits for slot lag and the nightly window don't count, so the ETA is for cleaning time
    const activeMin = (Date.now() - runStarted - totals.slotWaitMs - totals.nightlyWaitMs) / 60_000
    const rate = activeMin > 0 ? totals.deleted / activeMin : 0
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

  const waitForSlots = async (): Promise<void> => {
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
    }
  }

  let limitHit = false

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
      // every attempt, retry and split half passes the same guards as a new batch; keys left
      // here stay pending and the main loop stops or waits
      if (limitHit) {
        return
      }
      let stop = stopReason(options, totals.deleted)
      if (!stop && !options.dryRun && !options.skipSlotGuard) {
        await waitForSlots()
        stop = stopReason(options, totals.deleted)
      }
      if (stop) {
        log.info({ ...batchContext, attempts, reason: stop }, 'batch.not-started')
        return
      }
      attempts += 1
      try {
        metrics = await cleanBatch(
          db,
          tables,
          columns,
          keys,
          batchId,
          statuses,
          options.dryRun,
          options.maxRows === undefined ? undefined : options.maxRows - totals.deleted,
          { runId, platform, attempts, startedAt },
        )
        break
      } catch (err) {
        if (err instanceof MaxRowsError) {
          log.info({ ...batchContext, attempts, message: err.message }, 'batch.over-max-rows')
          limitHit = true
          return
        }
        const code = asPgError(err).code ?? ''
        if (!RETRYABLE_CODES[code]) {
          log.error({ ...pgFields(err), ...batchContext, attempts }, 'batch.failed')
          throw err
        }
        // a dry run only samples: a timeout is reported, not split or retried into more queries
        if (code === '57014' && options.dryRun) {
          log.warn({ pgCode: code, ...batchContext, attempts }, 'batch.dry-run-timeout')
          return
        }
        if (code === '57014' && keys.length > 1) {
          const half = Math.ceil(keys.length / 2)
          log.warn(
            { pgCode: code, ...batchContext, attempts, splitInto: [half, keys.length - half] },
            'batch.split',
          )
          totals.retries += 1
          await processKeys(platform, keys.slice(0, half), platformTotals)
          await processKeys(platform, keys.slice(half), platformTotals)
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
        await sleep(delay, () => !!stopReason(options, totals.deleted))
      }
    }

    totals.batches += 1
    totals.keys += keys.length
    totals.deleted += metrics.deleted
    totals.keptUpdated += metrics.keptUpdated
    totals.keysNoDuplicates += keys.length - metrics.keysWithDuplicates
    totals.crossYear += metrics.crossYear
    totals.carried +=
      metrics.carriedParentId + metrics.carriedConversationId + metrics.carriedSourceParentId
    totals.parentConflicts += metrics.parentConflicts
    totals.serverWalBytes += metrics.serverWalBytes
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
        keysNoDuplicates: keys.length - metrics.keysWithDuplicates,
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
      let sampled = 0
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
          const waitStarted = Date.now()
          await sleep(resume.getTime() - now.getTime(), () => {
            const other = stopReason({ ...options, allowNightly: true }, totals.deleted)
            return !!other
          })
          totals.nightlyWaitMs += Date.now() - waitStarted
          // a split batch stopped by the window left keys behind the cursor
          afterId = '0'
          continue
        }
        if (stop) {
          reason = stop
          exitCode = EXIT_STOPPED
          break platforms
        }

        const { batch: keys, limited } = await nextKeys(
          db,
          tables,
          platform,
          options,
          statuses,
          afterId,
          options.maxRows === undefined ? undefined : options.maxRows - totals.deleted,
        )
        if (keys.length === 0) {
          if (limited) {
            log.info({ platform, deleted: totals.deleted }, 'run.max-rows-reached')
            reason = 'max-rows'
            exitCode = EXIT_STOPPED
            break platforms
          }
          break
        }
        afterId = keys[keys.length - 1].id

        await processKeys(platform, keys, platformTotals)
        sampled += 1
        if (limitHit) {
          reason = 'max-rows'
          exitCode = EXIT_STOPPED
          break platforms
        }

        // counts batches tried, not only those that succeeded, so failing dry runs stay bounded
        if (options.dryRun && sampled >= options.sampleBatches) {
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
    if (exitCode === EXIT_DONE && !options.dryRun) {
      const deferred = await cleanPlan(db, tables, options, ['deferred'])
      const deferredKeysLeft = deferred.reduce((sum, p) => sum + p.keys, 0)
      if (deferredKeysLeft > 0) {
        log.warn({ deferredKeysLeft, deferred }, 'run.deferred-keys-left')
        reason = 'done-with-deferred-keys'
        exitCode = EXIT_DEFERRED
      }
    }
  } catch (err) {
    if (err instanceof InvariantError) {
      reason = 'invariant-violation'
      exitCode = EXIT_ERROR
    } else if (err instanceof GuardError) {
      reason = 'guard-failed'
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
            coalesce(sum((metrics ->> 'serverWalBytes')::bigint), 0)::text as "serverWalBytes",
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

type Queryable = Pick<Session, 'any' | 'one' | 'none' | 'result'>

const KEY_COLUMNS_JOIN = (alias: string) => `
  ${alias}."timestamp" = k."timestamp"
  and ${alias}.platform = k.platform
  and ${alias}.type = k.type
  and ${alias}."sourceId" = k."sourceId"
  and ${alias}."segmentId" = k."segmentId"
`

// to_jsonb keeps the microseconds a JS Date would drop; the *Us columns are for ranking
async function keyRows(t: Queryable, tables: ITables, keyIds: string[]): Promise<IKeyRow[]> {
  if (keyIds.length === 0) {
    return []
  }
  return t.any(
    `
      select k.id::text as "keyId",
        (extract(epoch from ar."updatedAt") * 1000000)::bigint::text as "updatedUs",
        (extract(epoch from ar."createdAt") * 1000000)::bigint::text as "createdUs",
        to_jsonb(ar) as row
      from ${tables.keys} k
      join "activityRelations" ar on ${KEY_COLUMNS_JOIN('ar')}
      where k.id in ($(keyIds:csv))
      order by k.id, ar."activityId"
    `,
    { keyIds },
  )
}

async function snapshot(db: Session, log: Log, options: ISnapshotOptions): Promise<number> {
  const tables = tablesFor(options.tag)
  if (!(await tableExists(db, tables.raw.keys))) {
    throw new UsageError(`No cleanup tables for tag ${options.tag}`)
  }
  if (options.platforms.length !== 1) {
    throw new UsageError(
      'snapshot takes exactly one platform: --platforms <the one clean will run>',
    )
  }
  const platform = options.platforms[0]
  const statuses = options.retryDeferred ? ['pending', 'deferred'] : ['pending']

  const file: ISnapshotFile = await db.tx(async (t) => {
    await t.none(`set transaction isolation level repeatable read read only`)
    await t.none(`set local timezone to 'UTC'`)
    const takenAt: { at: string; us: string } = await t.one(
      `select now()::text as at, (extract(epoch from now()) * 1000000)::bigint::text as us`,
    )
    // the alias differs from the column, so order by is numeric like clean's, not by the text
    const keys: { keyId: string; deleted: number | null }[] = await t.any(
      `
        select id::text as "keyId", deleted from ${tables.keys}
        where status in ($(statuses:csv)) and platform = $(platform)
          ${options.segmentIds.length > 0 ? `and "segmentId" in ($(segmentIds:csv))` : ''}
        order by id
        limit $(limit)
      `,
      { statuses, platform, segmentIds: options.segmentIds, limit: options.keys },
    )
    const keyIds = keys.map((k) => k.keyId)
    return {
      tag: options.tag,
      takenAt: takenAt.at,
      takenAtUs: takenAt.us,
      platform,
      segmentIds: options.segmentIds,
      keyIds,
      keyDeleted: Object.fromEntries(keys.map((k) => [k.keyId, k.deleted ?? 0])),
      rows: await keyRows(t, tables, keyIds),
    }
  })

  writeFileSync(options.out, JSON.stringify(file))
  log.info(
    {
      out: options.out,
      takenAt: file.takenAt,
      keys: file.keyIds.length,
      rows: file.rows.length,
      nullChannelRows: file.rows.filter((r) => r.row.channel === null).length,
      firstKeyId: file.keyIds[0] ?? null,
      lastKeyId: file.keyIds[file.keyIds.length - 1] ?? null,
    },
    'snapshot.saved',
  )
  return EXIT_DONE
}

function stableJson(value: Record<string, unknown>): string {
  return JSON.stringify(
    Object.keys(value)
      .sort()
      .map((k) => [k, value[k]]),
  )
}

function changedColumns(before: Record<string, unknown>, after: Record<string, unknown>): string[] {
  return [...new Set([...Object.keys(before), ...Object.keys(after)])].filter(
    (c) => JSON.stringify(before[c] ?? null) !== JSON.stringify(after[c] ?? null),
  )
}

// newest first, the order clean ranks copies in
function newer(a: IKeyRow, b: IKeyRow): boolean {
  const au = BigInt(a.updatedUs)
  const bu = BigInt(b.updatedUs)
  if (au !== bu) return au > bu
  const ac = BigInt(a.createdUs)
  const bc = BigInt(b.createdUs)
  if (ac !== bc) return ac > bc
  return String(a.row.activityId) > String(b.row.activityId)
}

// date_trunc('milliseconds') floors, also before 1970
function floorToMs(us: bigint): bigint {
  const ms = BigInt(1000)
  const r = us % ms
  return r < BigInt(0) ? us - r - ms : us - r
}

function auditKeys(
  snap: ISnapshotFile,
  keyStatus: Map<string, { status: string; deleted: number | null; error: string | null }>,
  now: IKeyRow[],
  backup: { keyId: string; keptActivityId: string; row: Record<string, unknown> }[],
): { problems: string[]; notes: string[]; stats: Record<string, number> } {
  const problems: string[] = []
  const notes: string[] = []
  const stats = {
    keysProcessed: 0,
    keysUntouched: 0,
    keysDeferred: 0,
    rowsDeleted: 0,
    keptUpdated: 0,
  }
  const group = <T extends { keyId: string }>(rows: T[]) => {
    const m = new Map<string, T[]>()
    for (const r of rows) m.set(r.keyId, [...(m.get(r.keyId) ?? []), r])
    return m
  }
  const beforeByKey = group(snap.rows)
  const nowByKey = group(now)
  const backupByKey = group(backup)
  const presentIds = new Set(now.map((r) => String(r.row.activityId)))
  const unchanged = (
    keyId: string,
    what: string,
    id: string,
    r: IKeyRow,
    a: IKeyRow | undefined,
  ) => {
    if (!a) {
      problems.push(`key ${keyId}: ${what} ${id} disappeared`)
    } else if (stableJson(a.row) !== stableJson(r.row)) {
      problems.push(
        `key ${keyId}: ${what} ${id} changed in ${changedColumns(r.row, a.row).join(', ')}`,
      )
    }
  }

  for (const keyId of snap.keyIds) {
    const before = new Map((beforeByKey.get(keyId) ?? []).map((r) => [String(r.row.activityId), r]))
    const after = new Map((nowByKey.get(keyId) ?? []).map((r) => [String(r.row.activityId), r]))
    const backedUp = backupByKey.get(keyId) ?? []
    const status = keyStatus.get(keyId)
    for (const id of after.keys()) {
      if (!before.has(id)) problems.push(`key ${keyId}: row ${id} appeared after the snapshot`)
    }
    if (!status) {
      problems.push(`key ${keyId}: missing from the keys table`)
      continue
    }

    if (status.status === 'pending' || status.status === 'deferred') {
      if (status.status === 'deferred') {
        stats.keysDeferred++
        notes.push(`key ${keyId}: deferred by clean (${status.error ?? 'no error'}), not checked`)
      } else {
        stats.keysUntouched++
      }
      for (const [id, r] of before) unchanged(keyId, 'untouched row', id, r, after.get(id))
      if (backedUp.length > 0) {
        problems.push(`key ${keyId} (${status.status}): ${backedUp.length} backup row(s)`)
      }
      continue
    }

    stats.keysProcessed++
    const nullBefore = [...before.values()].filter((r) => r.row.channel === null)
    const ranked = [...nullBefore].sort((a, b) => (newer(a, b) ? -1 : newer(b, a) ? 1 : 0))
    const kept = ranked[0] ?? null
    const keptId = kept ? String(kept.row.activityId) : null
    const deleted = ranked.slice(1)

    for (const [id, r] of before) {
      if (r.row.channel !== null)
        unchanged(keyId, `row with channel "${r.row.channel}"`, id, r, after.get(id))
    }

    const nullLeft = [...after.values()]
      .filter((r) => r.row.channel === null && before.has(String(r.row.activityId)))
      .map((r) => String(r.row.activityId))
    if (nullLeft.length !== (keptId ? 1 : 0) || (keptId && nullLeft[0] !== keptId)) {
      problems.push(
        `key ${keyId}: NULL-channel rows left ${JSON.stringify(nullLeft)}, expected only ${keptId}`,
      )
    }

    const backupById = new Map(backedUp.map((b) => [String(b.row.activityId), b]))
    const deletedIds = deleted.map((r) => String(r.row.activityId)).sort()
    if (JSON.stringify([...backupById.keys()].sort()) !== JSON.stringify(deletedIds)) {
      problems.push(
        `key ${keyId}: backup has ${JSON.stringify([...backupById.keys()].sort())}, deleted copies are ${JSON.stringify(deletedIds)}`,
      )
    }
    for (const r of deleted) {
      const id = String(r.row.activityId)
      if (presentIds.has(id)) problems.push(`key ${keyId}: deleted row ${id} still exists`)
      const b = backupById.get(id)
      if (!b) continue
      if (stableJson(b.row) !== stableJson(r.row)) {
        problems.push(
          `key ${keyId}: backup row ${id} differs from the snapshot in ${changedColumns(r.row, b.row).join(', ')}`,
        )
      }
      if (b.keptActivityId !== keptId) {
        problems.push(
          `key ${keyId}: backup row ${id} keptActivityId ${b.keptActivityId}, expected ${keptId}`,
        )
      }
    }
    stats.rowsDeleted += deleted.length
    const deletedNow = (status.deleted ?? 0) - (snap.keyDeleted[keyId] ?? 0)
    if (deletedNow !== backedUp.length) {
      problems.push(
        `key ${keyId}: keys.deleted grew by ${deletedNow} but ${backedUp.length} row(s) were backed up`,
      )
    }

    if (!kept || !keptId) continue
    const keptNow = after.get(keptId)
    if (!keptNow) {
      problems.push(`key ${keyId}: kept row ${keptId} is gone`)
      continue
    }
    if (deleted.length > 0) {
      // the 2 ms clean guarantees past every deleted copy, at the precision Tinybird keeps
      const maxDeletedUs = deleted.reduce(
        (m, r) => (BigInt(r.updatedUs) > m ? BigInt(r.updatedUs) : m),
        BigInt(deleted[0].updatedUs),
      )
      if (BigInt(keptNow.updatedUs) < floorToMs(maxDeletedUs) + BigInt(2000)) {
        problems.push(
          `key ${keyId}: kept updatedAt ${keptNow.row.updatedAt} isn't 2 ms past every deleted copy`,
        )
      }
    }
    const columns = changedColumns(kept.row, keptNow.row)
    // a kept row clean changed got updatedAt = now(), after the snapshot, so Tinybird merges it
    if (columns.length > 0 && BigInt(keptNow.updatedUs) < BigInt(snap.takenAtUs)) {
      problems.push(
        `key ${keyId}: kept row changed in ${columns.join(', ')} but its updatedAt ${keptNow.row.updatedAt} is older than the snapshot`,
      )
    }
    for (const col of columns) {
      const was = kept.row[col] ?? null
      const is = keptNow.row[col] ?? null
      if (col === 'updatedAt') {
        if (BigInt(keptNow.updatedUs) < BigInt(kept.updatedUs)) {
          problems.push(`key ${keyId}: kept updatedAt went back from ${was} to ${is}`)
        }
      } else if (!CARRIED_COLUMNS.includes(col)) {
        problems.push(
          `key ${keyId}: kept row column ${col} changed from ${JSON.stringify(was)} to ${JSON.stringify(is)}`,
        )
      }
    }
    // a kept row without a value takes the newest deleted copy's, so none is lost
    for (const col of CARRIED_COLUMNS) {
      const was = kept.row[col] ?? null
      const is = keptNow.row[col] ?? null
      const expected = was ?? deleted.find((r) => (r.row[col] ?? null) !== null)?.row[col] ?? null
      if (JSON.stringify(is) !== JSON.stringify(expected)) {
        problems.push(
          `key ${keyId}: kept ${col} is ${JSON.stringify(is)} (was ${JSON.stringify(was)}), expected ${JSON.stringify(expected)}`,
        )
      }
    }
    if (columns.length > 0) stats.keptUpdated++
  }
  return { problems, notes, stats }
}

function readSnapshot(path: string): ISnapshotFile {
  const snap = JSON.parse(readFileSync(path, 'utf8')) as Partial<ISnapshotFile>
  if (
    typeof snap.tag !== 'string' ||
    typeof snap.takenAt !== 'string' ||
    typeof snap.takenAtUs !== 'string' ||
    !Array.isArray(snap.keyIds) ||
    !Array.isArray(snap.rows) ||
    typeof snap.keyDeleted !== 'object'
  ) {
    throw new UsageError(`${path} isn't a snapshot file from this script's snapshot command`)
  }
  return snap as ISnapshotFile
}

async function verify(db: Session, log: Log, options: IVerifyOptions): Promise<number> {
  const snap = readSnapshot(options.snapshot)
  if (snap.tag !== options.tag) {
    throw new UsageError(`The snapshot is for tag ${snap.tag}, not ${options.tag}`)
  }
  const tables = tablesFor(options.tag)
  if (snap.keyIds.length === 0) {
    throw new UsageError('The snapshot has no keys')
  }

  const state = await db.tx(async (t) => {
    await t.none(`set transaction isolation level repeatable read read only`)
    await t.none(`set local timezone to 'UTC'`)
    const keys: {
      keyId: string
      status: string
      deleted: number | null
      error: string | null
      batchId: string | null
    }[] = await t.any(
      `select id::text as "keyId", status, deleted, error, "batchId" from ${tables.keys} where id in ($(keyIds:csv))`,
      { keyIds: snap.keyIds },
    )
    // only what clean backed up after the snapshot: re-opened keys have older backup rows
    const backup: {
      keyId: string
      batchId: string
      keptActivityId: string
      row: Record<string, unknown>
    }[] = await t.any(
      `
        select k.id::text as "keyId", b."batchId", b."keptActivityId",
          to_jsonb(b) - array['keptActivityId', 'batchId', 'deletedAt'] as row
        from ${tables.keys} k
        join ${tables.backup} b on ${KEY_COLUMNS_JOIN('b')}
        where k.id in ($(keyIds:csv)) and b."deletedAt" >= $(takenAt)::timestamptz
      `,
      { keyIds: snap.keyIds, takenAt: snap.takenAt },
    )
    // a key whose copies were gone by then is done without backup rows, so its batch counts too
    const batchIds = [
      ...new Set([
        ...backup.map((b) => b.batchId),
        ...keys.flatMap((k) => (k.batchId ? [k.batchId] : [])),
      ]),
    ]
    // every batch of the runs that touched these keys, so keys taken outside the snapshot show
    const batches: { batchId: string; deleted: number; backedUp: number; outside: number }[] =
      batchIds.length === 0
        ? []
        : await t.any(
            `
              with run_batches as (
                select "batchId", (metrics ->> 'deleted')::int as deleted
                from ${tables.batches}
                where "runId" in (
                  select "runId" from ${tables.batches}
                  where "batchId" in ($(batchIds:csv)) and "startedAt" >= $(takenAt)::timestamptz
                )
              ),
              backed_up as (
                select "batchId", count(*)::int as n from ${tables.backup}
                where "batchId" in (select "batchId" from run_batches) group by 1
              ),
              outside as (
                select "batchId", count(*)::int as n from ${tables.keys}
                where "batchId" in (select "batchId" from run_batches) and id not in ($(keyIds:csv))
                group by 1
              )
              select rb."batchId", rb.deleted, coalesce(bu.n, 0) as "backedUp",
                coalesce(o.n, 0) as outside
              from run_batches rb
              left join backed_up bu on bu."batchId" = rb."batchId"
              left join outside o on o."batchId" = rb."batchId"
            `,
            { batchIds, keyIds: snap.keyIds, takenAt: snap.takenAt },
          )
    return { keys, backup, batches, now: await keyRows(t, tables, snap.keyIds) }
  })

  const { problems, notes, stats } = auditKeys(
    snap,
    new Map(
      state.keys.map((k) => [k.keyId, { status: k.status, deleted: k.deleted, error: k.error }]),
    ),
    state.now,
    state.backup,
  )
  for (const b of state.batches) {
    if (b.deleted !== b.backedUp) {
      problems.push(
        `batch ${b.batchId}: metrics.deleted ${b.deleted} but ${b.backedUp} backup rows`,
      )
    }
    if (b.outside > 0) {
      problems.push(
        `batch ${b.batchId}: took ${b.outside} key(s) outside the snapshot, which verify didn't check (snapshot more --keys)`,
      )
    }
  }

  const context = {
    snapshot: options.snapshot,
    takenAt: snap.takenAt,
    keys: snap.keyIds.length,
    batches: state.batches.length,
    ...stats,
    notes: notes.slice(0, 50),
  }
  if (problems.length > 0) {
    log.error(
      { ...context, problemCount: problems.length, problems: problems.slice(0, 200) },
      'verify.fail',
    )
    return EXIT_ERROR
  }
  log.info(context, 'verify.pass')
  return EXIT_DONE
}

async function rollback(db: Session, log: Log, options: IRollbackOptions): Promise<number> {
  const tables = tablesFor(options.tag)
  for (const name of [tables.raw.keys, tables.raw.backup, tables.raw.batches]) {
    if (!(await tableExists(db, name))) {
      throw new UsageError(`Table "${name}" doesn't exist`)
    }
  }
  const columns = await activityRelationColumns(db)
  await assertBackupMatches(db, tables, columns)
  // the batch's own record: counting its backup rows before the index exists scans the whole table
  const batch: { deleted: number } | null = await db.oneOrNone(
    `select (metrics ->> 'deleted')::int as deleted from ${tables.batches} where "batchId" = $(batchId)`,
    { batchId: options.batchId },
  )
  if (!batch) {
    throw new UsageError(`Batch ${options.batchId} isn't in ${tables.batches}`)
  }
  await confirm(
    options,
    `Put the rows of batch ${options.batchId} (${batch.deleted} deleted) back into activityRelations and set its keys back to pending.`,
  )

  // a clean run of the same tag must not take these keys while they are restored
  await acquireRunLock(db, log, tables)
  // without it every rollback statement scans the whole backup table
  await db.none(
    `create index if not exists "ix_${tables.raw.backup}_batchId" on ${tables.backup} ("batchId")`,
  )
  const { rows }: { rows: number } = await db.one(
    `select count(*)::int as rows from ${tables.backup} where "batchId" = $(batchId)`,
    { batchId: options.batchId },
  )
  if (rows === 0) {
    throw new UsageError(`Batch ${options.batchId} has no rows left in ${tables.backup}`)
  }

  // the table's own delete rules: a row whose cascade parent is gone would have been deleted
  // with it, and a set-null reference that is gone would have been nulled
  const foreignKeys: { col: string; ref: string; refCol: string; onDelete: string }[] =
    await db.any(`
      select a.attname as col, c.confrelid::regclass::text as ref, af.attname as "refCol",
        c.confdeltype as "onDelete"
      from pg_constraint c
      join pg_attribute a on a.attrelid = c.conrelid and a.attnum = c.conkey[1]
      join pg_attribute af on af.attrelid = c.confrelid and af.attnum = c.confkey[1]
      where c.conrelid = '"activityRelations"'::regclass and c.contype = 'f'
        and array_length(c.conkey, 1) = 1
    `)
  const parentExists = (fk: { col: string; ref: string; refCol: string }) =>
    `exists (select 1 from ${fk.ref} p where p."${fk.refCol}" = b."${fk.col}")`
  const required = foreignKeys.filter((fk) => fk.onDelete === 'c')
  const nullable = new Map(
    foreignKeys.filter((fk) => fk.onDelete === 'n').map((fk) => [fk.col, fk]),
  )
  const selectList = columns
    .map((c) => {
      const fk = nullable.get(c)
      return fk ? `case when ${parentExists(fk)} then b."${c}" end` : `b."${c}"`
    })
    .join(', ')
  const restorable = required
    .map((fk) => `and (b."${fk.col}" is null or ${parentExists(fk)})`)
    .join(' ')

  const result = await db.tx(async (t) => {
    await t.none(`set local lock_timeout = '${CLEAN_LOCK_TIMEOUT}'`)
    const restored = await t.result(
      `
        insert into "activityRelations" (${columns.map((c) => `"${c}"`).join(', ')})
        select ${selectList} from ${tables.backup} b
        where b."batchId" = $(batchId) ${restorable}
        on conflict ("activityId") do nothing
      `,
      { batchId: options.batchId },
    )
    const reopen: { removed: number; keys: number; reopened: number } = await t.one(
      `
        with removed as (
          delete from ${tables.backup} b
          where b."batchId" = $(batchId)
            and exists (select 1 from "activityRelations" ar where ar."activityId" = b."activityId")
          returning "timestamp", platform, type, "sourceId", "segmentId"
        ),
        b as (
          select "timestamp", platform, type, "sourceId", "segmentId", count(*)::int as n
          from removed group by 1, 2, 3, 4, 5
        ),
        reopened as (
          update ${tables.keys} k set
            status = 'pending',
            deleted = greatest(coalesce(k.deleted, 0) - b.n, 0),
            "batchId" = null,
            "doneAt" = null
          from b
          where ${KEY_COLUMNS_JOIN('b')}
          returning 1
        )
        select (select count(*)::int from removed) as removed,
          (select count(*)::int from b) as keys,
          (select count(*)::int from reopened) as reopened
      `,
      { batchId: options.batchId },
    )
    if (reopen.reopened !== reopen.keys) {
      throw new InvariantError(
        `Restored rows of ${reopen.keys} keys but re-opened ${reopen.reopened} keys: rolled back`,
      )
    }
    return { restored: restored.rowCount, ...reopen }
  })

  log.info(
    {
      batchId: options.batchId,
      backupRows: rows,
      restored: result.restored,
      removedFromBackup: result.removed,
      reopenedKeys: result.reopened,
      // their member or segment is gone: they stay in the backup
      leftInBackup: rows - result.removed,
    },
    'rollback.done',
  )
  return EXIT_DONE
}

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

function parseDateFlag(flag: string, input: string | undefined): Date | undefined {
  if (input === undefined) {
    return undefined
  }
  const parsed = new Date(`${input}T00:00:00Z`)
  // Date rolls 2025-02-30 over to 2025-03-02: the date has to come back unchanged
  if (
    !DATE_PATTERN.test(input) ||
    Number.isNaN(parsed.getTime()) ||
    parsed.toISOString().slice(0, 10) !== input
  ) {
    throw new UsageError(`${flag} must be a valid YYYY-MM-DD date, got '${input}'`)
  }
  return parsed
}

function parseIntFlag(flag: string, input: string | undefined, min: number, max: number) {
  if (input === undefined) {
    return undefined
  }
  const parsed = Number(input)
  if (!Number.isInteger(parsed) || parsed < min || parsed > max) {
    throw new UsageError(`${flag} must be an integer from ${min} to ${max}, got '${input}'`)
  }
  return parsed
}

function parseListFlag(
  flag: string,
  input: string | undefined,
  pattern: RegExp,
): string[] | undefined {
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
      parseListFlag('--platforms', value(flags, '--platforms'), IDENTIFIER_PATTERN) ??
      DEFAULT_PLATFORMS,
    dryRun: flags.has('--dry-run'),
    yes: flags.has('--yes'),
  }
}

function parseSnapshot(flags: Map<string, string | true>): ISnapshotOptions {
  // a JSON snapshot past ~700k rows hits V8's string limit; gates need far fewer keys
  const keys = parseIntFlag('--keys', value(flags, '--keys'), 1, 20_000)
  const out = value(flags, '--out')
  if (keys === undefined || !out) {
    throw new UsageError('snapshot needs --keys and --out')
  }
  return {
    ...parseCommon(flags),
    segmentIds: parseListFlag('--segments', value(flags, '--segments'), UUID_PATTERN) ?? [],
    keys,
    out,
    retryDeferred: flags.has('--retry-deferred'),
  }
}

function parseVerify(flags: Map<string, string | true>): IVerifyOptions {
  const snapshotFile = value(flags, '--snapshot')
  if (!snapshotFile) {
    throw new UsageError('verify needs --snapshot')
  }
  return { ...parseCommon(flags), snapshot: snapshotFile }
}

function parseRollback(flags: Map<string, string | true>): IRollbackOptions {
  const batchId = value(flags, '--batch')
  if (!batchId || !UUID_PATTERN.test(batchId)) {
    throw new UsageError(`rollback needs --batch <uuid>, got '${batchId ?? ''}'`)
  }
  return { ...parseCommon(flags), batchId }
}

function parseCollect(flags: Map<string, string | true>): ICollectOptions {
  const from = parseDateFlag('--from', value(flags, '--from'))
  const to = parseDateFlag('--to', value(flags, '--to'))
  if (from && to && from >= to) {
    throw new UsageError('--from must be before --to')
  }
  if (from && flags.has('--rescan')) {
    throw new UsageError('--rescan and --from contradict each other')
  }
  return {
    ...parseCommon(flags),
    from,
    to,
    windowDays: parseIntFlag('--window-days', value(flags, '--window-days'), 1, 366) ?? 7,
    rescan: flags.has('--rescan'),
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
    segmentIds: parseListFlag('--segments', value(flags, '--segments'), UUID_PATTERN) ?? [],
    batchRows:
      parseIntFlag('--batch-rows', value(flags, '--batch-rows'), 2, MAX_BATCH_ROWS) ?? 20000,
    maxRows: parseIntFlag('--max-rows', value(flags, '--max-rows'), 1, 200_000_000),
    until,
    maxSlotLagBytes:
      (parseIntFlag('--max-slot-lag-mb', value(flags, '--max-slot-lag-mb'), 1, 1_000_000) ?? 1024) *
      1048576,
    skipSlotGuard: flags.has('--skip-slot-guard'),
    allowNightly: flags.has('--allow-nightly'),
    waitNightly: flags.has('--wait-nightly'),
    allowPartialCollect: flags.has('--allow-partial-collect'),
    retryDeferred: flags.has('--retry-deferred'),
    sampleBatches: parseIntFlag('--sample-batches', value(flags, '--sample-batches'), 1, 1000) ?? 3,
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
  // a signal at the prompt is a stop (exit 2) like one between batches; SIGTERM/SIGHUP only
  // set stopState, so the prompt watches it
  let signalled = false
  const watch = setInterval(() => {
    if (stopState.requested) {
      signalled = true
      rl.close()
    }
  }, 500)
  try {
    const answer = await new Promise<string>((resolve, reject) => {
      // readline takes Ctrl-C in a terminal, so the process signal handler never sees it
      rl.on('SIGINT', () => {
        signalled = true
        rl.close()
      })
      rl.on('close', () =>
        reject(
          signalled
            ? new NotStartedError('Stopped at the confirmation prompt')
            : new UsageError('Aborted by the operator'),
        ),
      )
      rl.question(`\n${plan}\nType the tag (${options.tag}) to proceed: `, resolve)
    })
    if (answer.trim() !== options.tag) {
      throw new UsageError('Aborted by the operator')
    }
  } finally {
    clearInterval(watch)
    rl.close()
  }
}

function printHelp(): void {
  process.stdout.write(USAGE)
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
  const parsers: Record<string, (f: Map<string, string | true>) => ICommonOptions> = {
    collect: parseCollect,
    clean: parseClean,
    status: parseCommon,
    snapshot: parseSnapshot,
    verify: parseVerify,
    rollback: parseRollback,
  }
  const options = parsers[command](flags)

  const runId = randomUUID()
  const log = baseLog.child({ runId, tag: options.tag, command })
  installSignalHandlers(log)

  // application_name in pg_stat_activity, so a DBA can tell this session from the worker's
  process.env.SERVICE = `cleanup-null-channel-${options.tag}`
  const timeoutMs =
    command === 'collect'
      ? COLLECT_STATEMENT_TIMEOUT_MS + 60_000
      : command === 'rollback'
        ? ROLLBACK_QUERY_TIMEOUT_MS
        : SESSION_QUERY_TIMEOUT_MS
  const db = await getDbConnection(
    { ...WRITE_DB_CONFIG(), queryTimeoutMs: timeoutMs, statementTimeoutMs: timeoutMs },
    1,
    0,
  )
  // one dedicated session for the whole run: the advisory run lock lives as long as it does
  const session = await db.connect()
  // from here a bare process.exit() can only come from @crowd/database's connection-error hook
  process.exitCode = EXIT_CONNECTION

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
    if (command === 'collect') {
      return await collect(session, log, options as ICollectOptions)
    }
    if (command === 'clean') {
      return await clean(session, log, options as ICleanOptions, runId)
    }
    if (command === 'snapshot') {
      return await snapshot(session, log, options as ISnapshotOptions)
    }
    if (command === 'verify') {
      return await verify(session, log, options as IVerifyOptions)
    }
    if (command === 'rollback') {
      return await rollback(session, log, options as IRollbackOptions)
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
    if (error instanceof NotStartedError) {
      baseLog.warn({ message: error.message }, 'run.not-started')
      process.exit(EXIT_STOPPED)
    }
    if (error instanceof GuardError) {
      baseLog.error({ message: error.message }, 'run.guard-failed')
      process.exit(EXIT_ERROR)
    }
    baseLog.error(pgFields(error), 'run.failed')
    process.exit(isConnectionError(error) ? EXIT_CONNECTION : EXIT_ERROR)
  })
