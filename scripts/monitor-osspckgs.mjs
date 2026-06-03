#!/usr/bin/env node
/**
 * TUI monitor for osspckgs_ingest_jobs.
 * Usage: node scripts/monitor-osspckgs.mjs
 *
 * Env: CROWD_PACKAGES_DB_WRITE_HOST, CROWD_PACKAGES_DB_PORT,
 *      CROWD_PACKAGES_DB_DATABASE, CROWD_PACKAGES_DB_USERNAME, CROWD_PACKAGES_DB_PASSWORD
 *
 * Keys: ↑/↓ navigate  Enter expand/collapse  r refresh  q quit
 */

import { createRequire } from 'module'
import readline from 'readline'

const require = createRequire(import.meta.url)
const { Client } = require('pg')

// ── ANSI helpers ──────────────────────────────────────────────────────────────

const ESC = '\x1b['
const A = {
  reset:     '\x1b[0m',
  bold:      '\x1b[1m',
  dim:       '\x1b[2m',
  hide:      '\x1b[?25l',
  show:      '\x1b[?25h',
  altOn:     '\x1b[?1049h',
  altOff:    '\x1b[?1049l',
  clear:     '\x1b[2J\x1b[H',
  // fg
  black:     '\x1b[30m',
  red:       '\x1b[31m',
  green:     '\x1b[32m',
  yellow:    '\x1b[33m',
  blue:      '\x1b[34m',
  magenta:   '\x1b[35m',
  cyan:      '\x1b[36m',
  white:     '\x1b[37m',
  gray:      '\x1b[90m',
  // bg
  bgBlue:    '\x1b[44m',
  bgGreen:   '\x1b[42m',
  bgRed:     '\x1b[41m',
  bgBlack:   '\x1b[40m',
  bgGray:    '\x1b[100m',
}

const move = (row, col) => `${ESC}${row};${col}H`
const clearLine = () => `${ESC}2K`
const w = process.stdout

function write(s) { w.write(s) }
function writeln(row, col, s) { write(move(row, col) + clearLine() + s) }

// ── DB ────────────────────────────────────────────────────────────────────────

function dbClient() {
  return new Client({
    host:     process.env.CROWD_PACKAGES_DB_WRITE_HOST     ?? 'localhost',
    port:     parseInt(process.env.CROWD_PACKAGES_DB_PORT  ?? '5432'),
    database: process.env.CROWD_PACKAGES_DB_DATABASE       ?? 'packages-db',
    user:     process.env.CROWD_PACKAGES_DB_USERNAME       ?? 'postgres',
    password: process.env.CROWD_PACKAGES_DB_PASSWORD       ?? '',
  })
}

async function fetchJobs() {
  const client = dbClient()
  await client.connect()
  try {
    const { rows } = await client.query(`
      SELECT
        id, job_kind, status, sync_mode,
        snapshot_at, provisional_snapshot_at,
        gcs_prefix, export_name,
        row_count_bq, row_count_staging, row_count_pg,
        bq_bytes_billed,
        table_row_counts,
        error_message,
        started_at, finished_at, cleaned_at
      FROM osspckgs_ingest_jobs
      ORDER BY started_at DESC
      LIMIT 100
    `)
    return rows
  } finally {
    await client.end()
  }
}

// ── Formatting ─────────────────────────────────────────────────────────────────

const STATUS_COLOR = {
  pending:   A.gray,
  exporting: A.yellow,
  exported:  A.yellow,
  loading:   A.cyan,
  merging:   A.blue,
  done:      A.green,
  failed:    A.red,
  cleaned:   A.dim + A.gray,
}

const STATUS_ICON = {
  pending:   '○',
  exporting: '⟳',
  exported:  '⟳',
  loading:   '⟳',
  merging:   '⟳',
  done:      '✓',
  failed:    '✗',
  cleaned:   '–',
}

function statusStr(status) {
  const c = STATUS_COLOR[status] ?? ''
  const i = STATUS_ICON[status] ?? '?'
  return `${c}${i} ${status}${A.reset}`
}

function fmtNum(n) {
  if (n == null) return A.dim + '—' + A.reset
  return Number(n).toLocaleString()
}

function fmtGb(bytes) {
  if (bytes == null) return A.dim + '—' + A.reset
  const gb = Number(bytes) / 1e9
  return gb >= 1 ? `${gb.toFixed(1)} GB` : `${(Number(bytes) / 1e6).toFixed(0)} MB`
}

function fmtCompact(n) {
  if (n == null) return A.dim + '—' + A.reset
  const v = Number(n)
  if (v >= 1e9) return `${(v / 1e9).toFixed(1)}B`
  if (v >= 1e6) return `${(v / 1e6).toFixed(1)}M`
  if (v >= 1e3) return `${(v / 1e3).toFixed(0)}K`
  return String(v)
}

function fmtRate(ratePerMin) {
  if (ratePerMin >= 60000) return fmtCompact(Math.round(ratePerMin)) + '/min'
  return fmtCompact(Math.round(ratePerMin / 60)) + '/s'
}

function fmtEtaStr(ms) {
  const s = Math.ceil(ms / 1000)
  const h = Math.floor(s / 3600)
  const m = Math.floor((s % 3600) / 60)
  const sec = s % 60
  if (h > 0) return `${h}h${String(m).padStart(2, '0')}m`
  if (m > 5) return `${m}m`
  return `${m}m${String(sec).padStart(2, '0')}s`
}

function fmtElapsed(start, end) {
  const ms = (end ? new Date(end) : new Date()) - new Date(start)
  const s = Math.floor(ms / 1000)
  const h = Math.floor(s / 3600)
  const m = Math.floor((s % 3600) / 60)
  const sec = s % 60
  return [h, m, sec].map(v => String(v).padStart(2, '0')).join(':')
}

function fmtDate(d) {
  if (!d) return A.dim + '—' + A.reset
  return new Date(d).toISOString().replace('T', ' ').slice(0, 19)
}

function pct(a, b) {
  if (!a || !b || Number(b) === 0) return ''
  return ` (${Math.round(Number(a) / Number(b) * 100)}%)`
}

function progressBar(ratio, width = 20) {
  if (ratio == null || isNaN(ratio)) return A.dim + '─'.repeat(width) + A.reset
  const filled = Math.min(Math.round(ratio * width), width)
  const empty = width - filled
  const color = ratio >= 1 ? A.green : A.cyan
  return color + '█'.repeat(filled) + A.dim + '░'.repeat(empty) + A.reset
}

// Re-applies `bg` before the padding spaces so cells that end with A.reset
// (e.g. fmtNum, statusStr) don't leave a bg gap in selected rows.
function padCell(s, len, bg) {
  const visible = s.replace(/\x1b\[[0-9;]*m/g, '')
  const pad = Math.max(0, len - visible.length)
  return s + bg + ' '.repeat(pad)
}

function truncate(s, len) {
  if (!s) return ''
  return s.length > len ? s.slice(0, len - 1) + '…' : s
}

// ── Table counts ──────────────────────────────────────────────────────────────

// Maps job_kind → target Postgres tables (in write order).
const KIND_TABLES = {
  packages:              ['packages'],
  versions:              ['versions'],
  package_dependencies:  ['package_dependencies'],
  repos:                 ['repos'],
  package_repos:         ['package_repos'],
  advisories:            ['advisories'],
  advisory_packages:     ['advisory_packages', 'advisory_affected_ranges'],
  dependent_counts:      ['packages'],
}

const TABLE_ABBREV = {
  packages:                  'pkgs',
  versions:                  'vers',
  package_dependencies:      'deps',
  repos:                     'repos',
  package_repos:             'pkg_repos',
  advisories:                'adv',
  advisory_packages:         'adv_pkgs',
  advisory_affected_ranges:  'adv_ranges',
}

async function fetchTableCounts() {
  const client = dbClient()
  await client.connect()
  try {
    const { rows } = await client.query(`
      SELECT relname, n_live_tup
      FROM pg_stat_user_tables
      WHERE relname IN (
        'packages', 'versions', 'package_dependencies',
        'repos', 'package_repos',
        'advisories', 'advisory_packages', 'advisory_affected_ranges'
      )
    `)
    const result = {}
    for (const row of rows) result[row.relname] = Number(row.n_live_tup)
    return result
  } finally {
    await client.end()
  }
}

// ── Ecosystem extraction ───────────────────────────────────────────────────────

const KNOWN_ECOSYSTEMS = ['npm', 'go', 'maven', 'pypi', 'nuget', 'cargo']

// Parses ecosystem names from gcs_prefix or export_name.
// Looks for -<eco>- or -<eco>/ patterns so "go" doesn't match inside "cargo".
function extractEcosystem(job) {
  const src = [job.gcs_prefix ?? '', job.export_name ?? ''].join(' ').toLowerCase()
  const found = KNOWN_ECOSYSTEMS.filter(e => new RegExp(`[-/]${e}[-/]|[-/]${e}$`).test(src))
  return found.length > 0 ? found.join(',') : null
}

// ── Layout ────────────────────────────────────────────────────────────────────

const COL = {
  id:      5,
  kind:    20,
  eco:     10,
  status:  18,
  mode:    10,
  bq:      10,
  files:   24,
  staging: 12,
  pg:      14,
  table:   18,
  elapsed: 12,
  chunk:   26,
  total:   24,
}

function tableHeader() {
  const content = (
    ' ' + 'ID'.padEnd(COL.id) +
    'KIND'.padEnd(COL.kind) +
    'ECO'.padEnd(COL.eco) +
    'STATUS'.padEnd(COL.status) +
    'MODE'.padEnd(COL.mode) +
    'BQ ROWS'.padEnd(COL.bq) +
    'FILES'.padEnd(COL.files) +
    'STAGING'.padEnd(COL.staging) +
    'PG ROWS'.padEnd(COL.pg) +
    'TABLE ROWS'.padEnd(COL.table) +
    'ELAPSED'.padEnd(COL.elapsed) +
    'MERGE ETA'.padEnd(COL.chunk) +
    'TOTAL ETA'
  )
  const rightPad = Math.max(0, (process.stdout.columns || 80) - content.length)
  return A.bold + A.bgGray + A.white + content + ' '.repeat(rightPad) + A.reset
}

function filesCell(job, bg) {
  const trc = job.table_row_counts ?? {}
  const done  = trc['progress:done']
  const total = trc['progress:total']
  if (done == null || total == null || Number(total) === 0) {
    return padCell(A.dim + '—' + A.reset, COL.files, bg)
  }
  const ratio = Number(done) / Number(total)
  const pctVal = Math.round(ratio * 100)
  const suffix = pctVal >= 100 ? '✓' : `${pctVal}%`
  return padCell(`${progressBar(ratio, 6)}${bg} ${done}/${total} ${suffix}`, COL.files, bg)
}

function stagingCell(job, bg) {
  return padCell(fmtCompact(job.row_count_staging), COL.staging, bg)
}

function tableCountCell(job, bg) {
  const tables = KIND_TABLES[job.job_kind]
  if (!tables) return padCell(A.dim + '—' + A.reset, COL.table, bg)
  if (tables.length === 1) {
    const count = tableCounts[tables[0]]
    return padCell(count != null ? fmtCompact(count) : A.dim + '—' + A.reset, COL.table, bg)
  }
  // Multi-table: "abbrev:N abbrev:M"
  const parts = tables.map(t => {
    const abbrev = TABLE_ABBREV[t] ?? t
    const count = tableCounts[t]
    return `${abbrev}:${count != null ? fmtCompact(count) : A.dim + '—' + A.reset}`
  })
  return padCell(parts.join(' '), COL.table, bg)
}

function pgCell(job, bg) {
  const pgRows = job.row_count_pg
  if (job.status === 'merging' && pgRows) {
    const totalRows = Number(job.row_count_staging) || Number(job.row_count_bq) || 0
    const pctVal = totalRows > 0 ? Math.round((Number(pgRows) / totalRows) * 100) : null
    const pctStr = pctVal != null ? ` ${pctVal}%` : ''
    return padCell(`${fmtCompact(pgRows)}${pctStr}`, COL.pg, bg)
  }
  return padCell(fmtNum(pgRows), COL.pg, bg)
}

function chunkEtaCell(job, bg) {
  const eta = computeChunkEta(job)
  if (eta == null) return padCell(A.dim + '—' + A.reset, COL.chunk, bg)
  return padCell(
    `${progressBar(eta.ratio, 6)}${bg} ~${fmtEtaStr(eta.ms)} ${A.dim}${fmtRate(eta.ratePerMin)}${A.reset}`,
    COL.chunk, bg,
  )
}

function totalEtaCell(job, bg) {
  const eta = computeTotalEta(job)
  if (eta == null) return padCell(A.dim + '—' + A.reset, COL.total, bg)
  return padCell(
    `${progressBar(eta.ratio, 8)}${bg} ~${fmtEtaStr(eta.ms)} ${A.dim}${fmtRate(eta.ratePerMin)}${A.reset}`,
    COL.total, bg,
  )
}

function tableRow(job, selected) {
  const bg = selected ? A.bgBlue + A.white : ''
  const elapsed = ['pending', 'failed'].includes(job.status)
    ? A.dim + '—' + A.reset
    : fmtElapsed(job.started_at, job.finished_at)

  return (
    bg +
    ' ' + String(job.id).padEnd(COL.id) +
    A.bold + truncate(job.job_kind, COL.kind - 1).padEnd(COL.kind) + A.reset + bg +
    padCell(extractEcosystem(job) ? A.cyan + extractEcosystem(job) + A.reset : A.dim + '—' + A.reset, COL.eco, bg) +
    padCell(statusStr(job.status), COL.status, bg) +
    job.sync_mode.padEnd(COL.mode) +
    padCell(fmtCompact(job.row_count_bq), COL.bq, bg) +
    filesCell(job, bg) +
    stagingCell(job, bg) +
    pgCell(job, bg) +
    tableCountCell(job, bg) +
    padCell(elapsed, COL.elapsed, bg) +
    chunkEtaCell(job, bg) +
    totalEtaCell(job, bg) +
    bg + '\x1b[K' +
    A.reset
  )
}

function renderDetail(job, cols) {
  const lines = []
  const sep = A.dim + '─'.repeat(cols - 2) + A.reset

  const snapshotDate = job.snapshot_at
    ? String(job.snapshot_at).slice(0, 10)
    : job.provisional_snapshot_at
      ? A.dim + String(job.provisional_snapshot_at).slice(0, 10) + ' (provisional)' + A.reset
      : A.dim + '—' + A.reset

  lines.push(` ${A.bold}Job #${job.id} — ${job.job_kind}${A.reset}   ${statusStr(job.status)}  ${A.dim}${job.sync_mode}${A.reset}`)
  lines.push(` ${sep}`)
  lines.push(` ${A.dim}snapshot:${A.reset}  ${snapshotDate}`)
  lines.push(` ${A.dim}started:${A.reset}   ${fmtDate(job.started_at)}   ${A.dim}finished:${A.reset} ${fmtDate(job.finished_at)}`)
  lines.push(` ${A.dim}elapsed:${A.reset}   ${fmtElapsed(job.started_at, job.finished_at)}   ${A.dim}bq cost:${A.reset} ${fmtGb(job.bq_bytes_billed)}`)
  if (job.gcs_prefix) {
    lines.push(` ${A.dim}gcs:${A.reset}       ${A.dim}${truncate(job.gcs_prefix, cols - 14)}${A.reset}`)
  }
  lines.push(` ${sep}`)

  // Pipeline section
  lines.push(` ${A.bold}Pipeline${A.reset}`)

  const trc = job.table_row_counts ?? {}
  const bqRows     = Number(job.row_count_bq)     || null
  const stagRows   = Number(job.row_count_staging) || null
  const pgRows     = Number(job.row_count_pg)      || null

  const isCursor   = trc['bq:stream'] != null
  const isExport   = trc['bq:export'] != null || job.gcs_prefix != null

  // All pipeline label cells are this wide — keeps bars aligned.
  const LW = 16

  if (isExport) {
    // bq:export: full bar only when rows > 0; dashes when 0 (reused/skipped BQ)
    const bqExportCount = trc['bq:export'] ?? job.row_count_bq
    const bqExportBar   = bqExportCount > 0 ? progressBar(1.0) : progressBar(null)
    lines.push(` ${A.dim}${'bq:export'.padEnd(LW)}${A.reset} ${bqExportBar} ${fmtNum(bqExportCount)}`)

    const stagKeys = Object.keys(trc).filter(k => k.startsWith('staging:'))
    if (stagKeys.length > 0) {
      for (const k of stagKeys) {
        const stagCount = Number(trc[k])
        // ratio vs bqRows when known; full bar when bqRows absent (reused export — staging completed)
        const ratio = bqRows ? stagCount / bqRows : 1.0
        lines.push(` ${A.dim}${truncate(k, LW).padEnd(LW)}${A.reset} ${progressBar(ratio)} ${fmtNum(stagCount)}${pct(stagCount, bqRows)}`)
      }
    } else {
      const ratio = bqRows ? stagRows / bqRows : stagRows ? 1.0 : null
      lines.push(` ${A.dim}${'staging'.padEnd(LW)}${A.reset} ${progressBar(ratio)} ${fmtNum(stagRows)}${pct(stagRows, bqRows)}`)
    }

    // Show file-level progress (loading and merging — persists after loading completes)
    const progressDone  = trc['progress:done']
    const progressTotal = trc['progress:total']
    if (progressDone != null && progressTotal != null && Number(progressTotal) > 0) {
      const ratio   = Number(progressDone) / Number(progressTotal)
      const pctVal  = Math.round(ratio * 100)
      const suffix  = pctVal >= 100 ? '✓' : `${pctVal}%`
      lines.push(` ${A.dim}${'files'.padEnd(LW)}${A.reset} ${progressBar(ratio)} ${progressDone}/${progressTotal} (${suffix})`)
    }

    const ref = stagRows || bqRows  // prefer staging as denominator for final rows
    const finalKeys = Object.keys(trc).filter(k => !k.startsWith('bq:') && !k.startsWith('staging:') && !k.startsWith('progress:'))
    if (finalKeys.length > 0) {
      for (const k of finalKeys) {
        const count = Number(trc[k])
        const ratio = ref ? count / ref : null
        lines.push(` ${A.dim}${truncate(k, LW).padEnd(LW)}${A.reset} ${progressBar(ratio)} ${fmtNum(count)}${pct(count, ref)}`)
      }
    } else if (pgRows) {
      const ratio = ref ? pgRows / ref : null
      lines.push(` ${A.dim}${'final'.padEnd(LW)}${A.reset} ${progressBar(ratio)} ${fmtNum(pgRows)}${pct(pgRows, ref)}`)
    } else {
      lines.push(` ${A.dim}${'final'.padEnd(LW)}${A.reset} ${progressBar(null)} ${A.dim}—${A.reset}`)
    }
  } else if (isCursor) {
    const bqCount  = Number(trc['bq:stream'])
    const finalKeys = Object.keys(trc).filter(k => k !== 'bq:stream')
    lines.push(` ${A.dim}${'bq:stream'.padEnd(LW)}${A.reset} ${progressBar(1.0)} ${fmtNum(bqCount)}`)
    for (const k of finalKeys) {
      const count = Number(trc[k])
      const ratio = bqCount ? count / bqCount : null
      lines.push(` ${A.dim}${truncate(k, LW).padEnd(LW)}${A.reset} ${progressBar(ratio)} ${fmtNum(count)}${pct(count, bqCount)}`)
    }
    if (finalKeys.length === 0 && pgRows) {
      lines.push(` ${A.dim}${'final'.padEnd(LW)}${A.reset} ${progressBar(pgRows / bqCount)} ${fmtNum(pgRows)}`)
    }
  } else {
    // No pipeline data yet
    lines.push(` ${A.dim}no pipeline data yet${A.reset}`)
  }

  lines.push(` ${sep}`)

  if (job.error_message) {
    lines.push(` ${A.red}${A.bold}Error:${A.reset} ${A.red}${truncate(job.error_message, cols - 10)}${A.reset}`)
  } else {
    lines.push(` ${A.dim}no errors${A.reset}`)
  }

  // table_row_counts raw (exclude ephemeral progress keys)
  const trcDisplay = Object.fromEntries(Object.entries(trc).filter(([k]) => !k.startsWith('progress:')))
  if (Object.keys(trcDisplay).length > 0) {
    lines.push(` ${sep}`)
    lines.push(` ${A.dim}table_row_counts: ${JSON.stringify(trcDisplay)}${A.reset}`)
  }

  return lines
}

// ── State ─────────────────────────────────────────────────────────────────────

let jobs = []
let tableCounts = {}
let selected = 0
let detailOpen = false
let lastRefresh = null
let error = null
let refreshTimer = null

// Per-chunk merge timing via status transitions.
// row_count_pg only updates once per chunk (after the full tx commits) so delta-based rate is useless.
// Instead: detect loading→merging as merge start, merging→loading/done as completion.
// Historical rate from completed chunks drives the ETA for the current chunk.
const chunkMergeHistory = new Map()
// { prevStatus, mergeStart: ms|null, prevStagingRows, completedChunks: [{stagingRows,durationMs}] }

function updateChunkHistory(job, now) {
  const stagingRows = Number(job.row_count_staging) || 0
  const existing = chunkMergeHistory.get(job.id)

  if (!existing) {
    // If already merging when monitor starts, use now as approximate merge start.
    chunkMergeHistory.set(job.id, {
      prevStatus: job.status,
      mergeStart: job.status === 'merging' ? now : null,
      prevStagingRows: stagingRows,
      completedChunks: [],
    })
    return
  }

  let { mergeStart, completedChunks } = existing

  // loading → merging: new chunk merge just started — capture start time
  if (job.status === 'merging' && existing.prevStatus === 'loading') {
    mergeStart = now
  }

  // staging changed mid-merge (shouldn't happen normally): treat as fresh merge
  if (job.status === 'merging' && existing.prevStatus === 'merging' && stagingRows !== existing.prevStagingRows) {
    mergeStart = now
  }

  // merging → loading/done/cleaned: chunk merge completed — record duration for rate history
  if (existing.prevStatus === 'merging' && job.status !== 'merging') {
    if (mergeStart != null && existing.prevStagingRows > 0) {
      const durationMs = now - mergeStart
      completedChunks = [...completedChunks, { stagingRows: existing.prevStagingRows, durationMs }].slice(-10)
    }
    mergeStart = null
  }

  chunkMergeHistory.set(job.id, {
    prevStatus: job.status,
    mergeStart,
    prevStagingRows: stagingRows,
    completedChunks,
  })
}

// ETA to finish merging the current staging chunk.
// Uses historical merge rate from completed chunks when available; falls back to overall job
// throughput (includes loading time, so slightly conservative) when none observed yet.
function computeChunkEta(job) {
  if (job.status !== 'merging') return null
  const stagingRows = Number(job.row_count_staging) || 0
  if (!stagingRows) return null
  const hist = chunkMergeHistory.get(job.id)
  if (!hist || hist.mergeStart == null) return null

  let rateRowsPerMs
  if (hist.completedChunks.length > 0) {
    // Exponential recency weighting: chunk i gets weight 2^i (oldest=0, newest=n-1).
    // Most recent chunk contributes ~50% of the rate; history stabilises it.
    const chunks = hist.completedChunks
    let weightedRate = 0, totalWeight = 0
    for (let i = 0; i < chunks.length; i++) {
      const w = Math.pow(2, i)
      weightedRate += (chunks[i].stagingRows / chunks[i].durationMs) * w
      totalWeight += w
    }
    if (totalWeight === 0) return null
    rateRowsPerMs = weightedRate / totalWeight
  } else {
    // No completed chunks observed — derive rate from overall job progress (conservative: includes loading)
    const pgRows = Number(job.row_count_pg) || 0
    if (!pgRows || !job.started_at) return null
    const jobElapsedMs = Date.now() - new Date(job.started_at).getTime()
    if (jobElapsedMs < 10000) return null
    rateRowsPerMs = pgRows / jobElapsedMs
  }

  const estimatedDurationMs = stagingRows / rateRowsPerMs
  const elapsedMs = Date.now() - hist.mergeStart
  const remainingMs = estimatedDurationMs - elapsedMs
  if (remainingMs <= 0) return null
  return { ms: remainingMs, ratio: Math.min(elapsedMs / estimatedDurationMs, 1), ratePerMin: rateRowsPerMs * 60000 }
}

// ETA for the entire job (all remaining files + merges).
// Uses job.started_at so rate includes both loading and merging time naturally.
function computeTotalEta(job) {
  if (!['loading', 'merging'].includes(job.status)) return null
  const pgRows = Number(job.row_count_pg) || 0
  const trc = job.table_row_counts ?? {}
  const filesDone = Number(trc['progress:done']) || 0
  const filesTotal = Number(trc['progress:total']) || 0
  const stagingRows = Number(job.row_count_staging) || 0
  if (!filesDone || !filesTotal || !pgRows || !job.started_at) return null
  const bqRows = Number(job.row_count_bq) || 0
  const rowsPerFile = bqRows > 0 && filesTotal > 0
    ? bqRows / filesTotal
    : (stagingRows > 0 ? stagingRows / filesDone : 0)
  if (!rowsPerFile) return null
  const estimatedTotal = bqRows > 0 ? bqRows : rowsPerFile * filesTotal
  if (pgRows >= estimatedTotal) return null
  const elapsedMs = Date.now() - new Date(job.started_at).getTime()
  if (elapsedMs < 10000 || pgRows < 1000) return null
  const ratePerMs = pgRows / elapsedMs
  if (ratePerMs <= 0) return null
  return { ms: (estimatedTotal - pgRows) / ratePerMs, ratio: Math.min(pgRows / estimatedTotal, 1), ratePerMin: ratePerMs * 60000 }
}

// ── Render ─────────────────────────────────────────────────────────────────────

function render() {
  const { rows, columns } = process.stdout
  write(A.reset + A.hide + A.clear)

  const refreshStr = lastRefresh ? `last refresh: ${lastRefresh}` : 'loading…'
  const title = ` ${A.bold}${A.cyan}OSSPCKGS Monitor${A.reset}  ${A.dim}${refreshStr}${A.reset}`
  const keys = `${A.dim}[↑↓] navigate  [Enter] detail  [r] refresh  [q] quit${A.reset}`
  writeln(1, 1, title)
  writeln(2, 1, keys)

  if (error) {
    writeln(3, 1, `${A.red}DB Error: ${error}${A.reset}`)
    write(A.show)
    return
  }

  // Summary bar
  const counts = { running: 0, done: 0, failed: 0 }
  for (const j of jobs) {
    if (['exporting','exported','loading','merging'].includes(j.status)) counts.running++
    else if (j.status === 'done' || j.status === 'cleaned') counts.done++
    else if (j.status === 'failed') counts.failed++
  }
  writeln(3, 1,
    ` ${A.dim}total:${A.reset} ${jobs.length}` +
    `  ${A.cyan}running:${A.reset} ${counts.running}` +
    `  ${A.green}done:${A.reset} ${counts.done}` +
    `  ${A.red}failed:${A.reset} ${counts.failed}`
  )

  // Header
  const headerRow = 5
  writeln(headerRow, 1, tableHeader())

  // List
  const listStart = headerRow + 1
  const detailLines = detailOpen && jobs[selected] ? renderDetail(jobs[selected], columns) : []
  const detailHeight = detailOpen ? Math.min(detailLines.length + 2, Math.floor(rows * 0.55)) : 0
  const listHeight = rows - listStart - detailHeight - 1

  const listEnd = listStart + listHeight
  for (let i = 0; i < listHeight; i++) {
    const job = jobs[i]
    if (!job) {
      writeln(listStart + i, 1, '')
    } else {
      writeln(listStart + i, 1, tableRow(job, i === selected))
    }
  }

  // Separator
  if (detailOpen) {
    const sepRow = listEnd
    writeln(sepRow, 1, A.dim + '─'.repeat(columns) + A.reset)

    // Detail panel
    const panelStart = sepRow + 1
    for (let i = 0; i < detailHeight - 1; i++) {
      const line = detailLines[i] ?? ''
      writeln(panelStart + i, 1, line)
    }
  }

  write(A.show)
}

// ── Data ───────────────────────────────────────────────────────────────────────

async function refresh() {
  try {
    const [newJobs, newTableCounts] = await Promise.all([fetchJobs(), fetchTableCounts()])
    jobs = newJobs
    tableCounts = newTableCounts
    if (selected >= jobs.length) selected = Math.max(0, jobs.length - 1)
    lastRefresh = new Date().toLocaleTimeString()
    error = null
  } catch (e) {
    error = e.message
  }
  render() // render first — ETAs use history from previous refresh
  const now = Date.now()
  for (const job of jobs) {
    updateChunkHistory(job, now)
  }
}

function scheduleRefresh() {
  if (refreshTimer) clearTimeout(refreshTimer)
  refreshTimer = setTimeout(async () => {
    await refresh()
    scheduleRefresh()
  }, 2000)
}

// ── Input ─────────────────────────────────────────────────────────────────────

function setupInput() {
  readline.emitKeypressEvents(process.stdin)
  if (process.stdin.isTTY) process.stdin.setRawMode(true)

  process.stdin.on('keypress', async (_str, key) => {
    if (!key) return
    if (key.name === 'q' || (key.ctrl && key.name === 'c')) {
      cleanup()
      process.exit(0)
    }
    if (key.name === 'up') {
      selected = Math.max(0, selected - 1)
      render()
    }
    if (key.name === 'down') {
      selected = Math.min(jobs.length - 1, selected + 1)
      render()
    }
    if (key.name === 'return') {
      if (jobs.length === 0) return
      detailOpen = !detailOpen
      render()
    }
    if (key.name === 'r') {
      await refresh()
      scheduleRefresh()
    }
  })
}

// ── Lifecycle ─────────────────────────────────────────────────────────────────

function cleanup() {
  if (refreshTimer) clearTimeout(refreshTimer)
  write(A.show + A.altOff)
  if (process.stdin.isTTY) process.stdin.setRawMode(false)
}

process.on('SIGINT', () => { cleanup(); process.exit(0) })
process.on('SIGTERM', () => { cleanup(); process.exit(0) })
process.on('exit', cleanup)
process.stdout.on('resize', render)

write(A.altOn + A.reset + A.hide + A.clear)
setupInput()
await refresh()
scheduleRefresh()
