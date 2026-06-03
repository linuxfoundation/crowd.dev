/**
 * Data-quality scorecard for the Maven enrichment output.
 *
 * Runs the read-only checks in ../maven/data_quality.sql against the packages DB
 * and prints a grouped table (coverage, ingestion-source breakdown, anomalies,
 * integrity, freshness). Useful before/after a backfill, and to validate prod.
 *
 * Run with env loaded:
 *   pnpm run validate:maven-quality:local
 *
 * Read-only: a single SELECT, safe to point at a prod read-replica.
 * Exits non-zero if any check has status FAIL (handy for CI / gating a deploy).
 */
import { readFileSync } from 'fs'
import { join } from 'path'

import { getServiceLogger } from '@crowd/logging'

import { getPackagesDb } from '../db'

const log = getServiceLogger()

interface ReportRow {
  section: string
  metric: string
  value: string | number
  pct: string | null
  status: string
}

const SQL_PATH = join(__dirname, '../maven/data_quality.sql')

function pad(s: string, len: number): string {
  return s.length >= len ? s : s + ' '.repeat(len - s.length)
}

function padLeft(s: string, len: number): string {
  return s.length >= len ? s : ' '.repeat(len - s.length) + s
}

function render(rows: ReportRow[]): string {
  const lines: string[] = ['', 'Maven enrichment — data quality scorecard', '']
  let currentSection = ''

  for (const r of rows) {
    if (r.section !== currentSection) {
      currentSection = r.section
      lines.push('', currentSection)
    }
    const value = padLeft(String(r.value), 10)
    const pct = padLeft(r.pct ?? '', 8)
    const status = pad(r.status, 4)
    lines.push(`  [${status}] ${pad(r.metric, 44)} ${value}  ${pct}`)
  }
  lines.push('')
  return lines.join('\n')
}

const main = async () => {
  const qx = await getPackagesDb()
  const sql = readFileSync(SQL_PATH, 'utf8')

  const rows: ReportRow[] = await qx.select(sql)

  process.stdout.write(render(rows))

  const failures = rows.filter((r) => r.status === 'FAIL')
  const warnings = rows.filter((r) => r.status === 'WARN' || r.status === 'POOR' || r.status === 'LOW')

  if (failures.length > 0) {
    log.error(
      { failures: failures.map((f) => `${f.section} / ${f.metric} = ${f.value}`) },
      `Data quality: ${failures.length} FAIL check(s)`,
    )
    process.exit(1)
  }

  log.info(
    { warnings: warnings.length, checks: rows.length },
    `Data quality OK — no FAIL checks (${warnings.length} warning(s))`,
  )
  process.exit(0)
}

main().catch((err) => {
  log.error({ err }, 'Data quality validation failed')
  process.exit(1)
})
