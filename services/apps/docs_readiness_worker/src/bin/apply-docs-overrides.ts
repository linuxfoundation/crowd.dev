// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
// Bulk-apply reviewed docs overrides (IN-1425). Dry run by default; writes only with --apply.
// Usage: pnpm run script:apply-docs-overrides <file.csv> [--apply]  (CSV columns: project,docsUrl)
// project is an insightsProjects id or slug; docsUrl is an http(s) URL, or none for "no docs".
import { readFileSync } from 'node:fs'

import {
  READ_DB_CONFIG,
  WRITE_DB_CONFIG,
  getDbConnection,
} from '@crowd/data-access-layer/src/database'
import {
  createProjectDocOverride,
  findActiveProjectDocOverride,
} from '@crowd/data-access-layer/src/project-doc-overrides'
import { QueryExecutor, pgpQx } from '@crowd/data-access-layer/src/queryExecutor'

const SUBMITTED_BY = 'script:IN-1425'
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i

type Status = 'change' | 'unchanged' | 'superseded' | 'error' | 'applied' | 'failed'

interface Entry {
  row: number
  project: string
  projectId?: string
  target?: string | null
  // undefined means the project has no active override.
  current?: string | null
  status: Status
  detail?: string
}

export interface Summary {
  change: number
  unchanged: number
  superseded: number
  error: number
  applied: number
  failed: number
}

export function parseCsv(text: string): string[][] {
  const s = text.replace(/^﻿/, '')
  const records: string[][] = []
  let record: string[] = []
  let field = ''
  let quoted = false

  for (let i = 0; i < s.length; i++) {
    const c = s[i]
    if (quoted) {
      if (c !== '"') field += c
      else if (s[i + 1] === '"') {
        field += '"'
        i++
      } else quoted = false
    } else if (c === '"') quoted = true
    else if (c === ',') {
      record.push(field)
      field = ''
    } else if (c === '\n' || c === '\r') {
      if (c === '\r' && s[i + 1] === '\n') i++
      record.push(field)
      records.push(record)
      record = []
      field = ''
    } else field += c
  }

  if (quoted) throw new Error('CSV has an unterminated quoted field')
  if (field !== '' || record.length > 0) {
    record.push(field)
    records.push(record)
  }

  return records
}

// Returns the URL string, null for "no docs", or an error message.
export function parseDocsUrl(value: string): { url: string | null } | { error: string } {
  const trimmed = value.trim()
  if (trimmed === 'none') return { url: null }
  if (trimmed === '') return { error: 'empty docsUrl (write none for "no docs")' }

  let parsed: URL
  try {
    parsed = new URL(trimmed)
  } catch {
    return { error: `invalid URL "${trimmed}"` }
  }
  if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
    return { error: `URL must be http or https: "${trimmed}"` }
  }
  return { url: trimmed }
}

async function findProjectIds(qx: QueryExecutor, keys: string[]): Promise<Map<string, string>> {
  const rows: { id: string; slug: string }[] = keys.length
    ? await qx.select(
        `
        SELECT "id", "slug"
        FROM "insightsProjects"
        WHERE "enabled" AND "deletedAt" IS NULL
          AND ("id"::text = ANY($(keys)) OR "slug" = ANY($(keys)))
        `,
        { keys },
      )
    : []

  const byKey = new Map<string, string>()
  for (const r of rows) {
    byKey.set(r.id, r.id)
    byKey.set(r.slug, r.id)
  }
  return byKey
}

const show = (v: string | null | undefined) => (v === undefined ? '(no override)' : (v ?? 'none'))

function printTable(entries: Entry[], out: (line: string) => void): void {
  const cols = (e: Entry) => [
    String(e.row),
    e.project,
    e.status === 'error' || e.status === 'superseded' ? '' : show(e.current),
    e.target === undefined ? '' : show(e.target),
    e.detail ? `${e.status}: ${e.detail}` : e.status,
  ]
  const lines = [['row', 'project', 'current', 'new', 'status'], ...entries.map(cols)]
  const widths = lines[0].map((_, i) => Math.max(...lines.map((l) => l[i].length)))
  for (const l of lines)
    out(
      l
        .map((c, i) => c.padEnd(widths[i]))
        .join('  ')
        .trimEnd(),
    )
}

export async function run(
  qx: QueryExecutor,
  csvText: string,
  { apply, out = console.log }: { apply: boolean; out?: (line: string) => void },
): Promise<Summary> {
  const [header = [], ...records] = parseCsv(csvText)
  const cols = header.map((h) => h.trim())
  const projectCol = cols.indexOf('project')
  const urlCol = cols.indexOf('docsUrl')
  if (projectCol < 0 || urlCol < 0) throw new Error('CSV header must contain project and docsUrl')

  const entries: Entry[] = []
  records.forEach((r, i) => {
    if (r.every((f) => f.trim() === '')) return
    const entry: Entry = { row: i + 2, project: (r[projectCol] ?? '').trim(), status: 'change' }
    const parsed = parseDocsUrl(r[urlCol] ?? '')
    if (!entry.project) {
      Object.assign(entry, { status: 'error', detail: 'empty project' })
    } else if ('error' in parsed) {
      Object.assign(entry, { status: 'error', detail: parsed.error })
    } else {
      entry.target = parsed.url
    }
    entries.push(entry)
  })

  const valid = entries.filter((e) => e.status !== 'error')
  const ids = await findProjectIds(qx, [
    ...new Set(valid.map((e) => (UUID_RE.test(e.project) ? e.project.toLowerCase() : e.project))),
  ])
  const lastRowByProject = new Map<string, number>()
  for (const e of valid) {
    const key = UUID_RE.test(e.project) ? e.project.toLowerCase() : e.project
    e.projectId = ids.get(key)
    if (!e.projectId) {
      Object.assign(e, {
        status: 'error',
        detail: 'project not found among enabled, non-deleted projects',
      })
    } else {
      lastRowByProject.set(e.projectId, e.row)
    }
  }

  // Duplicate rows for one project: the last one wins, earlier ones are skipped.
  for (const e of valid) {
    if (e.status === 'error') continue
    const winner = lastRowByProject.get(e.projectId)
    if (winner !== e.row) {
      Object.assign(e, { status: 'superseded', detail: `replaced by row ${winner}` })
      continue
    }
    const active = await findActiveProjectDocOverride(qx, e.projectId)
    e.current = active ? active.docsUrl : undefined
    if (active && active.docsUrl === e.target) e.status = 'unchanged'
  }

  out(apply ? 'Applying overrides:' : 'Dry run (no changes written; pass --apply to write):')
  printTable(entries, out)

  if (apply) {
    for (const e of entries.filter((x) => x.status === 'change')) {
      try {
        await createProjectDocOverride(qx, {
          projectId: e.projectId,
          docsUrl: e.target,
          submittedBy: SUBMITTED_BY,
        })
        e.status = 'applied'
      } catch (err) {
        Object.assign(e, {
          status: 'failed',
          detail: err instanceof Error ? err.message : `${err}`,
        })
        out(`row ${e.row} (${e.project}) failed: ${e.detail}`)
      }
    }
  }

  const count = (s: Status) => entries.filter((e) => e.status === s).length
  const summary: Summary = {
    change: count('change'),
    unchanged: count('unchanged'),
    superseded: count('superseded'),
    error: count('error'),
    applied: count('applied'),
    failed: count('failed'),
  }
  out(
    `Summary: ${apply ? `${summary.applied} applied` : `${summary.change} would change`}, ` +
      `${summary.unchanged} unchanged, ${summary.superseded} superseded, ` +
      `${summary.error} invalid, ${summary.failed} failed`,
  )
  return summary
}

async function main(): Promise<void> {
  const args = process.argv.slice(2).filter((a) => a !== '--')
  const apply = args.includes('--apply')
  const paths = args.filter((a) => a !== '--apply')
  if (paths.length !== 1 || paths[0].startsWith('--')) {
    console.error('Usage: apply-docs-overrides <file.csv> [--apply]')
    process.exit(2)
  }

  // Dry run connects to the read replica, so it cannot write even by mistake.
  const db = await getDbConnection(apply ? WRITE_DB_CONFIG() : READ_DB_CONFIG())
  const csvText = readFileSync(paths[0], 'utf8')
  const summary = await run(pgpQx(db), csvText, { apply })
  process.exit(apply && summary.error + summary.failed > 0 ? 1 : 0)
}

if (require.main === module) {
  main().catch((err) => {
    console.error(err)
    process.exit(1)
  })
}
