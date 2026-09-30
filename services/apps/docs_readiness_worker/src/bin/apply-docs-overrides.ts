// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
// Bulk-apply reviewed docs overrides (IN-1425). Dry run by default; writes only with --apply.
// Usage: pnpm run script:apply-docs-overrides <file.csv> [--apply]  (CSV columns: project,docsUrl)
// A relative CSV path resolves against services/apps/docs_readiness_worker under pnpm run.
// project is an insightsProjects id or slug; docsUrl is an http(s) URL, or none for "no docs".
// Env (CROWD_DB_*): READ_HOST (dry run), WRITE_HOST (--apply), PORT, DATABASE, USERNAME, PASSWORD
// exit 0 ok, 1 invalid or failed rows or fatal error, 2 usage
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
  let closed = false // a quoted field just ended
  const bad = (why: string) => new Error(`CSV row ${records.length + 1}: ${why}`)

  for (let i = 0; i < s.length; i++) {
    const c = s[i]
    if (quoted) {
      if (c !== '"') field += c
      else if (s[i + 1] === '"') {
        field += '"'
        i++
      } else {
        quoted = false
        closed = true
      }
    } else if (c === ',') {
      record.push(field)
      field = ''
      closed = false
    } else if (c === '\n' || c === '\r') {
      if (c === '\r' && s[i + 1] === '\n') i++
      record.push(field)
      records.push(record)
      record = []
      field = ''
      closed = false
    } else if (closed) throw bad('text after a closing quote')
    else if (c === '"') {
      if (field !== '') throw bad('quote inside an unquoted field')
      quoted = true
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
  const trimmed = value.replace(/^ +| +$/g, '')
  if (trimmed === 'none') return { url: null }
  if (trimmed === '') return { error: 'empty docsUrl (write none for "no docs")' }

  const invalid = {
    error: `invalid URL ${JSON.stringify(trimmed)} (need http(s)://host, no spaces)`,
  }
  if (/[\s\p{Cc}]/u.test(trimmed) || !/^https?:\/\/[^\s/]/i.test(trimmed)) {
    return invalid
  }
  try {
    new URL(trimmed)
  } catch {
    return invalid
  }
  return { url: trimmed }
}

// A key maps to every project whose id or slug equals it; more than one means it is ambiguous.
async function findProjectIds(
  qx: QueryExecutor,
  keys: string[],
): Promise<Map<string, Set<string>>> {
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

  const byKey = new Map<string, Set<string>>()
  for (const r of rows) {
    for (const k of [r.id, r.slug]) byKey.set(k, (byKey.get(k) ?? new Set()).add(r.id))
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
    const parsed = r.length === cols.length ? parseDocsUrl(r[urlCol]) : undefined
    if (!parsed) {
      const why = `expected ${cols.length} fields, got ${r.length} (quote URLs that contain commas)`
      Object.assign(entry, { status: 'error', detail: why })
    } else if (!entry.project) {
      Object.assign(entry, { status: 'error', detail: 'empty project' })
    } else if ('error' in parsed) {
      Object.assign(entry, { status: 'error', detail: parsed.error })
    } else {
      entry.target = parsed.url
    }
    entries.push(entry)
  })

  const keyOf = (e: Entry) => (UUID_RE.test(e.project) ? e.project.toLowerCase() : e.project)
  const named = entries.filter((e) => e.project)
  const ids = await findProjectIds(qx, [...new Set(named.map(keyOf))])
  for (const e of named) {
    const found = ids.get(keyOf(e)) ?? new Set<string>()
    if (found.size === 1) e.projectId = [...found][0]
    if (e.status === 'error') continue
    if (found.size === 0) {
      const why = 'project not found among enabled, non-deleted projects'
      Object.assign(e, { status: 'error', detail: why })
    } else if (found.size > 1) {
      Object.assign(e, { status: 'error', detail: 'ambiguous: key matches more than one project' })
    }
  }

  // One invalid row poisons every row of the same project, so nothing is applied for it.
  const byProject = new Map<string, Entry[]>()
  for (const e of named) {
    const key = e.projectId ?? `key:${keyOf(e)}`
    byProject.set(key, [...(byProject.get(key) ?? []), e])
  }
  for (const group of byProject.values()) {
    const bad = group.filter((e) => e.status === 'error')
    if (bad.length === 0) continue
    for (const e of group.filter((x) => x.status !== 'error')) {
      const why = `project has invalid row ${bad.map((b) => b.row).join(', ')}; nothing applied`
      Object.assign(e, { status: 'error', detail: why })
    }
  }

  const valid = entries.filter((e) => e.status !== 'error')
  const lastRowByProject = new Map<string, number>()
  for (const e of valid) lastRowByProject.set(e.projectId, e.row)

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
    out('Final statuses:')
    printTable(entries, out)
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
  if (apply) out('Re-run the same file; applied rows show as unchanged and are skipped.')
  return summary
}

type Connect = (apply: boolean) => Promise<QueryExecutor>

// Dry run connects to the read replica, so it cannot write even by mistake.
const connectDb: Connect = async (apply) =>
  pgpQx(await getDbConnection(apply ? WRITE_DB_CONFIG() : READ_DB_CONFIG()))

// Returns the process exit code.
export async function main(argv: string[], connect: Connect = connectDb): Promise<number> {
  const args = argv.filter((a) => a !== '--')
  const apply = args.includes('--apply')
  const paths = args.filter((a) => a !== '--apply')
  if (paths.length !== 1 || paths[0].startsWith('--')) {
    console.error('Usage: apply-docs-overrides <file.csv> [--apply]')
    return 2
  }

  // Read the file first so a missing one never opens a DB connection.
  const csvText = readFileSync(paths[0], 'utf8')
  const summary = await run(await connect(apply), csvText, { apply })
  return summary.error + summary.failed > 0 ? 1 : 0
}

if (require.main === module) {
  main(process.argv.slice(2)).then(
    (code) => process.exit(code),
    (err) => {
      console.error(err)
      process.exit(1)
    },
  )
}
