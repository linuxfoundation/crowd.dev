/**
 * Gerrit Remote vs Local Count Validator
 *
 * Compares per-activity-type counts for a Gerrit repo between the upstream
 * Gerrit REST API (source of truth) and local Tinybird storage.
 *
 * Usage:
 *   pnpm tsx src/bin/validate-gerrit-counts.ts <gerrit-repo-url>
 *
 * Edit the CONFIG block below before running.
 * Every value falls back to the corresponding env var if left empty.
 */

import { writeFile } from 'node:fs/promises'
import pgPromise from 'pg-promise'

// ─── config ───────────────────────────────────────────────────────────────────
// Edit these values or export the corresponding env vars before running.

const CONFIG = {
  pg: {
    host: process.env.CROWD_DB_READ_HOST || '',
    port: parseInt(process.env.CROWD_DB_PORT || '5432'),
    user: process.env.CROWD_DB_USERNAME || '',
    password: process.env.CROWD_DB_PASSWORD || '',
    database: process.env.CROWD_DB_DATABASE || '',
    ssl: (process.env.CROWD_DB_SSL || 'true') !== 'false',
  },
  tinybird: {
    baseUrl: (process.env.CROWD_TINYBIRD_BASE_URL || '').replace(/\/$/, ''),
    token: process.env.CROWD_TINYBIRD_ACTIVITIES_TOKEN || '',
  },
  gerritPageSize: 500,
  verbose: false,
  outputDir: '.', // directory for the JSON artifact
}

// ─── types ────────────────────────────────────────────────────────────────────

interface RemoteCounts {
  'changeset-created': number
  'changeset-merged': number
  'changeset-abandoned': number
  'changeset_comment-created': number
  'patchset-created': number
  'patchset_approval-created': number
  pagesWalked: number
  changesProcessed: number
  skipErrors: number
}

interface TypeResult {
  type: string
  remote: number | 'n/a'
  local: number
  diff: number | 'n/a'
  note?: string
}

interface Report {
  url: string
  channelUrl: string
  integrationId: string
  repoName: string
  runAt: string
  durationMs: number
  results: TypeResult[]
  gerritStats: { pagesWalked: number; changesProcessed: number; skipErrors: number }
}

// ─── startup ──────────────────────────────────────────────────────────────────

function mustHave(value: string, label: string): string {
  if (!value) throw new Error(`Missing required config: ${label}`)
  return value
}

function validateConfig(): void {
  mustHave(CONFIG.pg.host, 'pg.host / CROWD_DB_READ_HOST')
  mustHave(CONFIG.pg.user, 'pg.user / CROWD_DB_USERNAME')
  mustHave(CONFIG.pg.password, 'pg.password / CROWD_DB_PASSWORD')
  mustHave(CONFIG.pg.database, 'pg.database / CROWD_DB_DATABASE')
  mustHave(CONFIG.tinybird.baseUrl, 'tinybird.baseUrl / CROWD_TINYBIRD_BASE_URL')
  mustHave(CONFIG.tinybird.token, 'tinybird.token / CROWD_TINYBIRD_ACTIVITIES_TOKEN')
}

// ─── url utils ────────────────────────────────────────────────────────────────

function parseRepoUrl(rawUrl: string): { plainUrl: string; channelUrl: string } {
  let plain = rawUrl.trim().replace(/\/$/, '')

  // Normalise /q/project: form → plain form
  // e.g. https://review.opendev.org/q/project:openstack/nova
  //   → https://review.opendev.org/openstack/nova
  const qMatch = plain.match(/^(https?:\/\/[^/]+(?:\/[^/]+)*)\/q\/project:(.+)$/)
  if (qMatch) plain = `${qMatch[1]}/${qMatch[2]}`

  // Derive channel URL following repos_to_channels.pipe logic:
  //   /r/{project}      → /r/q/project:{project}
  //   /gerrit/{project} → /gerrit/q/project:{project}
  //   /{project}        → /q/project:{project}
  const { origin, pathname } = new URL(plain)
  let channelUrl: string
  if (pathname.startsWith('/r/')) {
    channelUrl = `${origin}/r/q/project:${pathname.slice(3)}`
  } else if (pathname.startsWith('/gerrit/')) {
    channelUrl = `${origin}/gerrit/q/project:${pathname.slice(8)}`
  } else {
    channelUrl = `${origin}/q/project:${pathname.slice(1)}`
  }

  return { plainUrl: plain, channelUrl }
}

// ─── db validation ────────────────────────────────────────────────────────────

async function validateInDb(
  db: pgPromise.IDatabase<object>,
  plainUrl: string,
): Promise<{ integrationId: string; orgUrl: string; repoName: string }> {
  const repo = await db.oneOrNone<{ sourceIntegrationId: string }>(
    `SELECT "sourceIntegrationId" FROM repositories WHERE url = $1 AND "deletedAt" IS NULL`,
    [plainUrl],
  )
  if (!repo) {
    throw new Error(
      `No repositories row found for: ${plainUrl}\n` +
        `Tip: use the plain form without /q/project: (e.g. https://host/org/repo)`,
    )
  }

  const integration = await db.oneOrNone<{
    id: string
    settings: { remote: { orgURL: string; repoNames: string[] } }
  }>(
    `SELECT id, settings FROM integrations WHERE id = $1 AND platform = 'gerrit' AND "deletedAt" IS NULL`,
    [repo.sourceIntegrationId],
  )
  if (!integration) {
    throw new Error(
      `Integration ${repo.sourceIntegrationId} not found or is not platform='gerrit'.`,
    )
  }

  const { orgURL, repoNames } = integration.settings.remote
  const repoName = plainUrl.replace(`${orgURL.replace(/\/$/, '')}/`, '')

  if (!repoNames.includes(repoName)) {
    throw new Error(
      `Repo '${repoName}' not found in integration repoNames.\n` +
        `Available: [${repoNames.slice(0, 10).join(', ')}${repoNames.length > 10 ? ', …' : ''}]`,
    )
  }

  return { integrationId: integration.id, orgUrl: orgURL, repoName }
}

// ─── gerrit rest ──────────────────────────────────────────────────────────────

function stripXssi(text: string): string {
  // Gerrit prepends )]}\' + newline to prevent XSSI
  return text.startsWith(")]}'") ? text.slice(5) : text
}

async function fetchChangePage(url: string): Promise<unknown[]> {
  const resp = await fetch(url)
  if (!resp.ok) throw new Error(`Gerrit ${resp.status}: GET ${url}`)
  return JSON.parse(stripXssi(await resp.text())) as unknown[]
}

async function walkGerritChanges(orgUrl: string, repoName: string): Promise<RemoteCounts> {
  const counts: RemoteCounts = {
    'changeset-created': 0,
    'changeset-merged': 0,
    'changeset-abandoned': 0,
    'changeset_comment-created': 0,
    'patchset-created': 0,
    'patchset_approval-created': 0,
    pagesWalked: 0,
    changesProcessed: 0,
    skipErrors: 0,
  }

  const base = orgUrl.replace(/\/$/, '')
  const options = 'o=MESSAGES&o=ALL_REVISIONS&o=DETAILED_LABELS'
  let start = 0

  // eslint-disable-next-line no-constant-condition
  while (true) {
    const url = `${base}/changes/?q=project:${encodeURIComponent(repoName)}&n=${CONFIG.gerritPageSize}&S=${start}&${options}`
    if (CONFIG.verbose) console.log(`  [gerrit] page S=${start}`)

    let page: unknown[]
    try {
      page = await fetchChangePage(url)
    } catch (err) {
      console.error(`  [gerrit] error at S=${start}: ${err}`)
      counts.skipErrors++
      break
    }

    if (!Array.isArray(page) || page.length === 0) break

    counts.pagesWalked++
    const hasMore = (page[page.length - 1] as Record<string, unknown>)._more_changes === true

    for (const raw of page) {
      const change = raw as Record<string, unknown>
      try {
        counts['changeset-created']++
        if (change['status'] === 'MERGED') counts['changeset-merged']++
        if (change['status'] === 'ABANDONED') counts['changeset-abandoned']++

        counts['changeset_comment-created'] += ((change['messages'] as unknown[]) ?? []).length

        counts['patchset-created'] += Object.keys(
          (change['revisions'] as Record<string, unknown>) ?? {},
        ).length

        for (const labelData of Object.values(
          (change['labels'] as Record<string, { all?: { value?: number }[] }>) ?? {},
        )) {
          for (const vote of labelData.all ?? []) {
            if (vote.value !== undefined && vote.value !== 0) {
              counts['patchset_approval-created']++
            }
          }
        }

        counts.changesProcessed++
      } catch {
        counts.skipErrors++
      }
    }

    if (CONFIG.verbose) console.log(`  [gerrit] cumulative: ${counts.changesProcessed} changes`)
    if (!hasMore) break
    start += CONFIG.gerritPageSize
  }

  return counts
}

// ─── tinybird ─────────────────────────────────────────────────────────────────

async function tinybirdSql<T>(sql: string): Promise<T> {
  const resp = await fetch(`${CONFIG.tinybird.baseUrl}/v0/sql`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${CONFIG.tinybird.token}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ q: sql }),
  })
  if (!resp.ok) {
    const body = await resp.text()
    throw new Error(`Tinybird ${resp.status}: ${body}`)
  }
  return resp.json() as Promise<T>
}

async function getLocalCounts(channelUrl: string): Promise<Record<string, number>> {
  const escaped = channelUrl.replace(/'/g, "\\'")
  const sql =
    `SELECT type, count() AS cnt ` +
    `FROM activityRelations_deduplicated_cleaned_bucket_union ` +
    `WHERE platform = 'gerrit' AND channel = '${escaped}' ` +
    `GROUP BY type FORMAT JSON`

  const result = await tinybirdSql<{ data: Array<{ type: string; cnt: number }> }>(sql)
  const counts: Record<string, number> = {}
  for (const row of result.data ?? []) {
    counts[row.type] = Number(row.cnt)
  }
  return counts
}

// ─── report ───────────────────────────────────────────────────────────────────

const IN_SCOPE: Array<keyof Omit<RemoteCounts, 'pagesWalked' | 'changesProcessed' | 'skipErrors'>> =
  [
    'changeset-created',
    'changeset-merged',
    'changeset-abandoned',
    'changeset_comment-created',
    'patchset-created',
    'patchset_approval-created',
  ]

const EXCLUDED = new Set(['changeset-new', 'changeset-closed', 'patchset_comment-created'])

function buildReport(
  url: string,
  channelUrl: string,
  integrationId: string,
  repoName: string,
  remote: RemoteCounts,
  local: Record<string, number>,
  durationMs: number,
): Report {
  const results: TypeResult[] = []

  for (const type of IN_SCOPE) {
    const r = remote[type]
    const l = local[type] ?? 0
    results.push({
      type,
      remote: r,
      local: l,
      diff: l - r,
      note:
        type === 'patchset_approval-created'
          ? 'remote = current-state votes only (non-historical estimate)'
          : undefined,
    })
  }

  // Surface excluded types that unexpectedly have local rows
  for (const type of EXCLUDED) {
    const l = local[type] ?? 0
    if (l > 0) {
      const reason =
        type === 'changeset-new'
          ? 'dead type (registry-deleted by migration V1759325136)'
          : type === 'changeset-closed'
            ? 'derivation logic lives in external Nango sync only'
            : 'out of scope (Tier 3)'
      results.push({ type, remote: 'n/a', local: l, diff: 'n/a', note: reason })
    }
  }

  // Any other unexpected local types
  for (const type of Object.keys(local)) {
    if (!(IN_SCOPE as string[]).includes(type) && !EXCLUDED.has(type)) {
      results.push({ type, remote: 'n/a', local: local[type], diff: 'n/a', note: 'unknown type' })
    }
  }

  return {
    url,
    channelUrl,
    integrationId,
    repoName,
    runAt: new Date().toISOString(),
    durationMs,
    results,
    gerritStats: {
      pagesWalked: remote.pagesWalked,
      changesProcessed: remote.changesProcessed,
      skipErrors: remote.skipErrors,
    },
  }
}

function printReport(report: Report): void {
  const W = 38
  const N = 11
  const header = 'Type'.padEnd(W) + 'Remote'.padStart(N) + 'Local'.padStart(N) + 'Δ'.padStart(N)

  console.log('')
  console.log(`Repo        ${report.url}`)
  console.log(`Channel     ${report.channelUrl}`)
  console.log(`Integration ${report.integrationId}`)
  console.log(`Run at      ${report.runAt}  (${(report.durationMs / 1000).toFixed(1)}s)`)
  console.log(
    `Gerrit      ${report.gerritStats.changesProcessed} changes / ` +
      `${report.gerritStats.pagesWalked} pages` +
      (report.gerritStats.skipErrors ? ` / ${report.gerritStats.skipErrors} skip errors` : ''),
  )
  console.log('')
  console.log(header)
  console.log('─'.repeat(header.length))

  for (const r of report.results) {
    const remote = r.remote === 'n/a' ? 'n/a' : (r.remote as number).toLocaleString()
    const local = r.local.toLocaleString()
    const diff =
      r.diff === 'n/a'
        ? 'n/a'
        : `${(r.diff as number) > 0 ? '+' : ''}${(r.diff as number).toLocaleString()}`
    console.log(r.type.padEnd(W) + remote.padStart(N) + local.padStart(N) + diff.padStart(N))
    if (r.note) console.log(`${''.padEnd(W + 1)}* ${r.note}`)
  }
  console.log('')
}

// ─── main ─────────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  const rawUrl = process.argv[2]
  if (!rawUrl) {
    console.error('Usage: pnpm tsx src/bin/validate-gerrit-counts.ts <gerrit-repo-url>')
    process.exit(1)
  }

  validateConfig()
  const startMs = Date.now()

  console.log(`\nValidating Gerrit counts for: ${rawUrl}`)

  const { plainUrl, channelUrl } = parseRepoUrl(rawUrl)
  if (plainUrl !== rawUrl.trim()) console.log(`Normalized to: ${plainUrl}`)
  console.log(`Channel URL:   ${channelUrl}`)

  // ── DB lookup ──
  console.log(`\nLooking up in CDP database...`)
  const pgp = pgPromise()
  const db = pgp({
    host: CONFIG.pg.host,
    port: CONFIG.pg.port,
    user: CONFIG.pg.user,
    password: CONFIG.pg.password,
    database: CONFIG.pg.database,
    ssl: CONFIG.pg.ssl ? { rejectUnauthorized: false } : false,
  })

  let integrationId: string
  let orgUrl: string
  let repoName: string

  try {
    const info = await validateInDb(db, plainUrl)
    integrationId = info.integrationId
    orgUrl = info.orgUrl
    repoName = info.repoName
    console.log(`  Integration: ${integrationId}`)
    console.log(`  Org URL:     ${orgUrl}`)
    console.log(`  Repo:        ${repoName}`)
  } finally {
    pgp.end()
  }

  // ── Remote counts ──
  console.log(`\nFetching remote counts from Gerrit (page size: ${CONFIG.gerritPageSize})...`)
  const remote = await walkGerritChanges(orgUrl, repoName)
  console.log(`  ${remote.changesProcessed} changes in ${remote.pagesWalked} pages`)
  if (remote.skipErrors > 0) console.warn(`  ⚠ ${remote.skipErrors} skip errors`)

  // ── Local counts ──
  console.log(`\nFetching local counts from Tinybird...`)
  const local = await getLocalCounts(channelUrl)
  console.log(`  ${Object.keys(local).length} activity types found locally`)

  // ── Report ──
  const durationMs = Date.now() - startMs
  const report = buildReport(plainUrl, channelUrl, integrationId, repoName, remote, local, durationMs)
  printReport(report)

  const slug = `${new URL(plainUrl).hostname}-${repoName.replace(/\//g, '-')}`
  const ts = new Date().toISOString().replace(/[:.]/g, '-')
  const outputPath = `${CONFIG.outputDir}/validation-${slug}-${ts}.json`
  await writeFile(outputPath, JSON.stringify(report, null, 2), 'utf8')
  console.log(`Report saved: ${outputPath}`)
}

main().catch((err) => {
  console.error(`\nError: ${err instanceof Error ? err.message : String(err)}`)
  process.exit(1)
})
