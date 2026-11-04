import { Context } from '@temporalio/activity'

import {
  findRepoIdsByUrl,
  updateRepoScorecards,
  upsertRepoScorecardChecks,
} from '@crowd/data-access-layer/src/osspckgs/repos'
import { getServiceChildLogger } from '@crowd/logging'
import { SlackChannel, SlackPersona, sendSlackNotification } from '@crowd/slack'

import { getScorecardConfig } from '../config'
import { getCdpDb, getPackagesDb } from '../db'
import { canonicalRepoUrl, parseRepoUrl } from '../deps-dev/canonicalRepoUrl'
import { buildInsert } from '../deps-dev/sqlUtils'
import { mapWithConcurrency } from '../utils/concurrency'
import { fetchScorecard } from './scorecardApi'
import { ScorecardRows, scorecardProjectPath, toScorecardRows } from './scorecardRows'
import { ScorecardPageCounts, emptyScorecardCounts } from './types'

const log = getServiceChildLogger('syncGithubRepos')
const ingestLog = getServiceChildLogger('ingestScorecard')

const CDP_GITHUB_REPOS_FROM = `
    FROM public.repositories r
    JOIN public.integrations i ON r."sourceIntegrationId" = i.id
    WHERE i.platform IN ('github', 'github-nango')
      AND r."deletedAt" IS NULL
      AND r.archived = false
      AND r.excluded = false
`

type RepoRow = { url: string; host: string; owner: string; name: string }

export function toRepoRow(url: string): RepoRow | null {
  let parsed: URL
  try {
    parsed = new URL(url)
  } catch {
    return null
  }
  if (parsed.hostname !== 'github.com') return null
  const canonical = canonicalRepoUrl('GITHUB', url)
  if (!canonical) return null
  const { host, owner, name } = parseRepoUrl(canonical)
  return { url: canonical, host, owner, name }
}

export async function syncGithubRepos(): Promise<{ inserted: number }> {
  const cdpDb = await getCdpDb()
  const pkgsDb = await getPackagesDb()

  const cdpRows = (await cdpDb.select(`SELECT r.url ${CDP_GITHUB_REPOS_FROM}`)) as Array<{
    url: string
  }>

  if (cdpRows.length === 0) {
    log.info('No GitHub repos found in CDP db')
    return { inserted: 0 }
  }

  log.info({ count: cdpRows.length }, 'Seeding GitHub repos into packages-db')

  const rows: RepoRow[] = []
  for (const { url } of cdpRows) {
    const row = toRepoRow(url)
    if (row) rows.push(row)
  }

  if (rows.length === 0) {
    log.warn('All CDP rows failed URL parsing — no repos seeded')
    return { inserted: 0 }
  }

  const sql =
    buildInsert('repos', ['url', 'host', 'owner', 'name'], rows) + '\nON CONFLICT (url) DO NOTHING'

  const inserted = await pkgsDb.result(sql)
  log.info({ inserted }, 'LF GitHub repos seeded into packages-db')
  return { inserted }
}

export interface IngestScorecardPageInput {
  afterId: string | null
  limit: number
}

export interface IngestScorecardPageOutput {
  nextAfterId: string | null
  counts: ScorecardPageCounts
}

type ScorecardTarget = { url: string; projectPath: string; repoId: number }

async function listCdpGithubRepos(
  input: IngestScorecardPageInput,
): Promise<Array<{ id: string; url: string }>> {
  const cdpDb = await getCdpDb()
  return (await cdpDb.select(
    `
    SELECT r.id, r.url
    ${CDP_GITHUB_REPOS_FROM}
      AND ($(afterId)::uuid IS NULL OR r.id > $(afterId)::uuid)
    ORDER BY r.id
    LIMIT $(limit)
    `,
    { afterId: input.afterId, limit: input.limit },
  )) as Array<{ id: string; url: string }>
}

async function resolveTargets(
  urls: string[],
  counts: ScorecardPageCounts,
): Promise<ScorecardTarget[]> {
  const pkgsDb = await getPackagesDb()
  const candidates = urls
    .map((url) => ({
      url,
      projectPath: scorecardProjectPath(url),
      canonicalUrl: toRepoRow(url)?.url,
    }))
    .filter((c): c is { url: string; projectPath: string; canonicalUrl: string } =>
      Boolean(c.projectPath && c.canonicalUrl),
    )
  counts.unmapped += urls.length - candidates.length

  const repoIds = await findRepoIdsByUrl(
    pkgsDb,
    candidates.map((c) => c.canonicalUrl),
  )
  const targetsByRepoId = new Map<number, ScorecardTarget>()
  for (const c of candidates) {
    const repoId = repoIds.get(c.canonicalUrl)
    if (repoId === undefined) {
      counts.unmapped++
      continue
    }
    targetsByRepoId.set(repoId, { url: c.url, projectPath: c.projectPath, repoId })
  }
  return [...targetsByRepoId.values()]
}

export async function ingestScorecardPage(
  input: IngestScorecardPageInput,
): Promise<IngestScorecardPageOutput> {
  const cdpRows = await listCdpGithubRepos(input)
  const counts = emptyScorecardCounts()
  if (cdpRows.length === 0) return { nextAfterId: null, counts }
  const nextAfterId = cdpRows.length < input.limit ? null : cdpRows[cdpRows.length - 1].id

  const targets = await resolveTargets(
    cdpRows.map((r) => r.url),
    counts,
  )
  const cfg = getScorecardConfig()
  const rowsByRepoId = new Map<number, ScorecardRows>()
  const notFoundSample: string[] = []

  await mapWithConcurrency(targets, cfg.concurrency, async (target) => {
    const heartbeat = () => Context.current().heartbeat(target.url)
    heartbeat()
    const result = await fetchScorecard(target.projectPath, { ...cfg, onRetryWait: heartbeat })
    counts.fetched++

    if (result.kind === 'NOT_FOUND') {
      counts.notFound++
      if (notFoundSample.length < 5) notFoundSample.push(target.url)
      return
    }
    if (result.kind !== 'OK') {
      counts.failed++
      ingestLog.warn(
        { url: target.url, kind: result.kind, statusCode: result.statusCode },
        `scorecard fetch failed: ${result.message}`,
      )
      return
    }
    const rows = toScorecardRows(target.repoId, result.payload)
    if (!rows) {
      counts.failed++
      ingestLog.warn({ url: target.url, date: result.payload.date }, 'scorecard payload unusable')
      return
    }
    counts.scored++
    rowsByRepoId.set(target.repoId, rows)
  })

  if (counts.fetched > 0 && counts.failed * 2 >= counts.fetched) {
    throw new Error(
      `scorecard api failed for ${counts.failed} of ${counts.fetched} repos in the page`,
    )
  }

  if (rowsByRepoId.size > 0) {
    const pkgsDb = await getPackagesDb()
    const rows = [...rowsByRepoId.values()]
    await pkgsDb.tx(async (tx) => {
      await updateRepoScorecards(
        tx,
        rows.map((r) => r.repo),
      )
      await upsertRepoScorecardChecks(
        tx,
        rows.flatMap((r) => r.checks),
      )
    })
  }

  ingestLog.info({ ...counts, notFoundSample, afterId: input.afterId }, 'scorecard page ingested')
  return { nextAfterId, counts }
}

export async function notifyScorecardIngestFailed(input: { message: string }): Promise<void> {
  sendSlackNotification(
    SlackChannel.CDP_AKRITES_ALERTS,
    SlackPersona.CRITICAL_ALERTER,
    ':warning: scorecard REST ingest failed — bootstrap continued with ranking',
    [
      {
        title: 'Action',
        text: `Pages committed before the failure are kept; the rest refreshes on the next run. ${input.message}`,
      },
    ],
  )
}
