import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import { findActiveNangoMappingForRepo } from '@crowd/data-access-layer/src/integrations'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceLogger } from '@crowd/logging'
import { initNangoCloudClient } from '@crowd/nango'

import { mirrorRepo } from '../mirrorRepo'
import { mirrorSince } from '../mirrorSweep'
import { type RepoRef, parseRepoArg } from './repoArg'

const log = getServiceLogger()

interface MirrorReportRow {
  owner: string
  repoName: string
  outcome: 'mirrored' | 'renamed' | 'skipped'
  reason?: string
  unitsInserted?: number
}

function usage(): never {
  log.error(
    'Usage: mirror-nango-repos-to-shadow-sync-units <owner/repo | github url> [more repos ...]',
  )
  process.exit(1)
}

function parseArgs(rawArgv: string[]): RepoRef[] {
  const argv = rawArgv.filter((arg) => arg !== '--')
  if (argv.length === 0) {
    usage()
  }
  return argv.map(parseRepoArg)
}

async function mirrorOne(
  qx: QueryExecutor,
  repo: RepoRef,
  since: string,
): Promise<MirrorReportRow> {
  const mapping = await findActiveNangoMappingForRepo(qx, repo.owner, repo.name)
  if (!mapping) {
    return { owner: repo.owner, repoName: repo.name, outcome: 'skipped', reason: 'no mapping' }
  }

  const result = await mirrorRepo(
    qx,
    {
      integrationId: mapping.integrationId,
      connectionId: mapping.connectionId,
      owner: mapping.owner,
      repoName: mapping.repoName,
    },
    since,
  )
  switch (result.outcome) {
    case 'mirrored':
      return {
        owner: repo.owner,
        repoName: repo.name,
        outcome: 'mirrored',
        unitsInserted: result.unitsInserted,
      }
    case 'renamed':
      return {
        owner: repo.owner,
        repoName: repo.name,
        outcome: 'renamed',
        reason: `${result.from} -> ${result.to}`,
      }
    case 'skipped':
      return {
        owner: repo.owner,
        repoName: repo.name,
        outcome: 'skipped',
        reason: result.message ? `${result.reason}: ${result.message}` : result.reason,
      }
  }
}

setImmediate(async () => {
  try {
    const repos = parseArgs(process.argv.slice(2))

    const db = await getDbConnection(WRITE_DB_CONFIG())
    const qx = pgpQx(db)

    await initNangoCloudClient()

    const since = mirrorSince(new Date())
    const rows: MirrorReportRow[] = []
    for (const repo of repos) {
      rows.push(await mirrorOne(qx, repo, since))
    }

    console.table(rows)
    process.exit(rows.some((row) => row.outcome === 'skipped') ? 1 : 0)
  } catch (err) {
    log.error(err, 'mirroring failed')
    process.exit(1)
  }
})
