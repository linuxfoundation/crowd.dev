import { getCredential } from '@crowd/connectors'
import {
  mintInstallationToken,
  resolveInstallationId,
} from '@crowd/connectors/src/connectors/github/appToken'
import {
  type ResolvedGithubRepo,
  resolveRepo,
} from '@crowd/connectors/src/connectors/github/discover'
import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import { findActiveNangoMappingForRepo } from '@crowd/data-access-layer/src/integrations'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { findIntegrationRepositoryUrl } from '@crowd/data-access-layer/src/repositories'
import { getServiceLogger } from '@crowd/logging'
import { githubChannelName } from '@crowd/nango'

import { isRenamedRepo } from '../githubLifecycle'
import { lifecycleSkipReasonFor } from '../githubSkipReasons'
import { onboardGithubRepo } from '../onboardGithubRepo'
import { type RepoRef, parseRepoArg } from './repoArg'

const log = getServiceLogger()

interface OnboardReportRow {
  owner: string
  repoName: string
  outcome: 'created' | 'renamed' | 'skipped'
  reason?: string
  unitsInserted?: number
}

function usage(): never {
  log.error('Usage: onboard-nango-repos-fresh <owner/repo | github url> [more repos ...]')
  process.exit(1)
}

function parseArgs(rawArgv: string[]): RepoRef[] {
  const argv = rawArgv.filter((arg) => arg !== '--')
  if (argv.length === 0) {
    usage()
  }
  return argv.map(parseRepoArg)
}

async function onboardOne(
  qx: QueryExecutor,
  token: string,
  repo: RepoRef,
): Promise<OnboardReportRow> {
  const row = { owner: repo.owner, repoName: repo.name }
  const mapping = await findActiveNangoMappingForRepo(qx, repo.owner, repo.name)
  if (!mapping) {
    return { ...row, outcome: 'skipped', reason: 'no mapping' }
  }

  let channel: ResolvedGithubRepo
  try {
    channel = await resolveRepo(token, mapping.owner, mapping.repoName)
  } catch (err) {
    return { ...row, outcome: 'skipped', reason: lifecycleSkipReasonFor('resolve', err) }
  }
  const mappedUrl = githubChannelName(mapping.owner, mapping.repoName)
  if (isRenamedRepo(mappedUrl.toLowerCase(), channel.channelName)) {
    return { ...row, outcome: 'renamed', reason: `${mappedUrl} -> ${channel.channelName}` }
  }
  if (!(await findIntegrationRepositoryUrl(qx, mapping.integrationId, channel.channelName))) {
    return { ...row, outcome: 'skipped', reason: 'no repository row' }
  }
  const result = await onboardGithubRepo(
    qx,
    token,
    mapping.integrationId,
    channel.channelName.toLowerCase(),
    { storedUrl: channel.channelName, resolved: channel },
  )
  switch (result.outcome) {
    case 'created':
      return { ...row, outcome: 'created', unitsInserted: result.unitsInserted }
    case 'renamed':
      return { ...row, outcome: 'renamed', reason: `${mappedUrl} -> ${result.to}` }
    case 'skipped':
      return {
        ...row,
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

    const first = await findActiveNangoMappingForRepo(qx, repos[0].owner, repos[0].name)
    if (!first) {
      throw new Error(`no nango mapping for ${repos[0].owner}/${repos[0].name}`)
    }
    const credential = await getCredential(qx, first.integrationId)
    const { token } = await mintInstallationToken(
      credential,
      await resolveInstallationId(credential),
    )

    const rows: OnboardReportRow[] = []
    for (const repo of repos) {
      rows.push(await onboardOne(qx, token, repo))
    }

    console.table(rows)
    process.exit(rows.some((row) => row.outcome !== 'created') ? 1 : 0)
  } catch (err) {
    log.error(err, 'onboarding failed')
    process.exit(1)
  }
})
