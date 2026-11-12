import { resolveRepo } from '@crowd/connectors/src/connectors/github/discover'
import { parseRepoChannel } from '@crowd/connectors/src/connectors/github/paging'
import {
  REPOSITORY_NOT_FOUND_ERROR_CLASS,
  listSyncUnitChannelNamesByErrorClass,
} from '@crowd/data-access-layer/src/connectors'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import {
  clearRepositoriesUnavailable,
  listUnavailableRepoUrlsForIntegration,
  markRepositoriesUnavailable,
} from '@crowd/data-access-layer/src/repositories'

import { chunk } from './chunk'
import type { TokenSource } from './githubInstallationToken'
import { type ILifecycleSkip, LIFECYCLE_RESOLVE_CONCURRENCY } from './githubLifecycle'
import { isGoneError, lifecycleSkipReasonFor } from './githubSkipReasons'

export interface IRepoAvailabilityResult {
  flagged: string[]
  cleared: string[]
  skipped: ILifecycleSkip[]
}

type Probe = { url: string } & (
  | { state: 'reachable' | 'gone' }
  | { state: 'unknown'; skip: ILifecycleSkip }
)

function errorMessage(err: unknown): string | undefined {
  return err instanceof Error ? err.message : undefined
}

async function probe(token: string, url: string): Promise<Probe> {
  try {
    const { owner, repo } = parseRepoChannel(url)
    await resolveRepo(token, owner, repo)
    return { url, state: 'reachable' }
  } catch (err) {
    if (isGoneError(err)) {
      return { url, state: 'gone' }
    }
    const reason = lifecycleSkipReasonFor('resolve', err)
    return { url, state: 'unknown', skip: { url, reason, message: errorMessage(err) } }
  }
}

async function probeAll(token: TokenSource, urls: string[]): Promise<Probe[]> {
  const probes: Probe[] = []
  for (const group of chunk(urls, LIFECYCLE_RESOLVE_CONCURRENCY)) {
    const current = await token()
    const results = await Promise.all(group.map((url) => probe(current, url)))
    probes.push(...results)
    if (results.some((p) => p.state === 'unknown' && p.skip.reason === 'rate_limited')) {
      break
    }
  }
  return probes
}

export async function syncGithubRepoAvailability(
  qx: QueryExecutor,
  token: TokenSource,
  integrationId: string,
): Promise<IRepoAvailabilityResult> {
  const [unavailable, suspects] = await Promise.all([
    listUnavailableRepoUrlsForIntegration(qx, integrationId),
    listSyncUnitChannelNamesByErrorClass(qx, integrationId, REPOSITORY_NOT_FOUND_ERROR_CLASS),
  ])
  const candidates = [...new Set([...unavailable, ...suspects])]
  if (candidates.length === 0) {
    return { flagged: [], cleared: [], skipped: [] }
  }

  let probes: Probe[]
  try {
    probes = await probeAll(token, candidates)
  } catch (err) {
    const reason = lifecycleSkipReasonFor('mint', err)
    return {
      flagged: [],
      cleared: [],
      skipped: candidates.map((url) => ({ url, reason, message: errorMessage(err) })),
    }
  }

  const flaggedBefore = new Set(unavailable)
  const gone = probes.filter((p) => p.state === 'gone' && !flaggedBefore.has(p.url))
  const back = probes.filter((p) => p.state === 'reachable' && flaggedBefore.has(p.url))
  const [flagged, cleared] = await Promise.all([
    markRepositoriesUnavailable(
      qx,
      gone.map((p) => p.url),
    ),
    clearRepositoriesUnavailable(
      qx,
      back.map((p) => p.url),
    ),
  ])
  return {
    flagged,
    cleared,
    skipped: probes.flatMap((p) => (p.state === 'unknown' ? [p.skip] : [])),
  }
}
