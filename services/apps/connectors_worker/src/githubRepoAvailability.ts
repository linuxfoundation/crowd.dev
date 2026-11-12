import { resolveRepo } from '@crowd/connectors/src/connectors/github/discover'
import { parseRepoChannel } from '@crowd/connectors/src/connectors/github/paging'
import {
  REPOSITORY_NOT_FOUND_ERROR_CLASS,
  listSyncUnitChannelNamesByErrorClass,
} from '@crowd/data-access-layer/src/connectors'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import {
  getReposForGithubIntegration,
  listUnavailableRepoUrlsForIntegration,
} from '@crowd/data-access-layer/src/repositories'

import { chunk } from './chunk'
import type { TokenSource } from './githubInstallationToken'
import {
  type ILifecycleSkip,
  LIFECYCLE_RESOLVE_CONCURRENCY,
  type LifecycleSkipReason,
  githubRepoKey,
} from './githubLifecycle'
import { isGoneError, lifecycleSkipReasonFor } from './githubSkipReasons'

export interface IRepoAvailabilityProbes {
  gone: string[]
  reachable: string[]
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

function unprobed(urls: string[], reason: LifecycleSkipReason, message?: string): Probe[] {
  return urls.map((url) => ({ url, state: 'unknown', skip: { url, reason, message } }))
}

async function probeAll(
  token: TokenSource,
  urls: string[],
  onProgress: () => void,
): Promise<Probe[]> {
  const probes: Probe[] = []
  const groups = chunk(urls, LIFECYCLE_RESOLVE_CONCURRENCY)
  for (const [index, group] of groups.entries()) {
    const remaining = () => groups.slice(index + 1).flat()
    let current: string
    try {
      current = await token()
    } catch (err) {
      const reason = lifecycleSkipReasonFor('mint', err)
      return [...probes, ...unprobed([...group, ...remaining()], reason, errorMessage(err))]
    }
    const results = await Promise.all(group.map((url) => probe(current, url)))
    probes.push(...results)
    onProgress()
    if (results.some((p) => p.state === 'unknown' && p.skip.reason === 'rate_limited')) {
      return [...probes, ...unprobed(remaining(), 'rate_limited', 'not probed after a rate limit')]
    }
  }
  return probes
}

async function candidateUrls(
  qx: QueryExecutor,
  integrationId: string,
): Promise<{ flagged: Set<string>; candidates: string[] }> {
  const [repos, unavailable, suspects] = await Promise.all([
    getReposForGithubIntegration(qx, integrationId),
    listUnavailableRepoUrlsForIntegration(qx, integrationId),
    listSyncUnitChannelNamesByErrorClass(qx, integrationId, REPOSITORY_NOT_FOUND_ERROR_CLASS),
  ])
  const storedUrlsByKey = new Map<string, string[]>()
  for (const { url } of repos) {
    const key = githubRepoKey(url)
    if (key !== null) {
      storedUrlsByKey.set(key, [...(storedUrlsByKey.get(key) ?? []), url])
    }
  }
  const ownedSuspects = suspects.flatMap((channelName) => {
    const key = githubRepoKey(channelName)
    return key === null ? [] : (storedUrlsByKey.get(key) ?? [])
  })
  return {
    flagged: new Set(unavailable),
    candidates: [...new Set([...unavailable, ...ownedSuspects])],
  }
}

export async function probeGithubRepoAvailability(
  qx: QueryExecutor,
  token: TokenSource,
  integrationId: string,
  onProgress: () => void,
): Promise<IRepoAvailabilityProbes> {
  const { flagged, candidates } = await candidateUrls(qx, integrationId)
  if (candidates.length === 0) {
    return { gone: [], reachable: [], skipped: [] }
  }
  const probes = await probeAll(token, candidates, onProgress)
  return {
    gone: probes.filter((p) => p.state === 'gone' && !flagged.has(p.url)).map((p) => p.url),
    reachable: probes
      .filter((p) => p.state === 'reachable' && flagged.has(p.url))
      .map((p) => p.url),
    skipped: probes.flatMap((p) => (p.state === 'unknown' ? [p.skip] : [])),
  }
}
