import type { IUnmirroredNangoRepo } from '@crowd/data-access-layer/src/integrations'
import { githubChannelName } from '@crowd/nango'

import { isRenamedRepo } from './githubLifecycle'
import { githubResolveSkipReason } from './githubSkipReasons'

export { MIRROR_RESOLVE_CONCURRENCY, chunk, mirrorRoom } from './mirrorSweepLimits'

export type MirrorSkipReason =
  | 'no_installation_id'
  | 'token_mint_failed'
  | 'repo_not_found'
  | 'repo_renamed'
  | 'rate_limited'
  | 'github_error'
  | 'nango_error'
  | 'already_mirrored'

export interface IMirrorCandidate {
  integrationId: string
  connectionId: string
  owner: string
  repoName: string
}

export interface IMirrorSweepInput {
  batchSize: number
  shadowCap: number
}

export interface IMirrorSweepSummary {
  batchSize: number
  shadowCap: number
  inShadow: number
  candidates: number
  attempted: number
  mirrored: number
  skipped: number
  unitsInserted: number
  failed: boolean
  durationMs: number
}

export type MirrorBatchResult = Pick<
  IMirrorSweepSummary,
  'candidates' | 'attempted' | 'mirrored' | 'skipped' | 'unitsInserted' | 'failed'
>

export type MirrorRepoResult =
  | { outcome: 'mirrored'; unitsInserted: number }
  | { outcome: 'skipped'; reason: MirrorSkipReason; message?: string }

export type MirrorStage = 'nango' | 'mint' | 'resolve'

const LOOKBACK_MS = 48 * 60 * 60 * 1000

export function mirrorSince(now: Date): string {
  const lookback = new Date(now.getTime() - LOOKBACK_MS)
  return new Date(
    Date.UTC(lookback.getUTCFullYear(), lookback.getUTCMonth(), lookback.getUTCDate()),
  ).toISOString()
}

const channelKey = (owner: string, repoName: string): string =>
  githubChannelName(owner, repoName).toLowerCase()

const isActive = (repo: IUnmirroredNangoRepo): boolean =>
  repo.enabled && !repo.archived && !repo.excluded

export function orderMirrorCandidates(
  repos: IUnmirroredNangoRepo[],
  activityByChannel: Map<string, number>,
): IMirrorCandidate[] {
  const activityOf = (repo: IUnmirroredNangoRepo): number =>
    activityByChannel.get(channelKey(repo.owner, repo.repoName)) ?? 0

  return [...repos]
    .sort(
      (a, b) =>
        Number(isActive(b)) - Number(isActive(a)) ||
        activityOf(b) - activityOf(a) ||
        a.owner.localeCompare(b.owner) ||
        a.repoName.localeCompare(b.repoName),
    )
    .map(({ integrationId, connectionId, owner, repoName }) => ({
      integrationId,
      connectionId,
      owner,
      repoName,
    }))
}

export function isRenamedChannel(candidate: IMirrorCandidate, channelName: string): boolean {
  return isRenamedRepo(channelKey(candidate.owner, candidate.repoName), channelName)
}

export function mirrorSkipReasonFor(stage: MirrorStage, err: unknown): MirrorSkipReason {
  switch (stage) {
    case 'nango':
      return 'nango_error'
    case 'mint':
      return 'token_mint_failed'
    case 'resolve':
      return githubResolveSkipReason(err)
  }
}
