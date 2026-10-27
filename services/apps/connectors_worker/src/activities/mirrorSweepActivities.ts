import { Context } from '@temporalio/activity'

import { countShadowRepos as countShadowReposInDb } from '@crowd/data-access-layer/src/connectors'
import { listUnmirroredNangoRepos } from '@crowd/data-access-layer/src/integrations'
import { type QueryExecutor, dbStoreQx } from '@crowd/data-access-layer/src/queryExecutor'
import { TinybirdClient } from '@crowd/database'
import { initNangoCloudClient } from '@crowd/nango'

import { readGithubChannelActivity } from '../githubChannelActivity'
import { svc } from '../main'
import { mirrorRepo } from '../mirrorRepo'
import {
  type IMirrorCandidate,
  type IMirrorSweepSummary,
  MIRROR_RESOLVE_CONCURRENCY,
  type MirrorBatchResult,
  chunk,
  mirrorSince,
  orderMirrorCandidates,
} from '../mirrorSweep'

export async function countShadowRepos(): Promise<number> {
  return countShadowReposInDb(dbStoreQx(svc.postgres.writer))
}

const HEARTBEAT_INTERVAL_MS = 30_000

async function mirrorCandidates(
  qx: QueryExecutor,
  candidates: IMirrorCandidate[],
  since: string,
  result: MirrorBatchResult,
  cancellation: AbortSignal,
): Promise<void> {
  try {
    for (const group of chunk(candidates, MIRROR_RESOLVE_CONCURRENCY)) {
      if (cancellation.aborted) {
        throw new Error('mirror batch cancelled')
      }
      const outcomes = await Promise.all(group.map((candidate) => mirrorRepo(qx, candidate, since)))
      result.attempted += group.length
      outcomes.forEach((outcome, i) => {
        if (outcome.outcome === 'mirrored') {
          result.mirrored++
          result.unitsInserted += outcome.unitsInserted
          return
        }
        const { integrationId, owner, repoName } = group[i]
        result.skipped++
        svc.log.warn(
          {
            event: 'mirror_repo_skipped',
            integrationId,
            owner,
            repoName,
            reason: outcome.reason,
            message: outcome.message,
          },
          'repo skipped by mirror sweep',
        )
      })
    }
  } catch (err) {
    svc.log.error({ err, ...result }, 'mirror batch stopped')
    result.failed = true
  }
}

export async function mirrorBatch(limit: number): Promise<MirrorBatchResult> {
  const activityContext = Context.current()
  const result: MirrorBatchResult = {
    candidates: 0,
    attempted: 0,
    mirrored: 0,
    skipped: 0,
    unitsInserted: 0,
    failed: false,
  }
  const heartbeat = setInterval(() => {
    try {
      activityContext.heartbeat(result)
    } catch (err) {
      svc.log.warn({ errMsg: (err as Error).message }, 'heartbeat failed')
    }
  }, HEARTBEAT_INTERVAL_MS)

  try {
    const now = new Date()
    const qx = dbStoreQx(svc.postgres.writer)
    const repos = await listUnmirroredNangoRepos(qx)
    const activityByChannel = await readGithubChannelActivity(new TinybirdClient(), now)
    const ordered = orderMirrorCandidates(repos, activityByChannel)
    result.candidates = ordered.length
    await initNangoCloudClient()
    await mirrorCandidates(
      qx,
      ordered.slice(0, limit),
      mirrorSince(now),
      result,
      activityContext.cancellationSignal,
    )
    return result
  } finally {
    clearInterval(heartbeat)
  }
}

export async function logMirrorSweepSummary(summary: IMirrorSweepSummary): Promise<void> {
  svc.log.info({ event: 'mirror_sweep_summary', ...summary }, 'mirror sweep summary')
}
