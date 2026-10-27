import { Context } from '@temporalio/activity'

import { countShadowRepos as countShadowReposInDb } from '@crowd/data-access-layer/src/connectors'
import { listUnmirroredNangoRepos } from '@crowd/data-access-layer/src/integrations'
import { dbStoreQx } from '@crowd/data-access-layer/src/queryExecutor'
import { TinybirdClient } from '@crowd/database'
import { initNangoCloudClient } from '@crowd/nango'

import { readGithubChannelActivity } from '../githubChannelActivity'
import { svc } from '../main'
import { mirrorRepo } from '../mirrorRepo'
import {
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

export async function mirrorBatch(limit: number): Promise<MirrorBatchResult> {
  const now = new Date()
  const qx = dbStoreQx(svc.postgres.writer)
  const repos = await listUnmirroredNangoRepos(qx)
  const activityByChannel = await readGithubChannelActivity(new TinybirdClient(), now)
  const ordered = orderMirrorCandidates(repos, activityByChannel)
  const since = mirrorSince(now)
  await initNangoCloudClient()

  const result: MirrorBatchResult = {
    candidates: ordered.length,
    attempted: 0,
    mirrored: 0,
    skipped: 0,
    unitsInserted: 0,
    failed: false,
  }

  try {
    for (const group of chunk(ordered.slice(0, limit), MIRROR_RESOLVE_CONCURRENCY)) {
      Context.current().heartbeat()
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

  return result
}

export async function logMirrorSweepSummary(summary: IMirrorSweepSummary): Promise<void> {
  svc.log.info({ event: 'mirror_sweep_summary', ...summary }, 'mirror sweep summary')
}
