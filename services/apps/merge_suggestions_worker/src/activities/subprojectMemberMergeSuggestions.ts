import { getMemberNoMerge } from '@crowd/data-access-layer/src/member_merge'
import * as db from '@crowd/data-access-layer/src/member_merge/subprojectSuggestions'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { RedisCache } from '@crowd/redis'

import { svc } from '../main'
import { ISubprojectMemberMergePair } from '../types'

const SUBPROJECT_MEMBER_MERGE_CACHE = 'subproject-member-merge'

export async function fetchRecentlyOnboardedSubprojects(): Promise<string[]> {
  const qx = pgpQx(svc.postgres.reader.connection())
  return db.fetchRecentlyOnboardedSubprojects(qx)
}

export async function fetchSubprojectMemberMergePairs(
  segmentId: string,
): Promise<ISubprojectMemberMergePair[]> {
  const qx = pgpQx(svc.postgres.reader.connection())
  const pairs = await db.fetchSubprojectMemberMergePairs(qx, segmentId)

  if (pairs.length === 0) return pairs

  const memberIds = [...new Set(pairs.flatMap((p) => [p.primary.id, p.other.id]))]
  const noMerge = await getMemberNoMerge(qx, memberIds)
  const blocked = new Set(
    noMerge.flatMap((nm) => [`${nm.memberId}:${nm.noMergeId}`, `${nm.noMergeId}:${nm.memberId}`]),
  )

  return pairs.filter((p) => !blocked.has(`${p.primary.id}:${p.other.id}`))
}

export async function fetchCachedSubprojects(subprojectIds: string[]): Promise<string[]> {
  if (subprojectIds.length === 0) return []

  const cache = new RedisCache(SUBPROJECT_MEMBER_MERGE_CACHE, svc.redis, svc.log)
  const exists = await Promise.all(subprojectIds.map((id) => cache.exists(id)))

  return subprojectIds.filter((_, i) => exists[i])
}

export async function markSubprojectDone(subprojectId: string): Promise<void> {
  const cache = new RedisCache(SUBPROJECT_MEMBER_MERGE_CACHE, svc.redis, svc.log)
  // 7d from this run; SQL already drops the project after insightsProjects.createdAt + 7d
  await cache.set(subprojectId, '1', 7 * 24 * 60 * 60)
}
