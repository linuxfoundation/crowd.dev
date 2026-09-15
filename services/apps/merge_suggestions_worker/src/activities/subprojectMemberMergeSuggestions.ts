import * as db from '@crowd/data-access-layer/src/member_merge/subprojectSuggestions'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { RedisCache } from '@crowd/redis'

import { svc } from '../main'
import { ISubprojectMemberMergePair } from '../types'

const SUBPROJECT_MEMBER_MERGE_CACHE = 'subproject-member-merge'

export async function fetchRecentlyOnboardedSubprojects(): Promise<string[]> {
  const qx = pgpQx(svc.postgres.writer.connection())
  return db.fetchRecentlyOnboardedSubprojects(qx)
}

export async function fetchSubprojectMemberMergePairs(
  segmentId: string,
): Promise<ISubprojectMemberMergePair[]> {
  const qx = pgpQx(svc.postgres.writer.connection())
  return db.fetchSubprojectMemberMergePairs(qx, segmentId)
}

export async function fetchCachedSubprojects(subprojectIds: string[]): Promise<string[]> {
  if (subprojectIds.length === 0) return []

  const cache = new RedisCache(SUBPROJECT_MEMBER_MERGE_CACHE, svc.redis, svc.log)
  const exists = await Promise.all(subprojectIds.map((id) => cache.exists(id)))

  return subprojectIds.filter((_, i) => exists[i])
}

export async function markSubprojectDone(subprojectId: string): Promise<void> {
  const cache = new RedisCache(SUBPROJECT_MEMBER_MERGE_CACHE, svc.redis, svc.log)
  // TTL matches the SQL eligibility window so the key expires when the project drops out
  await cache.set(subprojectId, '1', 7 * 24 * 60 * 60)
}
