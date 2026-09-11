import * as db from '@crowd/data-access-layer/src/member_merge/subprojectSuggestions'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'

import { svc } from '../main'
import { ISubprojectMemberMergePair } from '../types'

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
