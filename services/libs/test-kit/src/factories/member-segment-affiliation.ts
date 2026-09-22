import { insertMemberSegmentAffiliations } from '@crowd/data-access-layer'
import type { QueryExecutor } from '@crowd/database'
import type { MemberSegmentAffiliationDbInsert, MemberSegmentAffiliationDbRow } from '@crowd/types'

export async function createMemberSegmentAffiliations(
  qx: QueryExecutor,
  data: MemberSegmentAffiliationDbInsert[],
): Promise<MemberSegmentAffiliationDbRow[]> {
  if (data.length === 0) {
    return []
  }

  return insertMemberSegmentAffiliations(qx, data, true, true)
}
