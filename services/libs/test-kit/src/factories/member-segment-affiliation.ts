import { insertMemberSegmentAffiliationRows } from '@crowd/data-access-layer'
import type { QueryExecutor } from '@crowd/database'
import type { MemberSegmentAffiliationDbInsert, MemberSegmentAffiliationDbRow } from '@crowd/types'

export async function createMemberSegmentAffiliations(
  qx: QueryExecutor,
  data: MemberSegmentAffiliationDbInsert[],
): Promise<MemberSegmentAffiliationDbRow[]> {
  if (data.length === 0) {
    return []
  }

  return insertMemberSegmentAffiliationRows(qx, data, true, true)
}
