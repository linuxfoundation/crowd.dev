import { generateUUIDv1 } from '@crowd/common'
import { insertMemberSegmentAffiliationRows } from '@crowd/data-access-layer'
import type { QueryExecutor } from '@crowd/database'
import type { MemberSegmentAffiliationDbInsert, MemberSegmentAffiliationDbRow } from '@crowd/types'

import { withDefaults } from './defaults'

export const withMemberSegmentAffiliationDefaults = (
  data: Partial<MemberSegmentAffiliationDbInsert>[],
): MemberSegmentAffiliationDbInsert[] =>
  withDefaults<MemberSegmentAffiliationDbInsert>({
    id: () => generateUUIDv1(),
    verified: false,
  })(data)

export async function createMemberSegmentAffiliations(
  qx: QueryExecutor,
  data: MemberSegmentAffiliationDbInsert[],
): Promise<MemberSegmentAffiliationDbRow[]> {
  if (data.length === 0) {
    return []
  }

  return insertMemberSegmentAffiliationRows(qx, data, true, true)
}
