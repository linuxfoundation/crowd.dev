import { insertMemberOrganizations } from '@crowd/data-access-layer'
import type { QueryExecutor } from '@crowd/database'
import type { MemberOrganizationDbInsert, MemberOrganizationDbRow } from '@crowd/types'

export async function createMemberOrganizations(
  qx: QueryExecutor,
  data: MemberOrganizationDbInsert[],
): Promise<MemberOrganizationDbRow[]> {
  if (data.length === 0) {
    return []
  }

  return insertMemberOrganizations(qx, data, true, true)
}
