import { generateUUIDv1 } from '@crowd/common'
import { insertMemberOrganizations } from '@crowd/data-access-layer'
import type { QueryExecutor } from '@crowd/database'
import type { MemberOrganizationDbInsert, MemberOrganizationDbRow } from '@crowd/types'

import { withDefaults } from './defaults'

export const withMemberOrganizationDefaults = (
  data: Partial<MemberOrganizationDbInsert>[],
): MemberOrganizationDbInsert[] =>
  withDefaults<MemberOrganizationDbInsert>({
    id: () => generateUUIDv1(),
    verified: false,
  })(data)

export async function createMemberOrganizations(
  qx: QueryExecutor,
  data: MemberOrganizationDbInsert[],
): Promise<MemberOrganizationDbRow[]> {
  if (data.length === 0) {
    return []
  }

  return insertMemberOrganizations(qx, data, true, true)
}
