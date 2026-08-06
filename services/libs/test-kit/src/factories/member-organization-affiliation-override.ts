import { changeMemberOrganizationAffiliationOverrides } from '@crowd/data-access-layer'
import type { QueryExecutor } from '@crowd/database'
import type {
  MemberOrganizationAffiliationOverrideDbInsert,
  MemberOrganizationAffiliationOverrideDbRow,
} from '@crowd/types'

export async function upsertMemberOrganizationAffiliationOverrides(
  qx: QueryExecutor,
  data: MemberOrganizationAffiliationOverrideDbInsert[],
): Promise<MemberOrganizationAffiliationOverrideDbRow[]> {
  if (data.length === 0) {
    return []
  }

  return changeMemberOrganizationAffiliationOverrides(qx, data, true)
}
