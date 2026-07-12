import { changeMemberOrganizationAffiliationOverrides } from '@crowd/data-access-layer'
import type { QueryExecutor } from '@crowd/database'
import type {
  MemberOrganizationAffiliationOverrideDbInsert,
  MemberOrganizationAffiliationOverrideDbRow,
} from '@crowd/types'

export async function createMemberOrganizationAffiliationOverrides(
  qx: QueryExecutor,
  data: MemberOrganizationAffiliationOverrideDbInsert[],
): Promise<MemberOrganizationAffiliationOverrideDbRow[]> {
  if (data.length === 0) {
    return []
  }

  return changeMemberOrganizationAffiliationOverrides(
    qx,
    data.map((row) => ({
      id: row.id,
      memberId: row.memberId,
      memberOrganizationId: row.memberOrganizationId,
      allowAffiliation: row.allowAffiliation,
      isPrimaryWorkExperience: row.isPrimaryWorkExperience,
    })),
    true,
  )
}
