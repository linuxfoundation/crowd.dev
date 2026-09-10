import { generateUUIDv1 } from '@crowd/common'
import { insertMemberIdentities } from '@crowd/data-access-layer'
import type { QueryExecutor } from '@crowd/database'
import type { MemberIdentityDbInsert, MemberIdentityDbRow } from '@crowd/types'

import { withDefaults } from './defaults'

export const withMemberIdentityDefaults = withDefaults<MemberIdentityDbInsert>()({
  id: () => generateUUIDv1(),
})

export async function createMemberIdentities(
  qx: QueryExecutor,
  data: MemberIdentityDbInsert[],
): Promise<MemberIdentityDbRow[]> {
  if (data.length === 0) {
    return []
  }

  return insertMemberIdentities(qx, data, true, true)
}
