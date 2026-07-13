import { insertOrganizationIdentities } from '@crowd/data-access-layer'
import type { QueryExecutor } from '@crowd/database'
import type { OrganizationIdentityDbInsert, OrganizationIdentityDbRow } from '@crowd/types'

/** Persist organization identity rows via the production insert path. */
export async function createOrganizationIdentities(
  qx: QueryExecutor,
  data: OrganizationIdentityDbInsert[],
): Promise<OrganizationIdentityDbRow[]> {
  if (data.length === 0) {
    return []
  }

  return insertOrganizationIdentities(qx, data, true, true)
}
