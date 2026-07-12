import { faker } from '@faker-js/faker'

import { generateUUIDv1 } from '@crowd/common'
import { insertOrganizations } from '@crowd/data-access-layer'
import type { QueryExecutor } from '@crowd/database'
import type { OrganizationDbInsert, OrganizationDbRow } from '@crowd/types'

import { withDefaults } from './defaults'

export const withOrganizationDefaults = (
  data: Partial<OrganizationDbInsert>[],
): OrganizationDbInsert[] =>
  withDefaults<OrganizationDbInsert>({
    id: () => generateUUIDv1(),
    displayName: () => faker.company.name(),
    manuallyCreated: false,
    isTeamOrganization: false,
    isAffiliationBlocked: false,
  })(data)

export async function createOrganizations(
  qx: QueryExecutor,
  data: OrganizationDbInsert[],
): Promise<OrganizationDbRow[]> {
  if (data.length === 0) {
    return []
  }

  return insertOrganizations(qx, data, true, true)
}
