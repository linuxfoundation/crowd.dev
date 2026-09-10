import { faker } from '@faker-js/faker'

import { generateUUIDv1 } from '@crowd/common'
import { createMember as insertMember } from '@crowd/data-access-layer'
import type { QueryExecutor } from '@crowd/database'
import type { MemberDbInsert, MemberDbRow } from '@crowd/types'

import { withDefaults } from './defaults'

export const withMemberDefaults = withDefaults<MemberDbInsert>()({
  id: () => generateUUIDv1(),
  displayName: () => faker.person.fullName(),
  joinedAt: () => new Date().toISOString(),
  manuallyCreated: false,
})

export async function createMembers(
  qx: QueryExecutor,
  data: MemberDbInsert[],
): Promise<MemberDbRow[]> {
  if (data.length === 0) {
    return []
  }

  return Promise.all(data.map((item) => insertMember(qx, item)))
}
