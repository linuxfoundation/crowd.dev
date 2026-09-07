import { QueryExecutor } from '../queryExecutor'
import { prepareBulkInsert } from '../utils'

export async function insertFakeOrganizationSuggestions(
  qx: QueryExecutor,
  organizationIds: string[],
): Promise<void> {
  if (organizationIds.length === 0) {
    return
  }

  const query = prepareBulkInsert(
    'fakeOrganizationSuggestions',
    ['organizationId', 'createdAt'],
    organizationIds.map((organizationId) => ({
      organizationId,
      createdAt: new Date(),
    })),
    '("organizationId") DO NOTHING',
  )

  await qx.result(query)
}
