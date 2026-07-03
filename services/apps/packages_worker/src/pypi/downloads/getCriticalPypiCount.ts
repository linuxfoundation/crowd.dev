import { getCriticalPypiPackageCount } from '@crowd/data-access-layer'

import { getPackagesDb } from '../../db'

// Count of critical PyPI packages, so the daily downloads workflow can skip its BigQuery scan
// when there are none (the merge is scoped to is_critical, mirroring how deps.dev scopes to our
// packages in the Postgres merge rather than pushing our package list into BigQuery).
export async function getCriticalPypiCount(): Promise<{ count: number }> {
  const qx = await getPackagesDb()
  const count = await getCriticalPypiPackageCount(qx)
  return { count }
}
