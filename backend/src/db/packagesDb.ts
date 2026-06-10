import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { QueryExecutor, pgpQx } from '@crowd/data-access-layer/src/queryExecutor'

import { PACKAGES_DB_CONFIG } from '@/conf'

let _qx: QueryExecutor | undefined

export async function getPackagesQx(): Promise<QueryExecutor> {
  if (!_qx) {
    if (!PACKAGES_DB_CONFIG) {
      throw new Error(
        'Packages DB is not configured — set CROWD_PACKAGES_DB_* environment variables',
      )
    }
    const conn = await getDbConnection(PACKAGES_DB_CONFIG)
    _qx = pgpQx(conn)
  }
  return _qx
}
