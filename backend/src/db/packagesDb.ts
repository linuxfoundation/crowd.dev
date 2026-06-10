import { getDbConnection } from '@crowd/data-access-layer/src/database'
import { QueryExecutor, pgpQx } from '@crowd/data-access-layer/src/queryExecutor'

import { PACKAGES_DB_CONFIG } from '@/conf'

let _init: Promise<QueryExecutor> | undefined

export function getPackagesQx(): Promise<QueryExecutor> {
  if (!_init) {
    if (!PACKAGES_DB_CONFIG) {
      throw new Error(
        'Packages DB is not configured — set CROWD_PACKAGES_DB_* environment variables',
      )
    }
    _init = getDbConnection(PACKAGES_DB_CONFIG).then(pgpQx)
  }
  return _init
}
