import { getDbConnection } from '@crowd/database'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'

import { getPackagesDbConfig } from './config'

export async function getPackagesDb() {
  const conn = await getDbConnection(getPackagesDbConfig())
  return pgpQx(conn)
}
