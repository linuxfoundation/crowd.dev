import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { DbConnection, getDbConnection } from '@crowd/database'

import { getCdpDbConfig, getPackagesDbConfig } from './config'

export async function getPackagesDb() {
  const conn = await getDbConnection(getPackagesDbConfig())
  return pgpQx(conn)
}

export async function getCdpDb() {
  const conn = await getDbConnection(getCdpDbConfig())
  return pgpQx(conn)
}

// Raw pg-promise connection for the same pool getPackagesDb() wraps
// (getDbConnection caches per host:database). Needed for COPY FROM STDIN via
// pg-copy-streams, which requires direct access to the underlying pg client.
export async function getPackagesDbConnection(): Promise<DbConnection> {
  return getDbConnection(getPackagesDbConfig())
}
