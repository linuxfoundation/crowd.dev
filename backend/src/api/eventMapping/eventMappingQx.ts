import {
  QueryExecutor,
  WRITE_DB_CONFIG,
  getDbConnection,
  pgpQx,
} from '@crowd/data-access-layer/src/database'

// Writer on purpose, also for the list: the page reads right after a decision
// and the read replica can lag behind it.
export async function getEventMappingQx(): Promise<QueryExecutor> {
  return pgpQx(await getDbConnection(WRITE_DB_CONFIG()))
}
