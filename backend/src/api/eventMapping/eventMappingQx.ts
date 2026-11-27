import { DB_CONFIG } from '@/conf'
import { DbConfiguration } from '@/conf/configTypes'
import {
  IDatabaseConfig,
  QueryExecutor,
  getDbConnection,
  pgpQx,
} from '@crowd/data-access-layer/src/database'

export function eventMappingWriterConfig(db: DbConfiguration): IDatabaseConfig {
  const [user, password] = db.username
    ? [db.username, db.password]
    : [db.apiUsername, db.apiPassword]
  if (!user || !password) {
    throw new Error('No database credentials are configured for the event mapping endpoints!')
  }
  return { host: db.writeHost, port: db.port, database: db.database, user, password }
}

// Writer on purpose, also for the list: the page reads right after a decision
// and the read replica can lag behind it.
export async function getEventMappingQx(): Promise<QueryExecutor> {
  return pgpQx(await getDbConnection(eventMappingWriterConfig(DB_CONFIG)))
}
