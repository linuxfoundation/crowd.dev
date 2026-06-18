import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getDbConnection } from '@crowd/database'

import { enrichVersions } from './src/cargo/enrich'

async function main() {
  const conn = await getDbConnection({
    host: 'localhost',
    port: 5434,
    database: 'packages-db',
    user: 'postgres',
    password: 'example',
  } as never)
  const qx = pgpQx(conn)

  const t = Date.now()
  const r = await enrichVersions(qx)
  console.log(`enrichVersions: ${((Date.now() - t) / 1000).toFixed(1)}s ->`, r)
  process.exit(0)
}

main().catch((e) => {
  console.error(e)
  process.exit(1)
})
