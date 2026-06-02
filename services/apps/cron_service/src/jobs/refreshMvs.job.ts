import CronTime from 'cron-time-generator'

import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'

import { IJobDefinition } from '../types'

const MATERIALIZED_VIEWS = ['mv_maintainer_roles']

const job: IJobDefinition = {
  name: 'refresh-mvs',
  cronTime: CronTime.every(30).minutes(),
  timeout: 10 * 60, // 10 minutes
  process: async (ctx) => {
    ctx.log.info('Starting materialized view refresh job!')
    const dbConnection = await getDbConnection(WRITE_DB_CONFIG(), 1, 0)

    for (const mv of MATERIALIZED_VIEWS) {
      ctx.log.info({ mv }, `Refreshing materialized view: ${mv}`)
      const start = performance.now()
      await dbConnection.query(`REFRESH MATERIALIZED VIEW CONCURRENTLY "${mv}"`)
      const duration = ((performance.now() - start) / 1000.0).toFixed(2)
      ctx.log.info({ mv, duration }, `Refreshed materialized view ${mv} in ${duration}s`)
    }

    ctx.log.info('Materialized view refresh job completed!')
  },
}

export default job
