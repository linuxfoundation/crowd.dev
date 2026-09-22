import fs from 'fs'
import path from 'path'

import { CronJob } from 'cron'
import { QueryTypes, Sequelize } from 'sequelize'

import { databaseInit } from '@/database/databaseConnection'
import { getServiceLogger } from '@crowd/logging'
import { RedisClient, getRedisClient } from '@crowd/redis'

import { REDIS_CONFIG } from '../conf'
import jobs from './jobs'

const log = getServiceLogger()

for (const job of jobs) {
  const cronJob = new CronJob(
    job.cronTime,
    async () => {
      log.info({ job: job.name }, 'Triggering job.')
      try {
        await job.onTrigger(log)
      } catch (err) {
        log.error(err, { job: job.name }, 'Error while executing a job!')
      }
    },
    null,
    true,
    'Europe/Berlin',
  )
  if (cronJob.running) {
    log.info({ job: job.name }, 'Scheduled a job.')
  }
}

const liveFilePath = path.join(__dirname, 'tmp/job-generator-live.tmp')
const readyFilePath = path.join(__dirname, 'tmp/job-generator-ready.tmp')

let seq: Sequelize
let redis: RedisClient
const initRedisSeq = async () => {
  if (!seq) {
    seq = (await databaseInit()).sequelize as Sequelize
  }

  if (!redis) {
    redis = await getRedisClient(REDIS_CONFIG, true)
  }
}

setInterval(async () => {
  try {
    await initRedisSeq()
    log.debug('Checking liveness and readiness for job generator.')
    const [redisPingRes, dbPingRes] = await Promise.all([
      // ping redis,
      redis.ping().then((res) => res === 'PONG'),
      // ping database
      seq.query('select 1', { type: QueryTypes.SELECT }).then((rows) => rows.length === 1),
    ])
    if (redisPingRes && dbPingRes) {
      await Promise.all([
        fs.promises.open(liveFilePath, 'a').then((file) => file.close()),
        fs.promises.open(readyFilePath, 'a').then((file) => file.close()),
      ])
    }
  } catch (err) {
    log.error(`Error checking liveness and readiness for job generator: ${err}`)
  }
}, 5000)
