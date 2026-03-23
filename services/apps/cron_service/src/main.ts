import { spawn } from 'child_process'
import { CronJob } from 'cron'
import path from 'path'
import pidusage from 'pidusage'

import { getChildLogger, getServiceChildLogger, getServiceLogger } from '@crowd/logging'
import { SlackChannel, SlackPersona, sendSlackNotificationAsync } from '@crowd/slack'

import { loadJobs } from './loader'
import { IJobDefinition } from './types'

const log = getServiceLogger()

const jobQueue = new Map<string, boolean>()
const activeJobs = new Map<string, boolean>()
const MAX_CONCURRENT_JOBS = 5

let queue

const spawnJob = async (job: IJobDefinition) => {
  const jobLogger = getChildLogger(`job/${job.name}`, log)
  const start = performance.now()

  return new Promise<void>((resolve, reject) => {
    let finished = false
    let timeoutId: NodeJS.Timeout | undefined = undefined
    let intervalId: NodeJS.Timeout | undefined = undefined

    const child = spawn(
      './node_modules/.bin/tsx',
      [path.resolve(__dirname, 'worker.ts'), job.name],
      {
        stdio: 'pipe',
      },
    )

    const finish = (error?: Error) => {
      if (finished) return

      finished = true

      try {
        if (!child.killed) {
          child.kill('SIGKILL')
        }
      } catch (killErr) {
        // we can ignore this since we most likely were killing a dying child process
        // log.error(killErr, 'Error while killing a child process!')
      }

      if (timeoutId) {
        clearTimeout(timeoutId)
      }

      if (intervalId) {
        clearInterval(intervalId)
      }

      if (error) {
        reject(error)
      } else {
        resolve()
      }
    }

    timeoutId = setTimeout(async () => {
      finish(new Error(`Job was killed because it did not finish in time!`))
    }, job.timeout * 1000)

    intervalId = setInterval(async () => {
      try {
        if (child.pid) {
          const state = await pidusage(child.pid)
          const data = {
            cpu: state.cpu.toFixed(1),
            memory: (Math.round((state.memory / 1024 / 1024) * 100) / 100).toFixed(1),
          }

          const diff = ((performance.now() - start) / 1000.0).toFixed(2)
          jobLogger.info(
            `Job running for ${diff} seconds - current usage: CPU: ${data.cpu}%, RAM: ${data.memory}MB`,
          )
        }
      } catch (err) {
        // do nothing
      }
    }, 5000)

    // Handle process exit
    child.on('exit', (code, signal) => {
      if (code === 0) {
        finish()
      } else {
        finish(new Error(`Job exited with code ${code} (signal: ${signal})`))
      }
    })

    // Handle process errors
    child.on('error', (err) => {
      finish(new Error(`Job failed to start: ${err.message}`))
    })

    child.stdout.on('data', (data) => {
      console.log(data.toString().trim())
    })

    child.stderr.on('data', (data) => {
      console.error(data.toString().trim())
    })
  })
}

const queueJob = async (job: IJobDefinition) => {
  const jobLogger = getServiceChildLogger(`job/${job.name}`)

  // if a job is already running we don't run it again
  if (activeJobs.has(job.name)) {
    jobLogger.debug('Job is already running - skipping...')
    return
  }

  // if a job is already in queue
  if (jobQueue.has(job.name)) {
    jobLogger.debug('Job is already in queue - skipping...')
    return
  }

  jobLogger.debug('Queuing up the job.')

  // add to queue
  jobQueue.set(job.name, true)

  await queue.add(async () => {
    jobLogger.debug('Spawning a new job instance.')

    // remove from queue
    jobQueue.delete(job.name)
    const start = performance.now()

    try {
      activeJobs.set(job.name, true)
      await spawnJob(job)

      const diff = ((performance.now() - start) / 1000.0).toFixed(2)
      jobLogger.debug(`Job completed successfully in ${diff} seconds!`)
    } catch (err) {
      const diff = ((performance.now() - start) / 1000.0).toFixed(2)
      jobLogger.error(err, `Error while running a job! Job exited after ${diff} seconds!`)

      if (err instanceof Error && err.message.includes('did not finish in time')) {
        sendSlackNotificationAsync(
          SlackChannel.CDP_CRITICAL_ALERTS,
          SlackPersona.CRITICAL_ALERTER,
          `Cron job timed out: ${job.name}`,
          `Job \`${job.name}\` was killed after exceeding its ${job.timeout}s timeout (ran for ${diff}s).`,
        ).catch((slackErr) =>
          jobLogger.error(slackErr, 'Failed to send Slack timeout notification'),
        )
      }
    } finally {
      activeJobs.delete(job.name)
    }
  })
}

setImmediate(async () => {
  // initialize queue
  const pQueueModule = await import('p-queue')
  queue = new pQueueModule.default({ concurrency: MAX_CONCURRENT_JOBS })

  // initialize cron
  const availableJobs = await loadJobs()
  for (const job of availableJobs) {
    if ((job.enabled && (await job.enabled())) || !job.enabled) {
      const envCronTime = `${job.name.replaceAll('-', '_').replaceAll(' ', '_').toUpperCase()}_CRON_TIME`

      let cronTime = job.cronTime
      if (process.env[envCronTime]) {
        cronTime = process.env[envCronTime]
      }

      const cronJob = new CronJob(
        cronTime,
        async () => {
          await queueJob(job)
        },
        null,
        true,
        'Europe/Berlin',
      )

      if (cronJob.running) {
        log.info({ job: job.name, cronTime: job.cronTime }, 'Scheduled a new job.')
      }
    } else {
      log.info({ job: job.name }, 'Job is disabled - skipping...')
    }
  }
})
