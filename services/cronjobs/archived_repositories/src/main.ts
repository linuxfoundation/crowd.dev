import { Queue } from 'bullmq'

import { Config, getConfig } from './config.js'
import { closeConnection, fetchRepositoryUrls } from './database.js'
import { GERRIT_QUEUE_NAME, GITHUB_QUEUE_NAME, GITLAB_QUEUE_NAME } from './types'
import { JobData, Platform, RepositoryToCheck } from './types.js'

/**
 * Sets up the BullMQ queues for processing jobs for repository URLs, fetches all the URLs to be
 * processed from the database, and adds them to the respective queues.
 *
 * @param config An instance of a `Config` to be used by the main process.
 */
async function main(config: Config) {
  const queueOptions = {
    connection: { url: config.RedisUrl },
    defaultJobOptions: {
      attempts: 4,
      backoff: {
        type: 'exponential',
        delay: 3000,
        jitter: 0.5,
      },
    },
  }

  const githubQueue = new Queue(GITHUB_QUEUE_NAME, queueOptions)
  githubQueue.on('error', (err) => console.error('GitHub Queue Error:', err))

  const gitlabQueue = new Queue(GITLAB_QUEUE_NAME, queueOptions)
  gitlabQueue.on('error', (err) => console.error('GitLab Queue Error:', err))

  const gerritQueue = new Queue(GERRIT_QUEUE_NAME, queueOptions)
  gerritQueue.on('error', (err) => console.error('Gerrit Queue Error:', err))

  const closeQueues = () =>
    Promise.all([githubQueue.close(), gitlabQueue.close(), gerritQueue.close()])

  console.log(`Starting main cronjob to enqueue repository URLs`)

  const repos = await fetchRepositoryUrls(config)

  if (repos.length === 0) {
    console.log(`No repositories found, exiting.`)
    await closeQueues()
    await closeConnection()
    return
  }

  const { githubJobs, gitlabJobs, gerritJobs } = prepareJobsByPlatform(repos)

  // Add jobs to their respective queues
  if (githubJobs.length > 0) {
    await githubQueue.addBulk(githubJobs)
    githubJobs.forEach((job) => console.log(`Enqueued GitHub URL: ${job.data.url}`))
    console.log(`Enqueued ${githubJobs.length} GitHub repos`)
  }

  if (gitlabJobs.length > 0) {
    await gitlabQueue.addBulk(gitlabJobs)
    gitlabJobs.forEach((job) => console.log(`Enqueued Gitlab URL: ${job.data.url}`))
    console.log(`Enqueued ${gitlabJobs.length} GitLab repos`)
  }

  if (gerritJobs.length > 0) {
    await gerritQueue.addBulk(gerritJobs)
    gerritJobs.forEach((job) => console.log(`Enqueued Gerrit URL: ${job.data.url}`))
    console.log(`Enqueued ${gerritJobs.length} Gerrit repos`)
  }

  const totalProcessed = githubJobs.length + gitlabJobs.length + gerritJobs.length

  console.log(`${totalProcessed} jobs enqueued.`)

  await closeQueues()
  await closeConnection()
}

/**
 * prepareJobsByPlatform receives a list of repositories to be added to job definitions for
 * processing by BullMQ. Supports GitHub, Gitlab and Gerrit (repositories with a Gerrit base URL),
 * while other platforms will be logged and skipped.
 *
 * @param repos The list of repositories.
 */
function prepareJobsByPlatform(repos: RepositoryToCheck[]): {
  githubJobs: JobData[]
  gitlabJobs: JobData[]
  gerritJobs: JobData[]
} {
  const githubJobs: JobData[] = []
  const gitlabJobs: JobData[] = []
  const gerritJobs: JobData[] = []

  repos.forEach(({ url, gerritBaseUrl }) => {
    let platform: Platform

    try {
      const parsed = new URL(url)

      if (gerritBaseUrl) {
        platform = Platform.GERRIT
      } else {
        switch (parsed.hostname) {
          case 'github.com':
            platform = Platform.GITHUB
            break
          case 'gitlab.com':
            platform = Platform.GITLAB
            break
          default:
            console.warn(`Skipping URL from unsupported platform: ${url}`)
            return
        }
      }
    } catch (error) {
      console.warn(`Skipping URL due to error: ${error}`)
      return
    }

    const jobData = {
      name: `${platform}-repo-${url.replace(/[^a-zA-Z0-9]/g, '-')}`,
      data: {
        url,
        platform,
        ...(gerritBaseUrl ? { gerritBaseUrl } : {}),
      },
      opts: {
        removeOnComplete: 1000,
        removeOnFail: 5000,
      },
    }

    switch (platform) {
      case Platform.GITHUB:
        githubJobs.push(jobData)
        break
      case Platform.GITLAB:
        gitlabJobs.push(jobData)
        break
      case Platform.GERRIT:
        gerritJobs.push(jobData)
        break
    }
  })

  return { githubJobs, gitlabJobs, gerritJobs }
}

if (require.main === module) {
  const config = getConfig()

  main(config)
    .then(() => {
      console.log('Main finished successfully, exiting.')
      process.exit(0)
    })
    .catch((error) => {
      console.error('Error in main execution:', error)
      process.exit(1)
    })
}
