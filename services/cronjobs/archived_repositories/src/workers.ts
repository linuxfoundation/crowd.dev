import { Job, Worker, WorkerOptions } from 'bullmq'

import { getGerritRepoStatus } from './clients/gerrit'
import { getGithubRepoStatus } from './clients/github'
import { getGitlabRepoStatus } from './clients/gitlab'
import { getConfig } from './config.js'
import { updateRepositoryStatus } from './database'
import { GERRIT_QUEUE_NAME, GITHUB_QUEUE_NAME, GITLAB_QUEUE_NAME, Platform } from './types'

const config = getConfig()

async function handleJob(job: Job) {
  if (!job.data || !job.data.url) {
    throw new Error('Job data must contain a valid URL')
  }

  let archived, excluded
  if (job.data.platform === Platform.GITHUB) {
    console.log(`Processing GitHub repo: ${job.data.url}`)
    const githubStatus = await getGithubRepoStatus(job.data.url, config)
    archived = githubStatus.archived
    excluded = githubStatus.excluded

    // .github repositories should always be excluded from calculations, regardless of whether they are archived.
    const parsed = new URL(job.data.url)
    const parts = parsed.pathname.split('/').filter(Boolean)
    if (parts.length >= 2 && parts[1] === '.github') {
      console.log(`Forcefully marking .github repository as excluded: ${job.data.url}`)
      excluded = true
    }
  } else if (job.data.platform === Platform.GITLAB) {
    console.log(`Processing GitLab repo: ${job.data.url}`)
    const gitlabStatus = await getGitlabRepoStatus(job.data.url, config)
    archived = gitlabStatus.archived
    excluded = gitlabStatus.excluded
  } else if (job.data.platform === Platform.GERRIT) {
    if (!job.data.gerritBaseUrl) {
      throw new Error(`Gerrit job for ${job.data.url} has no Gerrit base URL`)
    }
    console.log(`Processing Gerrit repo: ${job.data.url}`)
    const gerritStatus = await getGerritRepoStatus(job.data.url, job.data.gerritBaseUrl)
    archived = gerritStatus.archived
    excluded = gerritStatus.excluded
  } else {
    throw new Error(`Unsupported platform: ${job.data.platform}`)
  }

  // Now update the database table with the result
  await updateRepositoryStatus(job.data.url, archived, excluded, config)
}

// TODO: make this configurable via environment variables
// The rate limits for GitHub and GitLab are different, so we need to set different limiters for each worker.
// GitHub allows 5000 requests per hour (for authenticated requests), while GitLab allows 2000 requests per minute.
// See the rate limits here:
// * https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api?apiVersion=2022-11-28
// * https://docs.gitlab.com/user/gitlab_com/#rate-limits-on-gitlabcom
//
// The `max` parameter below is the maximum number of jobs that can be processed in the `duration` period.
// Conversely, the `duration` parameter is the time period in milliseconds during which the `max` number
// of jobs can be processed. If the limit is reached, the worker will pause until the next period starts.
const githubWorkerOptions: WorkerOptions = {
  connection: { url: config.RedisUrl },
  concurrency: 5,
  limiter: {
    max: 3500, // GitHub allows 5000 requests per hour for authenticated requests, but let's play it safe.
    duration: 3600000,
  },
}

const gitlabWorkerOptions: WorkerOptions = {
  connection: { url: config.RedisUrl },
  concurrency: 5,
  limiter: {
    max: 1500, // GitLab allows 2000 requests per minute, but let's play it safe.
    duration: 60000,
  },
}

// Gerrit instances are self-hosted by each community and publish no rate limit, so keep the load
// low. One job is one anonymous request.
const gerritWorkerOptions: WorkerOptions = {
  connection: { url: config.RedisUrl },
  concurrency: 2,
  limiter: {
    max: 120,
    duration: 60000,
  },
}

const githubWorker = new Worker(GITHUB_QUEUE_NAME, handleJob, githubWorkerOptions)
const gitlabWorker = new Worker(GITLAB_QUEUE_NAME, handleJob, gitlabWorkerOptions)
const gerritWorker = new Worker(GERRIT_QUEUE_NAME, handleJob, gerritWorkerOptions)

githubWorker.on('failed', (job, err) => {
  console.error(`GitHub job ${job?.id || 'unknown'} has failed with message: ${err}, ${err.stack}`)
})
githubWorker.on('error', (err) => {
  console.error(`GitHub worker error: ${err}, ${err.stack}`)
})

gitlabWorker.on('failed', (job, err) => {
  console.error(`GitLab job ${job?.id || 'unknown'} has failed with message: ${err}, ${err.stack}`)
})
gitlabWorker.on('error', (err) => {
  console.error(`GitLab worker error: ${err}, ${err.stack}`)
})

gerritWorker.on('failed', (job, err) => {
  console.error(`Gerrit job ${job?.id || 'unknown'} has failed with message: ${err}, ${err.stack}`)
})
gerritWorker.on('error', (err) => {
  console.error(`Gerrit worker error: ${err}, ${err.stack}`)
})

// TODO: Check if there are event listeners for when we hit the rate limit, and log that, as well as for resume.

export { githubWorker, gitlabWorker, gerritWorker }
