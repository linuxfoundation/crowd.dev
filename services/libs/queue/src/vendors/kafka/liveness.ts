import { promises as fs } from 'fs'
import path from 'path'

import { Logger } from '@crowd/logging'

// above data-sink-worker's DB timeouts (lock 60, statement 70, query 75 min) so those fail first
const DEFAULT_THRESHOLD_MS = 90 * 60 * 1000
const CHECK_INTERVAL_MS = 30 * 1000
const WAITING_COUNT_MAX_AGE_MS = 5 * 60 * 1000

export interface IConsumerLiveness {
  start(): void
  stop(): void
  messageStarted(): number
  messageFinished(id: number): void
  messageSkipped(): void
  partitionsAssigned(): void
  setWaitingInAssignedPartitions(count: number): void
  consumerFailed(reason: string): void
}

export interface IConsumerProgress {
  inProgressStarts: number[]
  lastFinishedAt: number
  waiting: number
  waitingSince: number | undefined
  waitingUpdatedAt: number | undefined
  failure: string | undefined
}

const isWaitingCountFresh = (progress: IConsumerProgress, now: number): boolean =>
  progress.waitingUpdatedAt !== undefined &&
  now - progress.waitingUpdatedAt <= WAITING_COUNT_MAX_AGE_MS

export const getStuckReason = (
  progress: IConsumerProgress,
  thresholdMs: number,
  now: number,
): string | undefined => {
  if (progress.failure) {
    return progress.failure
  }

  const oldestStart = Math.min(...progress.inProgressStarts)
  if (now - oldestStart > thresholdMs) {
    return `a message has been in progress for ${Math.round((now - oldestStart) / 1000)}s`
  }

  const idleSince = Math.max(progress.lastFinishedAt, progress.waitingSince ?? 0)
  if (isWaitingCountFresh(progress, now) && progress.waiting > 0 && now - idleSince > thresholdMs) {
    return `${progress.waiting} messages are waiting and none finished in ${Math.round(
      (now - idleSince) / 1000,
    )}s`
  }

  return undefined
}

const parseThresholdMs = (value: string | undefined): number => {
  if (!value) {
    return DEFAULT_THRESHOLD_MS
  }

  const parsed = Number(value)
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`Invalid CROWD_QUEUE_LIVENESS_THRESHOLD_MS: ${value}`)
  }

  return parsed
}

const touchFile = async (file: string): Promise<void> => {
  const now = new Date()
  try {
    await fs.utimes(file, now, now)
  } catch (err) {
    if (err.code !== 'ENOENT') {
      throw err
    }
    await fs.writeFile(file, '')
  }
}

export const createConsumerLiveness = (
  log: Logger,
  file: string,
  thresholdMs: number,
): IConsumerLiveness => {
  const inProgress = new Map<number, number>()
  let nextId = 0
  let lastFinishedAt = Date.now()
  let waiting = 0
  let waitingSince: number | undefined
  let waitingUpdatedAt: number | undefined
  let failure: string | undefined
  let interval: NodeJS.Timeout | undefined
  let stuck = false
  let staleWaitingLogged = false

  const check = async (): Promise<void> => {
    const now = Date.now()
    const progress: IConsumerProgress = {
      inProgressStarts: [...inProgress.values()],
      lastFinishedAt,
      waiting,
      waitingSince,
      waitingUpdatedAt,
      failure,
    }

    const reason = getStuckReason(progress, thresholdMs, now)
    if (reason) {
      stuck = true
      log.error({ file, inProgress: inProgress.size, waiting }, `Consumer is stuck: ${reason}!`)
      return
    }

    if (stuck) {
      stuck = false
      log.info('Consumer is making progress again!')
    }

    const waitingIsStale = waitingUpdatedAt !== undefined && !isWaitingCountFresh(progress, now)
    if (waitingIsStale && !staleWaitingLogged) {
      log.warn({ file }, 'Waiting message count is stale, liveness ignores it until it updates!')
    }
    staleWaitingLogged = waitingIsStale

    try {
      await touchFile(file)
    } catch (err) {
      log.warn(err, 'Failed to touch the consumer liveness file!')
    }
  }

  return {
    start() {
      if (interval) {
        return
      }

      log.info({ file, thresholdMs }, 'Consumer liveness enabled!')
      lastFinishedAt = Date.now()
      interval = setInterval(() => void check(), CHECK_INTERVAL_MS)
      void fs
        .mkdir(path.dirname(file), { recursive: true })
        .catch((err) => log.warn(err, 'Failed to create the consumer liveness directory!'))
        .then(check)
    },
    stop() {
      clearInterval(interval)
      interval = undefined
    },
    messageStarted() {
      const id = nextId++
      inProgress.set(id, Date.now())
      return id
    },
    messageFinished(id) {
      inProgress.delete(id)
      lastFinishedAt = Date.now()
    },
    messageSkipped() {
      lastFinishedAt = Date.now()
    },
    partitionsAssigned() {
      waitingSince = Date.now()
    },
    setWaitingInAssignedPartitions(count) {
      if (count > 0 && waiting === 0) {
        waitingSince = Date.now()
      }
      waiting = count
      waitingUpdatedAt = Date.now()
    },
    consumerFailed(reason) {
      failure = reason
    },
  }
}

export const createConsumerLivenessFromEnv = (log: Logger): IConsumerLiveness | undefined => {
  const file = process.env.CROWD_QUEUE_LIVENESS_FILE
  if (!file) {
    return undefined
  }

  return createConsumerLiveness(
    log,
    file,
    parseThresholdMs(process.env.CROWD_QUEUE_LIVENESS_THRESHOLD_MS),
  )
}
