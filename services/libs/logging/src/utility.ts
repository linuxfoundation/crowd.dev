import { performance } from 'perf_hooks'

import { Logger } from './types'

export const logExecutionTime = async <T>(
  process: () => Promise<T>,
  log: Logger,
  name: string,
): Promise<T> => {
  const start = performance.now()
  try {
    log.info(`Starting timing process ${name}...`)
    return await process()
  } finally {
    const end = performance.now()
    const duration = end - start
    const durationInSeconds = duration / 1000
    log.info(`Process ${name} took ${durationInSeconds.toFixed(2)} seconds!`)
  }
}

export const logExecutionTimeV2 = async <T>(
  toProcess: () => Promise<T>,
  log: Logger,
  name: string,
): Promise<T> => {
  if (!process.env.CROWD_LOG_EXECUTION_TIME) {
    return toProcess()
  }

  const minDurationMs = Number(process.env['CROWD_LOG_EXECUTION_TIME_MIN_DURATION'] || 0)

  const start = performance.now()

  const formatDuration = (durationMs: number) => (durationMs / 1000).toFixed(2)

  try {
    if (process.env.CROWD_LOG_EXECUTION_START) {
      log.info(`Starting process ${name}...`)
    }

    const result = await toProcess()
    const durationMs = performance.now() - start
    if (durationMs >= minDurationMs) {
      log.info(`Process ${name} took ${formatDuration(durationMs)} seconds!`)
    }
    return result
  } catch (e) {
    const durationMs = performance.now() - start
    if (durationMs >= minDurationMs) {
      log.info(`Process ${name} failed after ${formatDuration(durationMs)} seconds!`)
    }
    throw e
  }
}

export const timer = (log: Logger, name?: string) => {
  const start = performance.now()
  let isEnded = false
  return {
    end: function (overrideName?: string) {
      if (isEnded) {
        return
      }
      isEnded = true

      const end = performance.now()
      const duration = end - start
      const durationInSeconds = duration / 1000
      log.info(`Process ${overrideName ?? name} took ${durationInSeconds.toFixed(2)} seconds!`)
    },
  }
}
