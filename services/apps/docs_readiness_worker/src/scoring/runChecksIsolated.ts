import path from 'node:path'
import { Worker } from 'node:worker_threads'

import { Afdocs } from './afdocs'
import { DeadlineExceededError } from './deadlineError'

export type AfdocsReport = Awaited<ReturnType<Afdocs['runChecks']>>

// Each thread may use up to its heap cap, so this bounds the pod's worst-case memory.
export const MAX_CONCURRENT_WORKERS = 4

interface IWorkerOptions {
  timeoutMs: number
  timeoutMessage: string
  maxOldGenerationSizeMb: number
}

let activeWorkers = 0
const waiting: (() => void)[] = []

const whileQueued = (message: string) => `${message} while waiting for a free scoring thread`

function acquireSlot(timeoutMs: number, timeoutMessage: string): Promise<void> {
  if (activeWorkers < MAX_CONCURRENT_WORKERS) {
    activeWorkers++
    return Promise.resolve()
  }

  return new Promise<void>((resolve, reject) => {
    const start = () => {
      clearTimeout(timer)
      activeWorkers++
      resolve()
    }
    const timer = setTimeout(() => {
      waiting.splice(waiting.indexOf(start), 1)
      reject(new DeadlineExceededError(whileQueued(timeoutMessage)))
    }, timeoutMs)
    waiting.push(start)
  })
}

function releaseSlot() {
  activeWorkers--
  waiting.shift()?.()
}

function startWorker<T>(
  file: string,
  workerData: unknown,
  { timeoutMs, timeoutMessage, maxOldGenerationSizeMb }: IWorkerOptions,
): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const worker = new Worker(file, {
      workerData,
      execArgv: [],
      resourceLimits: { maxOldGenerationSizeMb },
    })

    let settled = false
    const settle = (finish: () => void) => {
      if (settled) {
        return
      }
      settled = true
      clearTimeout(timer)
      worker.terminate().then(finish, finish)
    }

    const timer = setTimeout(
      () => settle(() => reject(new DeadlineExceededError(timeoutMessage))),
      timeoutMs,
    )

    worker.once('message', (message: { result?: T; error?: string }) =>
      settle(() =>
        'error' in message
          ? reject(new Error(message.error || 'afdocs worker failed without an error message'))
          : resolve(message.result),
      ),
    )
    worker.once('error', (err) => settle(() => reject(err)))
    worker.once('exit', (code) =>
      settle(() => reject(new Error(`afdocs worker exited with code ${code} without a result`))),
    )
  })
}

// A synchronous parse on a huge page blocks its thread for minutes to hours. In a worker thread
// it cannot block the main event loop, and terminate() can kill it, which a promise race cannot.
export async function runInWorker<T>(
  file: string,
  workerData: unknown,
  options: IWorkerOptions,
): Promise<T> {
  const startedAt = Date.now()
  await acquireSlot(options.timeoutMs, options.timeoutMessage)

  try {
    const timeoutMs = options.timeoutMs - (Date.now() - startedAt)
    if (timeoutMs <= 0) {
      throw new DeadlineExceededError(whileQueued(options.timeoutMessage))
    }
    return await startWorker<T>(file, workerData, { ...options, timeoutMs })
  } finally {
    releaseSlot()
  }
}

const AFDOCS_WORKER_FILE = path.join(__dirname, 'afdocsWorker.mjs')

// A page whose parsed DOM needs more fails this run instead of pushing the pod into GC thrash.
const AFDOCS_MAX_OLD_GENERATION_MB = 1024

export function runChecksIsolated(
  docsUrl: string,
  timeoutMs: number,
  timeoutMessage: string,
): Promise<AfdocsReport> {
  return runInWorker<AfdocsReport>(
    AFDOCS_WORKER_FILE,
    { url: docsUrl },
    { timeoutMs, timeoutMessage, maxOldGenerationSizeMb: AFDOCS_MAX_OLD_GENERATION_MB },
  )
}
