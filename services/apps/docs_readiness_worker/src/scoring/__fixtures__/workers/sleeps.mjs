import { parentPort, workerData } from 'node:worker_threads'

await new Promise((resolve) => setTimeout(resolve, workerData.ms))
parentPort.postMessage({ result: 'slept' })
