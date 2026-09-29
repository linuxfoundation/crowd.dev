import { parentPort, workerData } from 'node:worker_threads'

parentPort.postMessage({ result: { echoed: workerData } })
