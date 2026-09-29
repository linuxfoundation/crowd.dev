import { parentPort, workerData } from 'node:worker_threads'

const counters = new Int32Array(workerData.counters)
const running = Atomics.add(counters, 0, 1) + 1
let max = Atomics.load(counters, 1)
while (running > max) {
  const seen = Atomics.compareExchange(counters, 1, max, running)
  if (seen === max) {
    break
  }
  max = seen
}

await new Promise((resolve) => setTimeout(resolve, 150))
Atomics.sub(counters, 0, 1)
parentPort.postMessage({ result: 'held' })
