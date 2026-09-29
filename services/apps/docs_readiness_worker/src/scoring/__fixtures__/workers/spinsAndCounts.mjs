import { workerData } from 'node:worker_threads'

const counters = new Int32Array(workerData.counters)
for (;;) {
  Atomics.add(counters, 0, 1)
}
