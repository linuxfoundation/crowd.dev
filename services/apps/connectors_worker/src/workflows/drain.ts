import { sleep } from '@temporalio/workflow'

const DRAIN_POLL_INTERVAL = '30 seconds'
const DRAIN_MAX_POLLS = 120

export async function pollUntilDrained(isDrained: () => Promise<boolean>): Promise<boolean> {
  for (let poll = 0; poll < DRAIN_MAX_POLLS; poll += 1) {
    if (await isDrained()) {
      return true
    }
    await sleep(DRAIN_POLL_INTERVAL)
  }
  return false
}
