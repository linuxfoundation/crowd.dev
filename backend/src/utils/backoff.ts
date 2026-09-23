import { timeout } from '@crowd/common'

export async function retryBackoff<T>(fn: () => Promise<T>, maxRetries: number = 3): Promise<T> {
  let retries = 0
  while (retries < maxRetries) {
    try {
      return await fn()
    } catch {
      retries++
      // Exponential backoff with base of 2 seconds
      // 1st retry: 2s, 2nd: 4s, 3rd: 8s, etc
      const backoffMs = 2000 * 2 ** (retries - 1)
      await timeout(backoffMs)
    }
  }

  throw new Error('Max retries reached')
}
