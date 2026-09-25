import { describe, expect, test, vi } from 'vitest'

import { withTimeout } from './withTimeout'

describe('withTimeout', () => {
  test('resolves with the wrapped promise value when it settles before the deadline', async () => {
    await expect(withTimeout(Promise.resolve('ok'), 1000, 'timed out')).resolves.toBe('ok')
  })

  test('rejects with the timeout message once the deadline elapses without settling', async () => {
    vi.useFakeTimers()
    const result = withTimeout(new Promise(() => {}), 1000, 'timed out')
    const assertion = expect(result).rejects.toThrow('timed out')
    await vi.advanceTimersByTimeAsync(1000)
    await assertion
    vi.useRealTimers()
  })
})
