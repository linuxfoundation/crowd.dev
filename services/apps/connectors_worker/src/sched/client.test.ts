import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { SchedCallError, SchedRateLimitError, callSched } from './client'

const KEY = 'test-key-0123456789'

function stubFetch(status: number, body: string) {
  const fetchMock = vi.fn().mockImplementation(async () => new Response(body, { status }))
  vi.stubGlobal('fetch', fetchMock)
  return fetchMock
}

describe('callSched', () => {
  // The client keeps the last call time between tests, so each test starts an hour later.
  let clock = new Date('2026-01-01T00:00:00Z').getTime()
  beforeEach(() => {
    vi.useFakeTimers()
    clock += 3_600_000
    vi.setSystemTime(clock)
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.unstubAllGlobals()
  })

  it('waits 5 seconds between calls', async () => {
    const fetchMock = stubFetch(200, '[]')
    await callSched('testevent2021', KEY, 'user/list')
    const second = callSched('testevent2021', KEY, 'user/list')
    await vi.advanceTimersByTimeAsync(4_999)
    expect(fetchMock).toHaveBeenCalledTimes(1)

    await vi.advanceTimersByTimeAsync(1)
    await second
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })

  it('stops on HTTP 429', async () => {
    stubFetch(429, 'Too Many Requests')
    await expect(callSched('testevent2021', KEY, 'user/list')).rejects.toBeInstanceOf(
      SchedRateLimitError,
    )
  })

  it('treats an ERR: body on HTTP 200 as an authorization failure', async () => {
    stubFetch(200, 'ERR: Authorization failed.')
    const error = await callSched('testevent2021', KEY, 'user/list').catch(
      (err: SchedCallError) => err,
    )
    expect(error).toBeInstanceOf(SchedCallError)
    expect((error as SchedCallError).authFailed).toBe(true)
  })

  it.each([
    [429, 'rate limited'],
    [200, `ERR: Authorization failed for ${KEY}`],
    [500, `server error, api_key=${KEY}`],
    [200, `not json ${KEY}`],
  ])('keeps the key out of the error on HTTP %i', async (status, body) => {
    stubFetch(status, body)
    const error = await callSched('testevent2021', KEY, 'user/list').catch((err: Error) => err)
    expect(error).toBeInstanceOf(Error)
    expect((error as Error).message).not.toContain(KEY)
  })
})
