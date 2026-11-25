import axios from 'axios'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { RateLimitError } from '../../http/errors'
import type { Credential } from '../../types'
import { createSessionizeSessionHttp } from './sessionHttp'

vi.mock('@crowd/logging', () => ({
  getServiceChildLogger: () => ({ debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() }),
}))

vi.mock('./session', async (importOriginal) => ({
  ...(await importOriginal<typeof import('./session')>()),
  signInToSessionize: vi.fn().mockResolvedValue({ token: 'made-up-cookie' }),
}))

const CREDENTIAL: Credential = {
  platform: 'sessionize',
  kind: 'sessionize-organizer-account',
  data: { username: 'organizer@example.org', password: 'made-up-password' },
}

const mockedRequest = vi.spyOn(axios, 'request')

describe('createSessionizeSessionHttp', () => {
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['Date'] })
    vi.setSystemTime(new Date('2026-11-25T10:00:00Z'))
  })

  afterEach(() => {
    vi.useRealTimers()
    mockedRequest.mockReset()
  })

  it('refuses requests while rate limited and serves them again once the limit has passed', async () => {
    mockedRequest
      .mockResolvedValueOnce({ status: 429, headers: { 'retry-after': '60' }, data: '' })
      .mockResolvedValueOnce({ status: 200, headers: {}, data: 'events page' })
    const http = createSessionizeSessionHttp(CREDENTIAL)
    const request = {
      url: 'https://sessionize.com/app/organizer/events',
      responseType: 'text' as const,
    }

    await expect(http.request(request)).rejects.toBeInstanceOf(RateLimitError)
    vi.setSystemTime(new Date('2026-11-25T10:00:30Z'))
    await expect(http.request(request)).rejects.toBeInstanceOf(RateLimitError)
    vi.setSystemTime(new Date('2026-11-25T10:01:01Z'))
    await expect(http.request(request)).resolves.toBe('events page')
    expect(mockedRequest).toHaveBeenCalledTimes(2)
  })
})
