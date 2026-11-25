import axios, { AxiosHeaders } from 'axios'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { ProviderAuthError, RateLimitError } from '../../http/errors'
import type { Credential } from '../../types'
import { signInToSessionize } from './session'
import { createSessionizeFixedSessionHttp, createSessionizeSessionHttp } from './sessionHttp'

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

describe('createSessionizeFixedSessionHttp', () => {
  const request = {
    url: 'https://sessionize.com/app/organizer/events',
    responseType: 'text' as const,
  }

  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['Date'] })
    vi.setSystemTime(new Date('2026-11-25T10:00:00Z'))
    vi.mocked(signInToSessionize).mockClear()
  })

  afterEach(() => {
    vi.useRealTimers()
    mockedRequest.mockReset()
  })

  it('sends the given cookie and never signs in', async () => {
    mockedRequest.mockResolvedValueOnce({ status: 200, headers: {}, data: 'events page' })

    await expect(
      createSessionizeFixedSessionHttp('made-up-fixed-cookie').request(request),
    ).resolves.toBe('events page')
    expect(AxiosHeaders.from(mockedRequest.mock.calls[0][0].headers).get('Cookie')).toBe(
      'made-up-fixed-cookie',
    )
    expect(signInToSessionize).not.toHaveBeenCalled()
  })

  it('fails as an auth error once the session is rejected, without signing in again', async () => {
    mockedRequest.mockResolvedValue({ status: 302, headers: { location: '/login' }, data: '' })
    const http = createSessionizeFixedSessionHttp('made-up-fixed-cookie')

    await expect(http.request(request)).rejects.toBeInstanceOf(ProviderAuthError)
    await expect(http.request(request)).rejects.toBeInstanceOf(ProviderAuthError)
    expect(mockedRequest).toHaveBeenCalledTimes(1)
    expect(signInToSessionize).not.toHaveBeenCalled()
  })

  it('refuses requests while rate limited and serves them again once the limit has passed', async () => {
    mockedRequest
      .mockResolvedValueOnce({ status: 429, headers: { 'retry-after': '60' }, data: '' })
      .mockResolvedValueOnce({ status: 200, headers: {}, data: 'events page' })
    const http = createSessionizeFixedSessionHttp('made-up-fixed-cookie')

    await expect(http.request(request)).rejects.toBeInstanceOf(RateLimitError)
    vi.setSystemTime(new Date('2026-11-25T10:00:30Z'))
    await expect(http.request(request)).rejects.toBeInstanceOf(RateLimitError)
    vi.setSystemTime(new Date('2026-11-25T10:01:01Z'))
    await expect(http.request(request)).resolves.toBe('events page')
    expect(mockedRequest).toHaveBeenCalledTimes(2)
    expect(signInToSessionize).not.toHaveBeenCalled()
  })
})
