import axios from 'axios'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import type { Credential } from '../../types'
import { parseActivatedEvents } from './consoleEvents'
import { SESSIONIZE_CONSOLE_FIXTURE } from './consoleEvents.fixture'
import { createSessionizeDeps } from './deps'
import { discoverSessionize } from './discover'
import { sessionizeConnector } from './index'
import { SESSIONIZE_POOL_ENTRY_ID, signInToSessionize } from './session'
import { runEventExport } from './syncs/eventExport'

vi.mock('./session', async (importOriginal) => ({
  ...(await importOriginal<typeof import('./session')>()),
  signInToSessionize: vi.fn(),
}))

vi.mock('@crowd/logging', () => ({
  getServiceChildLogger: () => ({ debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() }),
}))

const FIXTURE = SESSIONIZE_CONSOLE_FIXTURE

const CREDENTIAL: Credential = {
  platform: 'sessionize',
  kind: 'sessionize-organizer-account',
  data: { username: 'organizer@example.org', password: 'secret' },
}

const mockedSignIn = vi.mocked(signInToSessionize)
const mockedRequest = vi.spyOn(axios, 'request')

describe('discoverSessionize', () => {
  beforeEach(() => {
    mockedSignIn.mockReset()
    mockedRequest.mockReset()
    mockedSignIn.mockResolvedValue({ token: 'cookie=1', expiresAt: '2026-12-01T00:00:00Z' })
    mockedRequest.mockResolvedValue({ status: 200, headers: {}, data: FIXTURE })
  })

  it('returns one channel per activated event, signing in once', async () => {
    const channels = await discoverSessionize(CREDENTIAL)

    expect(channels).toEqual(
      parseActivatedEvents(FIXTURE).map((event) => ({
        channelId: event.eventKey,
        channelName: event.eventName,
      })),
    )
    expect(channels.length).toBeGreaterThan(0)
    expect(mockedSignIn).toHaveBeenCalledTimes(1)
    expect(mockedRequest).toHaveBeenCalledTimes(1)
  })

  it('ends the call on a rate limit after a single request', async () => {
    mockedRequest.mockResolvedValue({ status: 429, headers: { 'retry-after': '60' }, data: '' })

    await expect(discoverSessionize(CREDENTIAL)).rejects.toMatchObject({
      errorClass: 'provider.rate_limit',
    })
    expect(mockedRequest).toHaveBeenCalledTimes(1)
  })

  it('signs in again once after a login redirect', async () => {
    mockedRequest
      .mockResolvedValueOnce({ status: 302, headers: { location: '/login' }, data: '' })
      .mockResolvedValueOnce({ status: 200, headers: {}, data: FIXTURE })

    await expect(discoverSessionize(CREDENTIAL)).resolves.not.toHaveLength(0)
    expect(mockedSignIn).toHaveBeenCalledTimes(2)
  })

  it('rejects a credential of another kind without signing in', async () => {
    const credential: Credential = {
      platform: 'octolens',
      kind: 'octolens-token',
      data: { token: 'token' },
    }

    await expect(discoverSessionize(credential)).rejects.toThrow(
      'expected sessionize-organizer-account credential, got octolens-token',
    )
    expect(mockedSignIn).not.toHaveBeenCalled()
  })

  it('fails instead of returning a partial list when the page is broken', async () => {
    mockedRequest.mockResolvedValue({ status: 200, headers: {}, data: '<html></html>' })

    await expect(discoverSessionize(CREDENTIAL)).rejects.toMatchObject({
      errorClass: 'provider.contract',
    })
  })
})

describe('sessionizeConnector', () => {
  it('registers the sessionize platform with its one paced eventExport sync', () => {
    expect(sessionizeConnector.platform).toBe('sessionize')
    expect(sessionizeConnector.syncs).toEqual([
      expect.objectContaining({ name: 'eventExport', cadenceMinutes: 1440, run: runEventExport }),
    ])
    expect(sessionizeConnector.createDeps).toBe(createSessionizeDeps)
    expect(sessionizeConnector.maxConcurrentRuns).toBe(1)
    expect(sessionizeConnector.discover).toBe(discoverSessionize)
    expect(SESSIONIZE_POOL_ENTRY_ID).toBe('sessionize-organizer')
  })
})
