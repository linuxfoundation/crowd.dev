import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import type { Logger } from '@crowd/logging'
import type { RedisClient } from '@crowd/redis'

import { createHttpClient } from '../http/client'
import {
  ConnectorCodeError,
  ConnectorError,
  ProviderAuthError,
  ProviderUnavailableError,
  RateLimitError,
} from '../http/errors'
import { MINT_RETRY_DELAY_MS, type TokenMinter, createTokenPool } from './tokenPool'

const PLATFORM = 'example'
const ENTRIES_KEY = `connectors:pool:${PLATFORM}:entries`

function createFakeRedis(): RedisClient {
  const hashes = new Map<string, Map<string, string>>()
  const sortedSets = new Map<string, Map<string, number>>()
  const hash = (key: string) => hashes.get(key) ?? hashes.set(key, new Map()).get(key)!
  const sortedSet = (key: string) => sortedSets.get(key) ?? sortedSets.set(key, new Map()).get(key)!

  const fake = {
    hGetAll: async (key: string) => Object.fromEntries(hash(key)),
    hSet: async (key: string, values: Record<string, string>) => {
      Object.entries(values).forEach(([field, value]) => hash(key).set(field, value))
    },
    hIncrBy: async (key: string, field: string, by: number) => {
      hash(key).set(field, String(Number(hash(key).get(field) ?? 0) + by))
    },
    zRange: async (key: string) =>
      [...sortedSet(key).entries()].sort((a, b) => a[1] - b[1]).map(([member]) => member),
    zAdd: async (key: string, { score, value }: { score: number; value: string }) => {
      sortedSet(key).set(value, score)
    },
    eval: async (
      _script: string,
      { keys, arguments: args }: { keys: string[]; arguments: string[] },
    ) => {
      const [entryId, rawUpdate] = args
      const current = hash(keys[0]).get(entryId)
      if (!current) {
        return 0
      }
      const state = JSON.parse(current) as Record<string, unknown>
      for (const [field, value] of Object.entries(
        JSON.parse(rawUpdate) as Record<string, unknown>,
      )) {
        if (value === null) {
          delete state[field]
        } else {
          state[field] = value
        }
      }
      hash(keys[0]).set(entryId, JSON.stringify(state))
      return 1
    },
  }
  return fake as unknown as RedisClient
}

async function poolWithEntries(entries: Record<string, object>, mintToken: TokenMinter) {
  const redis = createFakeRedis()
  await redis.hSet(
    ENTRIES_KEY,
    Object.fromEntries(Object.entries(entries).map(([id, state]) => [id, JSON.stringify(state)])),
  )
  for (const [index, id] of Object.keys(entries).entries()) {
    await redis.zAdd(`connectors:pool:${PLATFORM}:lru`, { score: index, value: id })
  }
  return createTokenPool(redis, PLATFORM, { mintToken })
}

function minterFailingWith(failures: Record<string, ConnectorError>): TokenMinter {
  return vi.fn(async (entryId: string) => {
    if (failures[entryId]) {
      throw failures[entryId]
    }
    return { token: `minted-${entryId}`, expiresAt: new Date(Date.now() + 3_600_000).toISOString() }
  })
}

describe('token pool acquire', () => {
  const NOW = new Date('2026-11-26T10:00:00.000Z')
  const at = (offsetMs: number) => new Date(NOW.getTime() + offsetMs)
  const exhaustedUntil = (resumeAt: Date) =>
    new RateLimitError('token pool exhausted', { resumeAt })
  const retryMintAt = (resumeAt: Date) =>
    new RateLimitError('token pool has no usable token, waiting to retry a failed mint', {
      resumeAt,
    })
  const empty = new ProviderAuthError('token pool empty')

  beforeEach(() => {
    vi.useFakeTimers({ now: NOW, toFake: ['Date'] })
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it.each<[string, Record<string, object>, Record<string, ConnectorError>, ConnectorError]>([
    [
      'a transient sign-in failure of the only entry waits for the mint retry delay',
      { only: {} },
      { only: new ProviderUnavailableError('sign-in page down') },
      retryMintAt(at(MINT_RETRY_DELAY_MS)),
    ],
    [
      'a rate-limited mint that may retry sooner still waits for the mint retry delay',
      { only: {} },
      { only: new RateLimitError('slow down', { resumeAt: at(120_000) }) },
      retryMintAt(at(MINT_RETRY_DELAY_MS)),
    ],
    [
      'a rate-limited mint that must wait longer passes that time on',
      { only: {} },
      { only: new RateLimitError('slow down', { resumeAt: at(900_000) }) },
      retryMintAt(at(900_000)),
    ],
    [
      'a parked entry decides the wait even when it frees up after the mint retry',
      { parked: { parkedUntil: at(900_000).toISOString() }, failing: {} },
      { failing: new ProviderUnavailableError('sign-in page down') },
      exhaustedUntil(at(900_000)),
    ],
    [
      'one quarantined and one transiently failing entry waits for the mint retry delay',
      { broken: { quarantined: true }, failing: {} },
      { failing: new RateLimitError('slow down') },
      retryMintAt(at(MINT_RETRY_DELAY_MS)),
    ],
    [
      'a rejected sign-in quarantines the entry and leaves the pool empty',
      { only: {} },
      { only: new ProviderAuthError('bad password') },
      empty,
    ],
    [
      'a mint failure of any other class leaves the pool empty',
      { only: {} },
      { only: new ConnectorCodeError('minter bug') },
      empty,
    ],
  ])('%s', async (_name, entries, failures, expected) => {
    const pool = await poolWithEntries(entries, minterFailingWith(failures))

    const error = (await pool.acquire().catch((err: unknown) => err)) as ConnectorError

    expect(error).toBeInstanceOf(expected.constructor)
    expect([error.message, error.options?.resumeAt]).toEqual([
      expected.message,
      expected.options?.resumeAt,
    ])
  })

  it('makes a request mint once and fail without retrying when the only mint fails transiently', async () => {
    const mint = minterFailingWith({ only: new ProviderUnavailableError('sign-in page down') })
    const pool = await poolWithEntries({ only: {} }, mint)
    const http = createHttpClient({
      acquireToken: () => pool.acquire('only'),
      parkToken: pool.park,
      invalidateToken: pool.invalidate,
      log: { info: vi.fn(), warn: vi.fn() } as unknown as Logger,
    })

    await expect(http.request({ url: 'https://example.test/items' })).rejects.toMatchObject({
      errorClass: 'provider.rate_limit',
      options: { resumeAt: at(MINT_RETRY_DELAY_MS) },
    })
    expect(mint).toHaveBeenCalledTimes(1)
  })

  it('borrows the next entry when one installation fails to mint, and quarantines nothing', async () => {
    const mint = minterFailingWith({ first: new ProviderUnavailableError('github 502') })
    const pool = await poolWithEntries({ first: {}, second: {} }, mint)

    await expect(pool.acquire('first')).resolves.toEqual({ id: 'second', value: 'minted-second' })
    await expect(pool.acquire('first')).resolves.toEqual({ id: 'second', value: 'minted-second' })
    expect(mint).toHaveBeenCalledTimes(3)
  })
})
