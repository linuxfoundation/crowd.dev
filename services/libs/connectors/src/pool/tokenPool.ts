import type { Logger } from '@crowd/logging'
import type { RedisClient } from '@crowd/redis'

import type { IPooledToken } from '../http/client'
import { ConnectorError, ProviderAuthError, RateLimitError } from '../http/errors'

const PROBE_STALENESS_MS = 90_000
const TOKEN_EXPIRY_MARGIN_MS = 120_000

export interface BudgetSnapshot {
  limit: number
  remaining: number
  resetAt: Date
}

export type BudgetProbe = (platform: string, token: IPooledToken) => Promise<BudgetSnapshot | null>

export type TokenMinter = (entryId: string) => Promise<{ token: string; expiresAt: string }>

// POC only: the probe is the single source of truth for budgets (github /rate_limit is free and
// limits are per installation, surviving token re-mints); other platforms are a later decision.
export interface TokenPoolOptions {
  probeBudget?: BudgetProbe
  mintToken?: TokenMinter
  log?: Logger
}

export interface TokenPool {
  acquire(preferredEntryId?: string): Promise<IPooledToken>
  hasHeadroom(estimate: number): Promise<boolean>
  park(entryId: string, resumeAt: Date): Promise<void>
  invalidate(entryId: string): Promise<void>
  quarantine(entryId: string): Promise<void>
  seed(entryIds: string[]): Promise<void>
  needsSeed(maxAgeMs: number): Promise<boolean>
  earliestResumeAt(): Promise<Date | null>
}

interface IEntryState {
  token?: string
  tokenExpiresAtMs?: number
  parkedUntil?: string
  quarantined?: boolean
}

type EntryStateUpdate = { [K in keyof IEntryState]?: IEntryState[K] | null }

// atomic get-merge-set per entry; JSON null in the update deletes the field
const MERGE_ENTRY_STATE_SCRIPT = `
local json = redis.call('HGET', KEYS[1], ARGV[1])
if not json then
  return 0
end
local state = cjson.decode(json)
local update = cjson.decode(ARGV[2])
for field, value in pairs(update) do
  if value == cjson.null then
    state[field] = nil
  else
    state[field] = value
  end
end
redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(state))
return 1
`

interface IBucket {
  limit: number
  remaining: number
  resetAtMs: number
  probedAtMs: number
}

export function createTokenPool(
  redis: RedisClient,
  platform: string,
  options?: TokenPoolOptions,
): TokenPool {
  const entriesKey = `connectors:pool:${platform}:entries`
  const lruKey = `connectors:pool:${platform}:lru`
  const seededAtKey = `connectors:pool:${platform}:seededAt`
  const log = options?.log

  const loggedSkips = new Set<string>()
  let lastAcquiredEntryId: string | null = null

  function logSkip(entryId: string, reason: string, fields: Record<string, unknown> = {}): void {
    const key = `${entryId}:${reason}`
    if (!log || loggedSkips.has(key)) {
      return
    }
    loggedSkips.add(key)
    log.info({ event: 'pool_entry_skipped', entryId, reason, ...fields }, 'pool entry skipped')
  }

  function logAcquired(entryId: string, preferredEntryId?: string): void {
    if (!log || lastAcquiredEntryId === entryId) {
      return
    }
    lastAcquiredEntryId = entryId
    log.info(
      {
        event: 'pool_token_acquired',
        entryId,
        preferredEntryId,
        borrowed: preferredEntryId ? entryId !== preferredEntryId : false,
      },
      'pool token acquired',
    )
  }
  const bucketKey = (entryId: string) => `connectors:pool:${platform}:budget:${entryId}`

  async function readStates(): Promise<Map<string, IEntryState>> {
    const raw = await redis.hGetAll(entriesKey)
    const states = new Map<string, IEntryState>()
    for (const [id, json] of Object.entries(raw)) {
      states.set(id, JSON.parse(json) as IEntryState)
    }
    return states
  }

  function isHealthy(state: IEntryState, nowMs: number): boolean {
    if (state.quarantined) {
      return false
    }
    if (state.parkedUntil && new Date(state.parkedUntil).getTime() > nowMs) {
      return false
    }
    return true
  }

  function usableToken(state: IEntryState, nowMs: number): string | null {
    if (!state.token || !state.tokenExpiresAtMs) {
      return null
    }
    if (state.tokenExpiresAtMs - TOKEN_EXPIRY_MARGIN_MS <= nowMs) {
      return null
    }
    return state.token
  }

  function earliestParkedUntil(states: Map<string, IEntryState>, nowMs: number): Date | null {
    let earliest: Date | null = null
    for (const state of states.values()) {
      if (state.quarantined || !state.parkedUntil) {
        continue
      }
      const parkedUntil = new Date(state.parkedUntil)
      if (parkedUntil.getTime() <= nowMs) {
        continue
      }
      if (!earliest || parkedUntil < earliest) {
        earliest = parkedUntil
      }
    }
    return earliest
  }

  async function updateState(entryId: string, update: EntryStateUpdate): Promise<void> {
    await redis.eval(MERGE_ENTRY_STATE_SCRIPT, {
      keys: [entriesKey],
      arguments: [entryId, JSON.stringify(update)],
    })
  }

  async function ensureUsableToken(
    entryId: string,
    state: IEntryState,
    nowMs: number,
  ): Promise<string | null> {
    const cached = usableToken(state, nowMs)
    if (cached) {
      return cached
    }
    const mint = options?.mintToken
    if (!mint) {
      return null
    }
    try {
      const { token, expiresAt } = await mint(entryId)
      await updateState(entryId, { token, tokenExpiresAtMs: new Date(expiresAt).getTime() })
      log?.info({ event: 'pool_token_minted', entryId, expiresAt }, 'pool token minted')
      return token
    } catch (err) {
      if (err instanceof ConnectorError) {
        if (err.errorClass === 'provider.auth' || err.errorClass === 'provider.contract') {
          await updateState(entryId, { quarantined: true })
          log?.warn(
            {
              event: 'pool_entry_quarantined',
              entryId,
              errorClass: err.errorClass,
              errMsg: err.message,
            },
            'pool entry quarantined on mint failure',
          )
          return null
        }
        log?.warn(
          { event: 'pool_mint_failed', entryId, errorClass: err.errorClass, errMsg: err.message },
          'token mint failed, skipping entry',
        )
        return null
      }
      throw err
    }
  }

  async function readBucket(entryId: string): Promise<IBucket | null> {
    const raw = await redis.hGetAll(bucketKey(entryId))
    if (!raw.probedAt) {
      return null
    }
    return {
      limit: Number(raw.limit),
      remaining: Number(raw.remaining),
      resetAtMs: Number(raw.resetAt),
      probedAtMs: Number(raw.probedAt),
    }
  }

  function needsProbe(bucket: IBucket | null, nowMs: number): boolean {
    return !bucket || nowMs - bucket.probedAtMs > PROBE_STALENESS_MS || nowMs >= bucket.resetAtMs
  }

  async function loadBucket(
    probe: BudgetProbe,
    entryId: string,
    token: string | null,
    nowMs: number,
  ): Promise<IBucket | null> {
    const bucket = await readBucket(entryId)
    if (!needsProbe(bucket, nowMs)) {
      return bucket
    }
    if (!token) {
      return null
    }
    const snapshot = await probe(platform, { id: entryId, value: token })
    if (!snapshot) {
      return null
    }
    const probed = {
      limit: snapshot.limit,
      remaining: snapshot.remaining,
      resetAtMs: snapshot.resetAt.getTime(),
      probedAtMs: nowMs,
    }
    await redis.hSet(bucketKey(entryId), {
      limit: String(probed.limit),
      remaining: String(probed.remaining),
      resetAt: String(probed.resetAtMs),
      probedAt: String(probed.probedAtMs),
    })
    return probed
  }

  return {
    async acquire(preferredEntryId?: string): Promise<IPooledToken> {
      const nowMs = Date.now()
      const states = await readStates()
      const lruOrder = await redis.zRange(lruKey, 0, -1)
      const ordered =
        preferredEntryId && states.has(preferredEntryId)
          ? [preferredEntryId, ...lruOrder.filter((id) => id !== preferredEntryId)]
          : lruOrder
      const probe = options?.probeBudget
      let earliestBudgetResetAt: Date | null = null
      for (const id of ordered) {
        const state = states.get(id)
        if (!state) {
          continue
        }
        if (state.quarantined) {
          logSkip(id, 'quarantined')
          continue
        }
        if (state.parkedUntil && new Date(state.parkedUntil).getTime() > nowMs) {
          logSkip(id, 'parked', { parkedUntil: state.parkedUntil })
          continue
        }
        const token = await ensureUsableToken(id, state, nowMs)
        if (!token) {
          continue
        }
        if (probe) {
          const bucket = await loadBucket(probe, id, token, nowMs)
          if (bucket && bucket.remaining <= 0) {
            const resetAt = new Date(bucket.resetAtMs)
            if (!earliestBudgetResetAt || resetAt < earliestBudgetResetAt) {
              earliestBudgetResetAt = resetAt
            }
            logSkip(id, 'budget_exhausted', { resetAt })
            continue
          }
          if (bucket) {
            await redis.hIncrBy(bucketKey(id), 'remaining', -1)
          }
        }
        await redis.zAdd(lruKey, { score: nowMs, value: id })
        logAcquired(id, preferredEntryId)
        return { id, value: token }
      }
      const parkedResumeAt = earliestParkedUntil(states, nowMs)
      const resumeAt =
        parkedResumeAt && earliestBudgetResetAt
          ? new Date(Math.min(parkedResumeAt.getTime(), earliestBudgetResetAt.getTime()))
          : (parkedResumeAt ?? earliestBudgetResetAt)
      if (resumeAt) {
        throw new RateLimitError('token pool exhausted', { resumeAt })
      }
      throw new ProviderAuthError('token pool empty')
    },

    async hasHeadroom(estimate: number): Promise<boolean> {
      const probe = options?.probeBudget
      if (!probe) {
        return true
      }
      const nowMs = Date.now()
      const states = await readStates()
      const healthy = [...states.entries()].filter(([, state]) => isHealthy(state, nowMs))
      if (healthy.length === 0) {
        return states.size === 0
      }
      let pooledRemaining = 0
      for (const [id, state] of healthy) {
        const bucket = await loadBucket(probe, id, usableToken(state, nowMs), nowMs)
        if (!bucket) {
          return true
        }
        pooledRemaining += bucket.remaining
        if (pooledRemaining >= estimate) {
          return true
        }
      }
      return false
    },

    async park(entryId: string, resumeAt: Date): Promise<void> {
      await updateState(entryId, { parkedUntil: resumeAt.toISOString() })
    },

    async invalidate(entryId: string): Promise<void> {
      await updateState(entryId, { token: null, tokenExpiresAtMs: null })
    },

    // POC only: quarantined entries are kept for inspection and never revived automatically
    async quarantine(entryId: string): Promise<void> {
      await updateState(entryId, { quarantined: true })
    },

    async seed(entryIds: string[]): Promise<void> {
      if (entryIds.length === 0) {
        return
      }
      const multi = redis.multi()
      for (const id of entryIds) {
        multi.hSetNX(entriesKey, id, '{}')
        multi.zAdd(lruKey, { score: 0, value: id }, { NX: true })
      }
      multi.set(seededAtKey, String(Date.now()))
      await multi.exec()
    },

    async needsSeed(maxAgeMs: number): Promise<boolean> {
      const seededAt = await redis.get(seededAtKey)
      return !seededAt || Date.now() - Number(seededAt) > maxAgeMs
    },

    async earliestResumeAt(): Promise<Date | null> {
      const states = await readStates()
      return earliestParkedUntil(states, Date.now())
    },
  }
}
