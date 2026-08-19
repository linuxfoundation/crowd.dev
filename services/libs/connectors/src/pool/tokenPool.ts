import type { RedisClient } from '@crowd/redis'

import type { IPooledToken } from '../http/client'
import { ProviderAuthError, RateLimitError } from '../http/errors'

const PROBE_STALENESS_MS = 90_000

export interface BudgetSnapshot {
  limit: number
  remaining: number
  resetAt: Date
}

export type BudgetProbe = (
  platform: string,
  connectionId: string,
  tokenId: string,
) => Promise<BudgetSnapshot | null>

// POC only: the probe is the single source of truth for budgets (github /rate_limit is free and
// limits are per installation token); budgets for other platforms are a later decision.
export interface TokenPoolOptions {
  probeBudget?: BudgetProbe
}

export interface TokenPool {
  acquire(): Promise<IPooledToken>
  hasHeadroom(estimate: number): Promise<boolean>
  park(tokenId: string, resumeAt: Date): Promise<void>
  quarantine(tokenId: string): Promise<void>
  seed(tokenId: string, value: string): Promise<void>
  earliestResumeAt(): Promise<Date | null>
}

interface ITokenState {
  value: string
  parkedUntil?: string
  quarantined?: boolean
}

interface IBucket {
  limit: number
  remaining: number
  resetAtMs: number
  probedAtMs: number
}

export function createTokenPool(
  redis: RedisClient,
  platform: string,
  connectionId: string,
  options?: TokenPoolOptions,
): TokenPool {
  const tokensKey = `connectors:pool:${platform}:${connectionId}:tokens`
  const lruKey = `connectors:pool:${platform}:${connectionId}:lru`
  const bucketKey = (tokenId: string) =>
    `connectors:pool:${platform}:${connectionId}:budget:${tokenId}`

  async function readStates(): Promise<Map<string, ITokenState>> {
    const raw = await redis.hGetAll(tokensKey)
    const states = new Map<string, ITokenState>()
    for (const [id, json] of Object.entries(raw)) {
      states.set(id, JSON.parse(json) as ITokenState)
    }
    return states
  }

  function isHealthy(state: ITokenState, nowMs: number): boolean {
    if (state.quarantined) {
      return false
    }
    if (state.parkedUntil && new Date(state.parkedUntil).getTime() > nowMs) {
      return false
    }
    return true
  }

  function earliestParkedUntil(states: Map<string, ITokenState>, nowMs: number): Date | null {
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

  // POC only: read-modify-write can lose a concurrent park/quarantine on the same token
  // within a ~ms window; accepted tradeoff — fix with atomic writes when productizing.
  async function updateState(tokenId: string, update: Partial<ITokenState>): Promise<void> {
    const json = await redis.hGet(tokensKey, tokenId)
    if (!json) {
      return
    }
    const state = JSON.parse(json) as ITokenState
    await redis.hSet(tokensKey, tokenId, JSON.stringify({ ...state, ...update }))
  }

  async function readBucket(tokenId: string): Promise<IBucket | null> {
    const raw = await redis.hGetAll(bucketKey(tokenId))
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
    tokenId: string,
    nowMs: number,
  ): Promise<IBucket | null> {
    const bucket = await readBucket(tokenId)
    if (!needsProbe(bucket, nowMs)) {
      return bucket
    }
    const snapshot = await probe(platform, connectionId, tokenId)
    if (!snapshot) {
      return null
    }
    const probed = {
      limit: snapshot.limit,
      remaining: snapshot.remaining,
      resetAtMs: snapshot.resetAt.getTime(),
      probedAtMs: nowMs,
    }
    await redis.hSet(bucketKey(tokenId), {
      limit: String(probed.limit),
      remaining: String(probed.remaining),
      resetAt: String(probed.resetAtMs),
      probedAt: String(probed.probedAtMs),
    })
    return probed
  }

  return {
    async acquire(): Promise<IPooledToken> {
      const nowMs = Date.now()
      const states = await readStates()
      const ordered = await redis.zRange(lruKey, 0, -1)
      const probe = options?.probeBudget
      let earliestBudgetResetAt: Date | null = null
      for (const id of ordered) {
        const state = states.get(id)
        if (!state || !isHealthy(state, nowMs)) {
          continue
        }
        if (probe) {
          const bucket = await loadBucket(probe, id, nowMs)
          if (bucket && bucket.remaining <= 0) {
            const resetAt = new Date(bucket.resetAtMs)
            if (!earliestBudgetResetAt || resetAt < earliestBudgetResetAt) {
              earliestBudgetResetAt = resetAt
            }
            continue
          }
          if (bucket) {
            await redis.hIncrBy(bucketKey(id), 'remaining', -1)
          }
        }
        await redis.zAdd(lruKey, { score: nowMs, value: id })
        return { id, value: state.value }
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
      if (states.size === 0) {
        return true
      }
      let pooledRemaining = 0
      for (const [id, state] of states.entries()) {
        if (!isHealthy(state, nowMs)) {
          continue
        }
        const bucket = await loadBucket(probe, id, nowMs)
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

    async park(tokenId: string, resumeAt: Date): Promise<void> {
      await updateState(tokenId, { parkedUntil: resumeAt.toISOString() })
    },

    // POC only: quarantined tokens are kept for inspection and never revived automatically
    async quarantine(tokenId: string): Promise<void> {
      await updateState(tokenId, { quarantined: true })
    },

    async seed(tokenId: string, value: string): Promise<void> {
      const json = await redis.hGet(tokensKey, tokenId)
      const state = json ? (JSON.parse(json) as ITokenState) : {}
      await redis.hSet(tokensKey, tokenId, JSON.stringify({ ...state, value }))
      await redis.zAdd(lruKey, { score: 0, value: tokenId }, { NX: true })
    },

    async earliestResumeAt(): Promise<Date | null> {
      const states = await readStates()
      return earliestParkedUntil(states, Date.now())
    },
  }
}
