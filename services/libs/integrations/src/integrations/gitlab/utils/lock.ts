import { randomUUID } from 'crypto'

import { ICache } from '@crowd/types'

interface SemaphoreState {
  holders: { [lockId: string]: string } // lockId -> expiresAt ISO string
}

export class RedisSemaphore {
  private readonly key: string
  private readonly maxConcurrent: number
  private readonly cache: ICache
  private readonly timeout: number
  private readonly lockTTL: number
  private lockId: string | null = null

  constructor({
    integrationId,
    apiCallType,
    maxConcurrent,
    cache,
    timeout = 60000,
    lockTTL = 60, // 1 minute default TTL for individual locks
  }: {
    integrationId: string
    apiCallType: string
    maxConcurrent: number
    cache: ICache
    timeout?: number
    lockTTL?: number
  }) {
    this.key = `gitlab-semaphore:${integrationId}:${apiCallType}`
    this.maxConcurrent = maxConcurrent
    this.cache = cache
    this.timeout = timeout
    this.lockTTL = lockTTL
  }

  async acquire(): Promise<boolean> {
    const startTime = Date.now()
    this.lockId = randomUUID()

    while (Date.now() - startTime < this.timeout) {
      // Get current state and clean up expired locks
      const state = await this.getState()
      const now = new Date()

      // Filter out expired locks
      const activeHolders: { [lockId: string]: string } = {}
      for (const [lockId, expiresAtISO] of Object.entries(state.holders)) {
        const expiresAt = new Date(expiresAtISO)
        if (expiresAt > now) {
          activeHolders[lockId] = expiresAtISO
        }
      }

      const activeLocks = Object.keys(activeHolders).length

      if (activeLocks < this.maxConcurrent) {
        // Add our lock
        const expiresAt = new Date(Date.now() + this.lockTTL * 1000)
        activeHolders[this.lockId] = expiresAt.toISOString()

        // Save the updated state with TTL
        // TTL is 2x the lock TTL to ensure cleanup even if release fails
        await this.setState({ holders: activeHolders }, this.lockTTL * 2)

        return true
      }

      // Randomize timeout between 100ms and 300ms to avoid thundering herd
      const randomTimeout = Math.floor(Math.random() * (300 - 100 + 1)) + 100
      await new Promise((resolve) => setTimeout(resolve, randomTimeout))
    }
    throw new Error(`Failed to acquire lock within timeout period for ${this.key}`)
  }

  async release(): Promise<void> {
    if (!this.lockId) {
      return
    }

    // Get current state
    const state = await this.getState()
    const now = new Date()

    // Remove our lock and any expired locks
    const activeHolders: { [lockId: string]: string } = {}
    for (const [lockId, expiresAtISO] of Object.entries(state.holders)) {
      const expiresAt = new Date(expiresAtISO)
      // Keep locks that are not ours and not expired
      if (lockId !== this.lockId && expiresAt > now) {
        activeHolders[lockId] = expiresAtISO
      }
    }

    // Save the updated state
    if (Object.keys(activeHolders).length > 0) {
      await this.setState({ holders: activeHolders }, this.lockTTL * 2)
    } else {
      // No more holders, delete the key
      await this.cache.delete(this.key)
    }

    this.lockId = null
  }

  private async getState(): Promise<SemaphoreState> {
    const stateJson = await this.cache.get(this.key)
    if (!stateJson) {
      return { holders: {} }
    }

    try {
      // Try to parse as JSON first (new format)
      const parsed = JSON.parse(stateJson)

      // Check if it's the new format with holders object
      if (parsed && typeof parsed === 'object' && 'holders' in parsed) {
        return parsed as SemaphoreState
      }

      // If it's just a number in JSON, fall through to migration logic below
      if (typeof parsed === 'number') {
        return this.migrateLegacyState(parsed)
      }
    } catch {
      // If JSON parsing fails, it might be a plain integer string (old format)
      const legacyCount = parseInt(stateJson, 10)
      if (!isNaN(legacyCount) && legacyCount > 0) {
        return this.migrateLegacyState(legacyCount)
      }
    }

    // If all parsing attempts fail, return empty state
    return { holders: {} }
  }

  private migrateLegacyState(count: number): SemaphoreState {
    // Convert old integer count to new format
    // Create placeholder holders with the current count
    const holders: { [lockId: string]: string } = {}
    const expiresAt = new Date(Date.now() + this.lockTTL * 1000).toISOString()

    for (let i = 0; i < count; i++) {
      // Use predictable IDs for legacy locks so they can be tracked
      const legacyId = `legacy-${i}-${Date.now()}`
      holders[legacyId] = expiresAt
    }

    return { holders }
  }

  private async setState(state: SemaphoreState, ttlSeconds: number): Promise<void> {
    await this.cache.set(this.key, JSON.stringify(state), ttlSeconds)
  }
}
