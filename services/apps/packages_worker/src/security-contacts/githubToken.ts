import { getServiceChildLogger } from '@crowd/logging'

import { getGithubAppConfig } from '../config'
import {
  GithubAppConfig,
  fetchRateLimitDiagnostics,
  getInstallationToken,
  resolveInstallations,
} from '../enricher/githubAppAuth'
import { InstallationPool } from '../enricher/installationPool'

const log = getServiceChildLogger('security-contacts:github-token')

interface Pool {
  pool: InstallationPool
  appConfig: GithubAppConfig
}

// Module-scoped so installations are resolved once and reused across activity invocations.
let cached: Pool | null = null
let initPromise: Promise<Pool | null> | null = null

async function ensurePool(): Promise<Pool | null> {
  if (cached) return cached
  if (!initPromise) {
    initPromise = (async () => {
      try {
        const appConfig = getGithubAppConfig()
        const discovered = await resolveInstallations(appConfig)
        if (discovered.length === 0) {
          log.warn('No GitHub App installations — authed extractors will run unauthenticated')
          return null
        }
        const healthy = await fetchRateLimitDiagnostics(
          appConfig.appId,
          appConfig.privateKeyPem,
          discovered,
        )
        cached = { pool: new InstallationPool(healthy.length ? healthy : discovered), appConfig }
        return cached
      } catch (err) {
        log.warn(
          { errMsg: (err as Error).message },
          'GitHub token pool unavailable — running unauthenticated',
        )
        return null
      }
    })()
  }
  return initPromise
}

/**
 * Returns an installation token (round-robined across the pool) or null when the
 * GitHub App is unavailable, so the authed extractors degrade to unauthenticated calls.
 */
export async function getSecurityContactsToken(): Promise<string | null> {
  const resolved = await ensurePool()
  if (!resolved) return null
  try {
    const { installationId } = resolved.pool.select()
    return await getInstallationToken(
      resolved.appConfig.appId,
      resolved.appConfig.privateKeyPem,
      installationId,
    )
  } catch (err) {
    log.warn(
      { errMsg: (err as Error).message },
      'Token mint failed — falling back to unauthenticated',
    )
    return null
  }
}
