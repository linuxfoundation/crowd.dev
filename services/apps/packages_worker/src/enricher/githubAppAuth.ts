import jwt from 'jsonwebtoken'

import { getServiceChildLogger } from '@crowd/logging'

const log = getServiceChildLogger('github-app-auth')

const GITHUB_API = 'https://api.github.com'

// Per-installation token cache: installationId -> { token, expiresAt }
const tokenCache = new Map<number, { token: string; expiresAt: Date }>()

/**
 * Build a short-lived GitHub App JWT (valid for 10 minutes, accepted for ~9m50s
 * by GitHub after the 60s iat backdate). Cheap to build — we regenerate on each mint.
 */
function buildAppJwt(appId: string, privateKeyPem: string): string {
  const now = Math.floor(Date.now() / 1000)
  return jwt.sign({ iat: now - 60, exp: now + 10 * 60, iss: appId }, privateKeyPem, {
    algorithm: 'RS256',
  })
}

/**
 * Enumerate all installations of the GitHub App by paginating GET /app/installations.
 * Returns a list of installation ids.
 */
async function listInstallationIds(appJwt: string): Promise<number[]> {
  const ids: number[] = []
  let page = 1

  for (;;) {
    const resp = await fetch(`${GITHUB_API}/app/installations?per_page=100&page=${page}`, {
      headers: {
        Authorization: `Bearer ${appJwt}`,
        Accept: 'application/vnd.github+json',
      },
    })

    if (!resp.ok) {
      const body = await resp.text()
      throw new Error(`Failed to list installations (${resp.status}): ${body}`)
    }

    const data = (await resp.json()) as Array<{ id: number }>
    if (data.length === 0) break

    for (const inst of data) ids.push(inst.id)

    if (data.length < 100) break
    page++
  }

  return ids
}

/**
 * Mint (or return a cached) installation access token for the given installation id.
 * Refreshes automatically when the cached token is within 5 minutes of expiry.
 */
export async function getInstallationToken(
  appId: string,
  privateKeyPem: string,
  installationId: number,
): Promise<string> {
  const cached = tokenCache.get(installationId)
  const fiveMinFromNow = new Date(Date.now() + 5 * 60 * 1000)

  if (cached && cached.expiresAt > fiveMinFromNow) {
    return cached.token
  }

  const appJwt = buildAppJwt(appId, privateKeyPem)
  const resp = await fetch(`${GITHUB_API}/app/installations/${installationId}/access_tokens`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${appJwt}`,
      Accept: 'application/vnd.github+json',
    },
  })

  if (!resp.ok) {
    const body = await resp.text()
    throw new Error(
      `Failed to mint token for installation ${installationId} (${resp.status}): ${body}`,
    )
  }

  const data = (await resp.json()) as { token: string; expires_at: string }
  tokenCache.set(installationId, { token: data.token, expiresAt: new Date(data.expires_at) })

  return data.token
}

interface RateLimitEntry {
  installationId: number
  limit: number
  remaining: number
  used: number
  resetAt: string
}

/**
 * Fetches GraphQL rate limit info for every installation in parallel (one-time startup diagnostic).
 * GET /rate_limit does not consume quota.
 */
export async function fetchRateLimitDiagnostics(
  appId: string,
  privateKeyPem: string,
  installationIds: number[],
): Promise<void> {
  const results = await Promise.all(
    installationIds.map(
      async (id): Promise<RateLimitEntry | { installationId: number; error: string }> => {
        try {
          const token = await getInstallationToken(appId, privateKeyPem, id)
          const resp = await fetch(`${GITHUB_API}/rate_limit`, {
            headers: { Authorization: `bearer ${token}`, Accept: 'application/vnd.github+json' },
          })
          if (!resp.ok) return { installationId: id, error: `HTTP ${resp.status}` }
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          const data = (await resp.json()) as any
          const g = data.resources?.graphql
          return {
            installationId: id,
            limit: g.limit,
            remaining: g.remaining,
            used: g.used,
            resetAt: new Date(g.reset * 1000).toISOString(),
          }
        } catch (err) {
          return { installationId: id, error: (err as Error).message }
        }
      },
    ),
  )

  let totalLimit = 0
  let totalRemaining = 0
  let failures = 0
  for (const r of results) {
    if ('error' in r) {
      failures++
    } else {
      totalLimit += r.limit
      totalRemaining += r.remaining
    }
  }

  log.info(
    {
      installations: installationIds.length,
      failures,
      totalLimit,
      totalRemaining,
      totalUsed: totalLimit - totalRemaining,
    },
    'GitHub App rate limit capacity',
  )
}

export interface GithubAppConfig {
  appId: string
  privateKeyPem: string
  /** Override list from env; if empty, all installations are enumerated from the API. */
  installationIdOverrides: number[]
}

/**
 * Resolve the list of installation ids to use for the token pool.
 * Uses env override if set; otherwise enumerates all installations from the GitHub API.
 */
export async function resolveInstallations(config: GithubAppConfig): Promise<number[]> {
  if (config.installationIdOverrides.length > 0) {
    log.info(
      { count: config.installationIdOverrides.length },
      'Using installation id override list from env',
    )
    return config.installationIdOverrides
  }

  const appJwt = buildAppJwt(config.appId, config.privateKeyPem)
  const ids = await listInstallationIds(appJwt)
  log.info({ count: ids.length }, 'Discovered GitHub App installations')
  return ids
}
