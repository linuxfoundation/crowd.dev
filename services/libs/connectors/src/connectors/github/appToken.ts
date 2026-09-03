import axios from 'axios'
import * as jwt from 'jsonwebtoken'

import {
  ConnectorError,
  ProviderAuthError,
  ProviderContractError,
  ProviderUnavailableError,
} from '../../http/errors'
import type { TokenMinter, TokenPool } from '../../pool/tokenPool'
import type { Credential, PoolPreparation } from '../../types'

const GITHUB_API_VERSION = '2022-11-28'

export const GITHUB_REQUEST_TIMEOUT_MS = 30_000

function mintAppJwt(credential: Credential): string {
  const now = Math.floor(Date.now() / 1000)
  return jwt.sign(
    { iat: now - 60, exp: now + 600, iss: credential.data.appId },
    credential.data.privateKey,
    { algorithm: 'RS256' },
  )
}

function classifyAppApiError(err: unknown, operation: string): ConnectorError | null {
  if (!axios.isAxiosError(err)) {
    return null
  }
  const status = err.response?.status
  const body = err.response?.data as { message?: string } | undefined
  const detail = body?.message ?? err.message
  const options = { status, cause: err }
  if (status === 401) {
    return new ProviderAuthError(`github app ${operation} unauthorized: ${detail}`, options)
  }
  if (status === undefined || status >= 500) {
    return new ProviderUnavailableError(`github app ${operation} failed: ${detail}`, options)
  }
  return new ProviderContractError(
    `github app ${operation} returned status ${status}: ${detail}`,
    options,
  )
}

export async function mintInstallationToken(
  credential: Credential,
  installationId: string,
): Promise<{ token: string; expiresAt: string }> {
  try {
    // POC only: minting cannot go through ConnectorHttp — it feeds the pool the client draws from
    const response = await axios.post(
      `https://api.github.com/app/installations/${installationId}/access_tokens`,
      {},
      {
        headers: {
          Authorization: `Bearer ${mintAppJwt(credential)}`,
          Accept: 'application/vnd.github+json',
          'X-GitHub-Api-Version': GITHUB_API_VERSION,
        },
        timeout: GITHUB_REQUEST_TIMEOUT_MS,
      },
    )
    return { token: response.data.token, expiresAt: response.data.expires_at }
  } catch (err) {
    throw classifyAppApiError(err, 'installation token minting') ?? err
  }
}

const INSTALLATIONS_PER_PAGE = 100
const MAX_INSTALLATION_PAGES = 100

export async function listInstallationIds(credential: Credential): Promise<string[]> {
  const appJwt = mintAppJwt(credential)
  const installationIds: string[] = []
  try {
    for (let page = 1; page <= MAX_INSTALLATION_PAGES; page++) {
      const response = await axios.get('https://api.github.com/app/installations', {
        headers: {
          Authorization: `Bearer ${appJwt}`,
          Accept: 'application/vnd.github+json',
          'X-GitHub-Api-Version': GITHUB_API_VERSION,
        },
        params: { per_page: INSTALLATIONS_PER_PAGE, page },
        timeout: GITHUB_REQUEST_TIMEOUT_MS,
      })
      const installations = response.data as { id: number }[]
      installationIds.push(...installations.map((installation) => String(installation.id)))
      if (installations.length < INSTALLATIONS_PER_PAGE) {
        return installationIds
      }
    }
  } catch (err) {
    throw classifyAppApiError(err, 'installation listing') ?? err
  }
  // truncating would silently shrink the pool, so refuse instead
  throw new Error(
    `github app has more than ${MAX_INSTALLATION_PAGES * INSTALLATIONS_PER_PAGE} installations`,
  )
}

// TODO(CM-1372): POC-only resolution; store the installation id per integration after the POC
export async function resolveInstallationId(credential: Credential): Promise<string> {
  const fromEnv = process.env.CROWD_GITHUB_INSTALLATION_ID
  if (fromEnv) {
    return fromEnv
  }

  const installationIds = await listInstallationIds(credential)
  if (installationIds.length === 0) {
    throw new Error('github app has no installations')
  }
  return installationIds[0]
}

const POOL_RESEED_INTERVAL_MS = 3_600_000

function poolInstallationAllowlist(): Set<string> | null {
  const raw = process.env.CROWD_GITHUB_POOL_INSTALLATION_IDS
  if (!raw) {
    return null
  }
  const ids = raw
    .split(',')
    .map((id) => id.trim())
    .filter(Boolean)
  return ids.length > 0 ? new Set(ids) : null
}

export async function prepareGithubPool(
  credential: Credential,
  pool: TokenPool,
): Promise<PoolPreparation> {
  const preferredEntryId = process.env.CROWD_GITHUB_INSTALLATION_ID
  if (!(await pool.needsSeed(POOL_RESEED_INTERVAL_MS))) {
    return { preferredEntryId }
  }
  const allowlist = poolInstallationAllowlist()
  const installationIds = (await listInstallationIds(credential)).filter(
    (id) => !allowlist || allowlist.has(id),
  )
  if (installationIds.length === 0) {
    throw new Error(
      allowlist
        ? 'none of CROWD_GITHUB_POOL_INSTALLATION_IDS match an installation of this github app'
        : 'github app has no installations',
    )
  }
  await pool.seed(installationIds)
  return { preferredEntryId: preferredEntryId ?? installationIds[0] }
}

export function createGithubTokenMinter(credential: Credential): TokenMinter {
  return (installationId) => mintInstallationToken(credential, installationId)
}
