import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import type { Credential } from './types'

export async function getCredential(qx: QueryExecutor, integrationId: string): Promise<Credential> {
  const integration: { platform: string } | null = await qx.selectOneOrNone(
    `SELECT platform
     FROM integrations
     WHERE id = $(integrationId) AND "deletedAt" IS NULL`,
    { integrationId },
  )

  if (!integration) {
    throw new Error(`integration ${integrationId} not found`)
  }

  switch (integration.platform) {
    case 'github':
    case 'github-nango':
      return githubAppCredential()
    case 'octolens':
      return octolensTokenCredential()
    case 'credly':
      return credlyOAuthClientCredential()
    default:
      throw new Error(`unsupported platform ${integration.platform}`)
  }
}

// POC only: secrets come from env; secret-manager adoption (OCI Vault) swaps this
// body without touching callers — getCredential stays the single entry point
function githubAppCredential(): Credential {
  const appId = process.env.CROWD_GITHUB_APP_ID
  const rawPrivateKey = process.env.CROWD_GITHUB_PRIVATE_KEY

  if (!appId || !rawPrivateKey) {
    throw new Error('missing CROWD_GITHUB_APP_ID or CROWD_GITHUB_PRIVATE_KEY environment variables')
  }

  const privateKey = rawPrivateKey.startsWith('-----')
    ? rawPrivateKey
    : Buffer.from(rawPrivateKey, 'base64').toString('ascii')

  return {
    platform: 'github',
    kind: 'github-app',
    data: { appId, privateKey },
  }
}

// Lets scheduled Octolens work skip environments that never got a token, instead of failing hourly.
export function isOctolensCredentialConfigured(): boolean {
  return Boolean(process.env.CROWD_OCTOLENS_API_TOKEN)
}

// Octolens is one shared org-wide bearer token, not per-integration like GitHub's app
// installations — every octolens integration resolves to the same env-provided token.
function octolensTokenCredential(): Credential {
  const token = process.env.CROWD_OCTOLENS_API_TOKEN

  if (!token) {
    throw new Error('missing CROWD_OCTOLENS_API_TOKEN environment variable')
  }

  return {
    platform: 'octolens',
    kind: 'octolens-token',
    data: { token },
  }
}

// One OAuth client for the whole Linux Foundation organization, not per-integration like GitHub.
function credlyOAuthClientCredential(): Credential {
  const clientId = process.env.CROWD_CREDLY_CLIENT_ID
  const clientSecret = process.env.CROWD_CREDLY_CLIENT_SECRET
  const organizationId = process.env.CROWD_CREDLY_ORGANIZATION_ID

  if (!clientId || !clientSecret || !organizationId) {
    throw new Error(
      'missing CROWD_CREDLY_CLIENT_ID, CROWD_CREDLY_CLIENT_SECRET or CROWD_CREDLY_ORGANIZATION_ID environment variables',
    )
  }

  return {
    platform: 'credly',
    kind: 'credly-oauth-client',
    data: { clientId, clientSecret, organizationId },
  }
}
