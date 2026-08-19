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

  // POC scope: GitHub only; each migrated connector adds its platform case here
  switch (integration.platform) {
    case 'github':
    case 'github-nango':
      return githubAppCredential()
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
