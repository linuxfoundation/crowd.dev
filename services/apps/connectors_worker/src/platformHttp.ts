import type { ConnectorHttp } from '@crowd/connectors'
import { createHttpClient, createTokenPool, getCredential, getManifest } from '@crowd/connectors'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import type { Logger } from '@crowd/logging'

import { svc } from './main'

export async function createPlatformHttp(
  qx: QueryExecutor,
  platform: string,
  integrationId: string,
  log: Logger,
): Promise<ConnectorHttp> {
  const manifest = getManifest(platform)
  const credential = manifest.mintToken ? await getCredential(qx, integrationId) : null
  const pool = createTokenPool(svc.redis, platform, {
    probeBudget: manifest.probeBudget,
    mintToken: credential && manifest.mintToken ? manifest.mintToken(credential) : undefined,
    log,
  })
  const preferredEntryId =
    credential && manifest.preparePool
      ? (await manifest.preparePool(credential, pool)).preferredEntryId
      : undefined

  return createHttpClient({
    acquireToken: () => pool.acquire(preferredEntryId),
    parkToken: pool.park,
    invalidateToken: pool.invalidate,
    interpretResponse: manifest.interpretResponse,
    log,
  })
}
