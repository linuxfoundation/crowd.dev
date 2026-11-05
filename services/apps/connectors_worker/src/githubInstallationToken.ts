import { type Credential, getCredential } from '@crowd/connectors'
import {
  mintInstallationToken,
  resolveInstallationId,
} from '@crowd/connectors/src/connectors/github/appToken'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

const TOKEN_REFRESH_MARGIN_MS = 5 * 60_000

export type TokenSource = () => Promise<string>

async function resolveInstallation(
  qx: QueryExecutor,
  integrationId: string,
): Promise<{ credential: Credential; installationId: string }> {
  const credential = await getCredential(qx, integrationId)
  return { credential, installationId: await resolveInstallationId(credential) }
}

export function installationTokenSource(qx: QueryExecutor, integrationId: string): TokenSource {
  let installation: ReturnType<typeof resolveInstallation> | null = null
  let current: { token: string; expiresAt: string } | null = null
  return async () => {
    if (current && Date.parse(current.expiresAt) - Date.now() >= TOKEN_REFRESH_MARGIN_MS) {
      return current.token
    }
    installation ??= resolveInstallation(qx, integrationId).catch((err: unknown) => {
      installation = null
      throw err
    })
    const { credential, installationId } = await installation
    current = await mintInstallationToken(credential, installationId)
    return current.token
  }
}
