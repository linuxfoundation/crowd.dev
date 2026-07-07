import { Client, Connection, getDataConverter } from '@crowd/temporal'
import { IS_DEV_ENV, SERVICE } from '@crowd/common'

import { PACKAGES_TEMPORAL_CONFIG } from '@/conf'

let _init: Promise<Client> | undefined

// Separate connection from the API's default req.temporal client — packages_worker
// (npm/maven/pypi/osv/security-contacts/...) polls task queues in its own Temporal
// namespace (CROWD_PACKAGES_TEMPORAL_NAMESPACE), not the API's default namespace.
export function getPackagesTemporalClient(): Promise<Client> {
  if (!_init) {
    if (!PACKAGES_TEMPORAL_CONFIG?.serverUrl) {
      throw new Error(
        'Packages Temporal is not configured — set CROWD_PACKAGES_TEMPORAL_NAMESPACE',
      )
    }

    const cfg = PACKAGES_TEMPORAL_CONFIG
    _init = Connection.connect({
      address: cfg.serverUrl,
      tls:
        cfg.certificate && cfg.privateKey
          ? {
              clientCertPair: {
                crt: Buffer.from(cfg.certificate, 'base64'),
                key: Buffer.from(cfg.privateKey, 'base64'),
              },
            }
          : undefined,
    })
      .then(
        async (connection) =>
          new Client({
            connection,
            namespace: cfg.namespace,
            identity: SERVICE,
            dataConverter: IS_DEV_ENV ? undefined : await getDataConverter(),
          }),
      )
      .catch((err) => {
        _init = undefined
        throw err
      })
  }
  return _init
}
