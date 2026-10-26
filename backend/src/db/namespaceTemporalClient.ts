import { IS_DEV_ENV, SERVICE } from '@crowd/common'
import { Client, Connection, ITemporalConfig, getDataConverter } from '@crowd/temporal'

export function connectNamespaceTemporalClient(cfg: ITemporalConfig): Promise<Client> {
  return Connection.connect({
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
  }).then(
    async (connection) =>
      new Client({
        connection,
        namespace: cfg.namespace,
        identity: SERVICE,
        dataConverter: IS_DEV_ENV ? undefined : await getDataConverter(),
      }),
  )
}
