import { CONNECTORS_TEMPORAL_CONFIG } from '@/conf'
import { Client } from '@crowd/temporal'

import { connectNamespaceTemporalClient } from './namespaceTemporalClient'

let _init: Promise<Client> | undefined

export function getConnectorsTemporalClient(): Promise<Client> {
  if (!_init) {
    if (!CONNECTORS_TEMPORAL_CONFIG?.serverUrl) {
      throw new Error(
        'Connectors Temporal is not configured — set CROWD_CONNECTORS_TEMPORAL_SERVER_URL and CROWD_CONNECTORS_TEMPORAL_NAMESPACE',
      )
    }

    _init = connectNamespaceTemporalClient(CONNECTORS_TEMPORAL_CONFIG).catch((err) => {
      _init = undefined
      throw err
    })
  }
  return _init
}
