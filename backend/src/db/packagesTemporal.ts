import { PACKAGES_TEMPORAL_CONFIG } from '@/conf'
import { Client } from '@crowd/temporal'

import { connectNamespaceTemporalClient } from './namespaceTemporalClient'

let _init: Promise<Client> | undefined

// Separate connection from the API's default req.temporal client — packages_worker
// (npm/maven/pypi/osv/security-contacts/...) polls task queues in its own Temporal
// namespace (CROWD_PACKAGES_TEMPORAL_NAMESPACE), not the API's default namespace.
export function getPackagesTemporalClient(): Promise<Client> {
  if (!_init) {
    if (!PACKAGES_TEMPORAL_CONFIG?.serverUrl) {
      throw new Error('Packages Temporal is not configured — set CROWD_PACKAGES_TEMPORAL_NAMESPACE')
    }

    _init = connectNamespaceTemporalClient(PACKAGES_TEMPORAL_CONFIG).catch((err) => {
      _init = undefined
      throw err
    })
  }
  return _init
}
