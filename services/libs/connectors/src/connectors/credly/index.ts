import type { Manifest } from '../../types'
import { createCredlyDeps } from './deps'
import type { CredlyDeps } from './deps'
import { discoverCredly } from './discover'
import { credlyActivitySchema } from './mapper'
import { createCredlyTokenMinter, prepareCredlyPool } from './oauth'
import { runBadgePoll } from './syncs/badgePoll'
import { runExpirySync } from './syncs/expirySync'

export const credlyConnector: Manifest<CredlyDeps> = {
  platform: 'credly',
  syncs: [
    {
      name: 'badgePoll',
      cadenceMinutes: 720,
      schema: credlyActivitySchema,
      run: runBadgePoll,
    },
    {
      name: 'expirySync',
      cadenceMinutes: 1440,
      schema: credlyActivitySchema,
      run: runExpirySync,
    },
  ],
  discover: discoverCredly,
  preparePool: prepareCredlyPool,
  mintToken: createCredlyTokenMinter,
  createDeps: createCredlyDeps,
  emitsOwnSegment: true,
}
