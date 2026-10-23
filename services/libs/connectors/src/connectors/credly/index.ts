import type { Manifest } from '../../types'
import { discoverCredly } from './discover'
import { credlyActivitySchema } from './mapper'
import { createCredlyTokenMinter, prepareCredlyPool } from './oauth'
import { runBadgePoll } from './syncs/badgePoll'

export const credlyConnector: Manifest = {
  platform: 'credly',
  syncs: [
    {
      name: 'badgePoll',
      cadenceMinutes: 720,
      schema: credlyActivitySchema,
      run: runBadgePoll,
    },
  ],
  discover: discoverCredly,
  preparePool: prepareCredlyPool,
  mintToken: createCredlyTokenMinter,
}
