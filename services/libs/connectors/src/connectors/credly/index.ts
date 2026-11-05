import type { Manifest } from '../../types'
import { CREDLY_SAFE_CALLS_PER_WINDOW, probeCredlyBudget } from './budget'
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
  probeBudget: probeCredlyBudget,
  runCostEstimate: CREDLY_SAFE_CALLS_PER_WINDOW,
  createDeps: createCredlyDeps,
  emitsOwnSegment: true,
}
