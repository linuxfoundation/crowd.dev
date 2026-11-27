import type { Manifest } from '../../types'
import { createSessionizeDeps } from './deps'
import type { SessionizeDeps } from './deps'
import { discoverSessionize } from './discover'
import { sessionizeActivitySchema } from './mapper'
import {
  applySessionizeCookie,
  createSessionizeSessionMinter,
  interpretSessionizeResponse,
  prepareSessionizePool,
} from './session'
import { runEventExport } from './syncs/eventExport'

export const sessionizeConnector: Manifest<SessionizeDeps> = {
  platform: 'sessionize',
  syncs: [
    {
      name: 'eventExport',
      cadenceMinutes: 1440,
      schema: sessionizeActivitySchema,
      run: runEventExport,
    },
  ],
  discover: discoverSessionize,
  preparePool: prepareSessionizePool,
  mintToken: createSessionizeSessionMinter,
  applyToken: applySessionizeCookie,
  interpretResponse: interpretSessionizeResponse,
  createDeps: createSessionizeDeps,
  maxConcurrentRuns: 1,
}
