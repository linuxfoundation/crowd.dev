import type { Manifest } from '../../types'
import { discoverSessionize } from './discover'
import {
  applySessionizeCookie,
  createSessionizeSessionMinter,
  interpretSessionizeResponse,
  prepareSessionizePool,
} from './session'

export const sessionizeConnector: Manifest = {
  platform: 'sessionize',
  syncs: [],
  discover: discoverSessionize,
  preparePool: prepareSessionizePool,
  mintToken: createSessionizeSessionMinter,
  applyToken: applySessionizeCookie,
  interpretResponse: interpretSessionizeResponse,
}
