import { getSessionizeEvent } from '@crowd/data-access-layer/src/sessionize'

import type { DepsFactoryInput } from '../../types'
import type { SessionizeEventContext } from './mapper'

export interface SessionizeDeps {
  getEvent(eventKey: string): Promise<SessionizeEventContext | null>
}

export function createSessionizeDeps({ qx }: DepsFactoryInput): SessionizeDeps {
  return {
    getEvent(eventKey) {
      return getSessionizeEvent(qx, eventKey)
    },
  }
}
