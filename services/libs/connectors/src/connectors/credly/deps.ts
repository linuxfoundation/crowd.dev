import type { ICredlyBadgeInput, ICredlyBadgeRow } from '@crowd/data-access-layer/src/credly/badges'
import {
  findCredlyBadges,
  listDueCredlyExpiries,
  markCredlyExpiriesEmitted,
  upsertCredlyBadges,
} from '@crowd/data-access-layer/src/credly/badges'
import type { ICredlyExpiryCursor } from '@crowd/data-access-layer/src/credly/badges'

import type { DepsFactoryInput } from '../../types'

export interface CredlyDeps {
  segmentId: string
  findBadges(badgeIds: string[]): Promise<ICredlyBadgeRow[]>
  saveBadges(badges: ICredlyBadgeInput[]): Promise<void>
  listDueExpiries(now: Date, limit: number, after?: ICredlyExpiryCursor): Promise<ICredlyBadgeRow[]>
  markExpiriesEmitted(badgeIds: string[]): Promise<number>
}

export function createCredlyDeps({ qx, integrationId, segmentId }: DepsFactoryInput): CredlyDeps {
  if (!segmentId) {
    throw new Error(`credly integration ${integrationId} has no segment`)
  }
  return {
    segmentId,
    findBadges(badgeIds) {
      return findCredlyBadges(qx, badgeIds)
    },
    saveBadges(badges) {
      return upsertCredlyBadges(qx, badges)
    },
    listDueExpiries(now, limit, after) {
      return listDueCredlyExpiries(qx, now, limit, after)
    },
    markExpiriesEmitted(badgeIds) {
      return markCredlyExpiriesEmitted(qx, badgeIds)
    },
  }
}
