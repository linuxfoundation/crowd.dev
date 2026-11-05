import {
  listActiveCredlyTemplateMappings,
  listStaleCredlyTemplateMappings,
  lockCredlyTemplateMapping,
} from '@crowd/data-access-layer/src/credly'
import type {
  ICredlyBadgeInput,
  ICredlyBadgeRow,
  ICredlyExpiryCursor,
} from '@crowd/data-access-layer/src/credly/badges'
import {
  findCredlyBadges,
  listDueCredlyExpiries,
  markCredlyExpiriesEmitted,
  upsertCredlyBadges,
} from '@crowd/data-access-layer/src/credly/badges'
import type {
  ICredlyTemplateRelease,
  ICredlyTemplateReleaseCursor,
} from '@crowd/data-access-layer/src/credly/releases'
import {
  completeCredlyTemplateRelease,
  nextPendingCredlyTemplateRelease,
  saveCredlyTemplateReleaseCursor,
} from '@crowd/data-access-layer/src/credly/releases'

import type { DepsFactoryInput } from '../../types'

export interface CredlyTemplateTarget {
  segmentId: string
  integrationId: string
  locked: boolean
}

export interface CredlyTemplateRouting {
  targets: Map<string, CredlyTemplateTarget>
  staleTemplateIds: string[]
}

export interface CredlyDeps {
  loadTemplateRouting(fresh?: boolean): Promise<CredlyTemplateRouting>
  lockTemplate(badgeTemplateId: string, target: CredlyTemplateTarget): Promise<boolean>
  findBadges(badgeIds: string[]): Promise<ICredlyBadgeRow[]>
  saveBadges(badges: ICredlyBadgeInput[]): Promise<void>
  listDueExpiries(now: Date, limit: number, after?: ICredlyExpiryCursor): Promise<ICredlyBadgeRow[]>
  markExpiriesEmitted(badgeIds: string[]): Promise<number>
  nextRelease(): Promise<ICredlyTemplateRelease | null>
  saveReleaseCursor(
    release: ICredlyTemplateRelease,
    cursor: ICredlyTemplateReleaseCursor,
  ): Promise<number>
  completeRelease(release: ICredlyTemplateRelease): Promise<number>
}

export function createCredlyDeps({ qx }: DepsFactoryInput): CredlyDeps {
  let routing: Promise<CredlyTemplateRouting> | null = null

  return {
    loadTemplateRouting(fresh = false) {
      if (fresh) routing = null
      routing ??= Promise.all([
        listActiveCredlyTemplateMappings(qx),
        listStaleCredlyTemplateMappings(qx),
      ]).then(([active, stale]) => ({
        targets: new Map(
          active.map((m) => [
            m.badgeTemplateId,
            { segmentId: m.segmentId, integrationId: m.integrationId, locked: m.lockedAt !== null },
          ]),
        ),
        staleTemplateIds: stale.map((m) => m.badgeTemplateId),
      }))
      return routing
    },
    lockTemplate(badgeTemplateId, target) {
      return lockCredlyTemplateMapping(qx, badgeTemplateId, target)
    },
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
    nextRelease() {
      return nextPendingCredlyTemplateRelease(qx)
    },
    saveReleaseCursor(release, cursor) {
      return saveCredlyTemplateReleaseCursor(qx, release, cursor)
    },
    completeRelease(release) {
      return completeCredlyTemplateRelease(qx, release)
    },
  }
}
