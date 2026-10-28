import {
  listActiveCredlyTemplateMappings,
  listStaleCredlyTemplateMappings,
  lockCredlyTemplateMapping,
} from '@crowd/data-access-layer/src/credly'

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
  loadTemplateRouting(): Promise<CredlyTemplateRouting>
  lockTemplate(badgeTemplateId: string, target: CredlyTemplateTarget): Promise<boolean>
}

export function createCredlyDeps({ qx }: DepsFactoryInput): CredlyDeps {
  let routing: Promise<CredlyTemplateRouting> | null = null

  return {
    loadTemplateRouting() {
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
  }
}
