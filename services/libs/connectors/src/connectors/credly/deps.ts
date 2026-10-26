import {
  listActiveCredlyTemplateMappings,
  listStaleCredlyTemplateMappings,
} from '@crowd/data-access-layer/src/credly'

import type { DepsFactoryInput } from '../../types'

export interface CredlyTemplateTarget {
  segmentId: string
  integrationId: string
}

export interface CredlyTemplateRouting {
  targets: Map<string, CredlyTemplateTarget>
  staleTemplateIds: string[]
}

export interface CredlyDeps {
  loadTemplateRouting(): Promise<CredlyTemplateRouting>
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
            { segmentId: m.segmentId, integrationId: m.integrationId },
          ]),
        ),
        staleTemplateIds: stale.map((m) => m.badgeTemplateId),
      }))
      return routing
    },
  }
}
