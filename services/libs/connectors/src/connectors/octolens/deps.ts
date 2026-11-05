import {
  InsightsProjectField,
  queryInsightsProjects,
} from '@crowd/data-access-layer/src/collections'
import { listActiveOctolensKeywordMappings } from '@crowd/data-access-layer/src/octolens'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { findSegmentById, isSegmentSubproject } from '@crowd/data-access-layer/src/segments'

import type { DepsFactoryInput } from '../../types'

export interface OctolensKeywordTarget {
  segmentId: string
  integrationId: string
}

export interface OctolensDeps {
  // Where a keyword's mentions land: its subproject's Octolens integration, never the poll's own.
  resolveKeywordTarget(keywordId: number): Promise<OctolensKeywordTarget | null>
}

// Activities only land on subproject segments, so a slug resolving to anything else is unusable.
export async function resolveOctolensProjectSegmentId(
  qx: QueryExecutor,
  slug: string,
): Promise<string | null> {
  const [project] = await queryInsightsProjects(qx, {
    fields: [InsightsProjectField.SEGMENT_ID],
    filter: { slug: { eq: slug } },
    limit: 1,
  })
  if (!project?.segmentId) return null

  const segment = await findSegmentById(qx, project.segmentId)
  return segment && isSegmentSubproject(segment) ? segment.id : null
}

export function createOctolensDeps({ qx }: DepsFactoryInput): OctolensDeps {
  let keywordTargets: Map<number, OctolensKeywordTarget | null> | null = null

  return {
    async resolveKeywordTarget(keywordId) {
      if (!keywordTargets) {
        const targets = new Map<number, OctolensKeywordTarget | null>()
        for (const m of await listActiveOctolensKeywordMappings(qx)) {
          // Discovery never maps one keyword to two subprojects; if it ever did, routing to either
          // would misroute the other's mentions, so the keyword counts as unmapped.
          targets.set(
            m.keywordId,
            targets.has(m.keywordId)
              ? null
              : { segmentId: m.segmentId, integrationId: m.integrationId },
          )
        }
        keywordTargets = targets
      }
      return keywordTargets.get(keywordId) ?? null
    },
  }
}
