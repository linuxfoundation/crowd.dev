import {
  countRepositoriesBySegment,
  fetchIntegrationsForSegment,
  findSubprojectsBySourceId,
} from '@crowd/data-access-layer'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { CdpSegmentLookupResult, ICdpSegmentMatch, toCdpIntegrationState } from './requestResolver'

async function toSegmentMatch(
  qx: QueryExecutor,
  segment: { id: string; name: string | null; slug: string },
): Promise<ICdpSegmentMatch> {
  const integrations = await fetchIntegrationsForSegment(qx, segment.id)

  return {
    segmentId: segment.id,
    name: segment.name ?? segment.slug,
    slug: segment.slug,
    integration: toCdpIntegrationState(integrations.map((integration) => integration.platform)),
  }
}

export function createCdpSegmentLookup(
  qx: QueryExecutor,
): (pccProjectId: string) => Promise<CdpSegmentLookupResult> {
  return async (pccProjectId) => {
    const segments = await findSubprojectsBySourceId(qx, pccProjectId)

    if (segments.length === 0) {
      return null
    }

    const matches = await Promise.all(segments.map((segment) => toSegmentMatch(qx, segment)))
    if (matches.length === 1) {
      return matches[0]
    }

    const counts = await countRepositoriesBySegment(
      qx,
      matches.map((match) => match.segmentId),
    )
    return {
      duplicates: matches.map((match) => ({
        ...match,
        repositoryCount: counts.get(match.segmentId) ?? 0,
      })),
    }
  }
}
