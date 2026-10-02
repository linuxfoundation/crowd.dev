import { fetchIntegrationsForSegment, findSubprojectsBySourceId } from '@crowd/data-access-layer'
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

    return matches.length === 1 ? matches[0] : { duplicates: matches }
  }
}
