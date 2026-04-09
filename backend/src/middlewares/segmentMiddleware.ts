import { isSegmentSubproject, populateSegmentRelations } from '@crowd/data-access-layer/src/segments'
import { getServiceChildLogger } from '@crowd/logging'

import SegmentRepository from '../database/repositories/segmentRepository'

const log = getServiceChildLogger('segmentMiddleware')

export async function segmentMiddleware(req, res, next) {
  try {
    let segments: any = null
    const segmentRepository = new SegmentRepository(req)

    // Note: req.params is NOT available here. This middleware is registered via app.use(),
    // which runs before Express matches a specific route and populates req.params.
    // Any check on req.params (e.g. req.params.segmentId) would always be undefined.
    // Route handlers that need a specific segment by ID (e.g. GET /segment/:segmentId)
    // read req.params directly and ignore req.currentSegments entirely — so the
    // resolution below is harmless for those endpoints.
    if (req.query.segments) {
      // GET requests
      segments = {
        rows: await resolveToLeafSegments(segmentRepository, req.query.segments, req),
      }
    } else if (req.body.segments) {
      // POST/PUT requests
      segments = {
        rows: await resolveToLeafSegments(segmentRepository, req.body.segments, req),
      }
    } else {
      segments = await segmentRepository.querySubprojects({ limit: 1, offset: 0 })
    }

    req.currentSegments = segments.rows

    next()
  } catch (error) {
    next(error)
  }
}

/**
 * Resolves segment IDs to their leaf sub-projects.
 *
 * If all provided IDs are already sub-projects (leaf level), returns them as-is
 * without any extra DB call — fully backward-compatible with the current behavior.
 *
 * If any ID is a project or project group (non-leaf), expands it to all its
 * active sub-projects and applies populateSegmentRelations to match the shape
 * that downstream services expect from req.currentSegments.
 */
async function resolveToLeafSegments(
  segmentRepository: SegmentRepository,
  segmentIds: string[],
  req: any,
) {
  const fetched = await segmentRepository.findInIds(segmentIds)

  const nonLeaf = fetched.filter((s) => !isSegmentSubproject(s))

  const segmentLevel = (s: any) =>
    s.grandparentSlug ? 'subproject' : s.parentSlug ? 'project' : 'projectGroup'

  if (nonLeaf.length === 0) {
    // All IDs are already leaf segments — current behavior, no change.
    log.debug(
      {
        api: `${req.method} ${req.path}`,
        usedInDbQueries: fetched.map((s) => ({ id: s.id, name: s.name, level: segmentLevel(s) })),
      },
      `All segments are already leaf — used as-is in DB queries`,
    )
    return fetched
  }

  const leafRecords = await segmentRepository.getSegmentSubprojects(segmentIds)

  log.warn(
    {
      api: `${req.method} ${req.path}`,
      '⚠️  WITHOUT_RESOLUTION_would_have_used': {
        segments: nonLeaf.map((s) => ({ id: s.id, name: s.name, level: segmentLevel(s) })),
        count: segmentIds.length,
      },
      '✅ WITH_RESOLUTION_will_use': {
        segments: leafRecords.map((s: any) => ({ id: s.id, name: (s as any).name })),
        count: leafRecords.length,
      },
    },
    `⚠️  NON-LEAF SEGMENT DETECTED — DB queries will use resolved leaf segments instead of the received ones`,
  )

  return leafRecords.map(populateSegmentRelations)
}
