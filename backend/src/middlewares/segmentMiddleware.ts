import { NextFunction, Request, Response } from 'express'

import {
  isSegmentSubproject,
  populateSegmentRelations,
} from '@crowd/data-access-layer/src/segments'
import { getServiceChildLogger } from '@crowd/logging'

import { IRepositoryOptions } from '../database/repositories/IRepositoryOptions'
import SegmentRepository from '../database/repositories/segmentRepository'

const log = getServiceChildLogger('segmentMiddleware')

export async function segmentMiddleware(req: Request, _res: Response, next: NextFunction) {
  try {
    let segments: any = null
    const segmentRepository = new SegmentRepository(req as unknown as IRepositoryOptions)

    // Note: req.params is NOT available here. This middleware is registered via app.use(),
    // which runs before Express matches a specific route and populates req.params.
    // Any check on req.params (e.g. req.params.segmentId) would always be undefined.
    // Route handlers that need a specific segment by ID (e.g. GET /segment/:segmentId)
    // read req.params directly and ignore req.currentSegments entirely — so the
    // resolution below is harmless for those endpoints.
    const querySegments = toStringArray(req.query.segments)
    const bodySegments = toStringArray((req.body as Record<string, unknown>)?.segments)

    if (querySegments.length > 0) {
      segments = {
        rows: await resolveToLeafSegments(segmentRepository, querySegments, req),
      }
    } else if (bodySegments.length > 0) {
      segments = {
        rows: await resolveToLeafSegments(segmentRepository, bodySegments, req),
      }
    } else {
      segments = await segmentRepository.querySubprojects({ limit: 1, offset: 0 })
    }

    const options = req as unknown as IRepositoryOptions
    options.currentSegments = segments.rows

    next()
  } catch (error) {
    next(error)
  }
}

/**
 * Safely extracts a string[] from an unknown query/body value.
 * Rejects ParsedQs objects (e.g. ?segments[key]=val) that would cause type confusion.
 */
function toStringArray(value: unknown): string[] {
  if (value === undefined || value === null) return []
  const items = Array.isArray(value) ? value : [value]
  return items.filter((v): v is string => typeof v === 'string')
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
  req: Request,
) {
  const fetched = await segmentRepository.findInIds(segmentIds)

  const nonLeaf = fetched.filter((s) => !isSegmentSubproject(s))

  const segmentLevel = (s: any) => {
    if (s.grandparentSlug) return 'subproject'
    if (s.parentSlug) return 'project'
    return 'projectGroup'
  }

  if (nonLeaf.length === 0) {
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

  log.debug(
    {
      api: `${req.method} ${req.path}`,
      input_segments: nonLeaf.map((s) => ({ id: s.id, name: s.name, level: segmentLevel(s) })),
      resolved_leaf_segments: leafRecords.map((s: any) => ({ id: s.id, name: (s as any).name })),
      resolved_count: leafRecords.length,
    },
    'Non-leaf segments resolved to leaf sub-projects',
  )

  return leafRecords.map(populateSegmentRelations)
}
