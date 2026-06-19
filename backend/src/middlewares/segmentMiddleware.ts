import { NextFunction, Request, Response } from 'express'

import { IRepositoryOptions } from '../database/repositories/IRepositoryOptions'
import SegmentRepository from '../database/repositories/segmentRepository'

/** Resolves segment(s) from the request and sets `req.currentSegments` for downstream handlers. */
export async function segmentMiddleware(req: Request, _res: Response, next: NextFunction) {
  try {
    const options = req as unknown as IRepositoryOptions
    const segmentRepository = new SegmentRepository(options)

    const querySegments = toStringArray(req.query.segments)
    const bodySegments = toStringArray((req.body as Record<string, unknown>)?.segments)

    const segmentIds = querySegments.length > 0 ? querySegments : bodySegments

    if (segmentIds.length > 0) {
      options.currentSegments = await segmentRepository.findInIds(segmentIds)
    } else {
      const { rows } = await segmentRepository.querySubprojects({ limit: 1, offset: 0 })
      options.currentSegments = rows
    }

    next()
  } catch (error) {
    next(error)
  }
}

/**
 * Safely extracts a string[] from an unknown query/body value.
 */
function toStringArray(value: unknown): string[] {
  if (value === undefined || value === null) return []

  const items = Array.isArray(value) ? value : [value]

  return items
    .filter((item): item is string => typeof item === 'string')
    .map((item) => item.trim())
    .filter(Boolean)
}
