import { NextFunction, Request, Response } from 'express'

import { IRepositoryOptions } from '../database/repositories/IRepositoryOptions'
import SegmentRepository from '../database/repositories/segmentRepository'

/** Resolves active segment(s) from the request and sets `req.currentSegments` for downstream handlers. */
export async function segmentMiddleware(req: Request, _res: Response, next: NextFunction) {
  try {
    const options = req as unknown as IRepositoryOptions
    const segmentRepository = new SegmentRepository(options)

    // Query parameters take precedence; request body acts as the fallback.
    const rawSegments = req.query.segments ?? (req.body as Record<string, unknown>)?.segments

    // Express can parse segments as a single string or an array (?segments=a vs ?segments=a&segments=b).
    // Normalize into a clean, flat array of non-empty strings.
    const segmentIds = toStringArray(rawSegments)

    if (segmentIds.length > 0) {
      options.currentSegments = await segmentRepository.findInIds(segmentIds)
    } else {
      // No segmentIds in the request — use the first subproject as the default
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
