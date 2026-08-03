import type { Request, Response } from 'express'

import {
  type ReportingProtocolRow,
  getReportingProtocolsByPurls,
} from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import {
  type ProjectProfilingBulkEntry,
  toAkritesExternalProjectProfiling,
} from './akritesExternalProjectProfiling'
import { paginatePurls, paginatedPurlsBodySchema } from './purl'

const bodySchema = paginatedPurlsBodySchema()

export async function getAkritesExternalProjectProfilingBatch(
  req: Request,
  res: Response,
): Promise<void> {
  const { page, pageSize, total, pagedPurls, normalizedPurls } = paginatePurls(
    validateOrThrow(bodySchema, req.body),
  )

  const qx = await getPackagesQx()
  const rows = await getReportingProtocolsByPurls(qx, normalizedPurls)

  const byPurl = new Map<string, ReportingProtocolRow>(rows.map((r) => [r.purl, r]))

  const results: ProjectProfilingBulkEntry[] = pagedPurls.map((requestedPurl, i) => {
    const row = byPurl.get(normalizedPurls[i])
    return {
      requestedPurl,
      found: row !== undefined,
      profiling: row ? toAkritesExternalProjectProfiling(row) : null,
    }
  })

  ok(res, { page, pageSize, total, results })
}
