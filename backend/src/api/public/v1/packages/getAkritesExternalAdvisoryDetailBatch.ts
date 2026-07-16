import type { Request, Response } from 'express'

import { type AkritesExternalAdvisoryRow, getAdvisoriesByPurls } from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import {
  type AdvisoryDetailBulkEntry,
  toAkritesExternalAdvisoryDetail,
} from './akritesExternalAdvisoryDetail'
import { paginatePurls, paginatedPurlsBodySchema } from './purl'

const bodySchema = paginatedPurlsBodySchema()

export async function getAkritesExternalAdvisoryDetailBatch(
  req: Request,
  res: Response,
): Promise<void> {
  const { page, pageSize, total, pagedPurls, normalizedPurls } = paginatePurls(
    validateOrThrow(bodySchema, req.body),
  )

  const qx = await getPackagesQx()
  const rows = await getAdvisoriesByPurls(qx, normalizedPurls)

  // Group the flat rows by purl. A found package always has >= 1 row (a null-osvId
  // sentinel when it has no advisories), so map presence == package found.
  const byPurl = new Map<string, AkritesExternalAdvisoryRow[]>()
  for (const row of rows) {
    const existing = byPurl.get(row.purl)
    if (existing) existing.push(row)
    else byPurl.set(row.purl, [row])
  }

  const results: AdvisoryDetailBulkEntry[] = pagedPurls.map((requestedPurl, i) => {
    const purlRows = byPurl.get(normalizedPurls[i])
    return {
      requestedPurl,
      found: purlRows !== undefined,
      advisories: purlRows ? toAkritesExternalAdvisoryDetail(purlRows[0].purl, purlRows) : null,
    }
  })

  ok(res, { page, pageSize, total, results })
}
