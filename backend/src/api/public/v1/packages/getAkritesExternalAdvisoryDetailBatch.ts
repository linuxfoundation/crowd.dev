import type { Request, Response } from 'express'

import { type AkritesExternalAdvisoryRow, getAdvisoriesByPurls } from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import {
  type AdvisoryDetailBulkEntry,
  toAkritesExternalAdvisoryDetail,
} from './akritesExternalAdvisoryDetail'
import { normalizePurl, purlsBodySchema } from './purl'

const bodySchema = purlsBodySchema()

export async function getAkritesExternalAdvisoryDetailBatch(
  req: Request,
  res: Response,
): Promise<void> {
  const { purls: rawPurls } = validateOrThrow(bodySchema, req.body)
  // Normalize after parsing (not in the schema) so rawPurls keeps the client's
  // original form — echoed back as requestedPurl so callers can self-correlate.
  const normalizedPurls = rawPurls.map(normalizePurl)

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

  const results: AdvisoryDetailBulkEntry[] = rawPurls.map((requestedPurl, i) => {
    const purlRows = byPurl.get(normalizedPurls[i])
    return {
      requestedPurl,
      found: purlRows !== undefined,
      advisories: purlRows ? toAkritesExternalAdvisoryDetail(purlRows[0].purl, purlRows) : null,
    }
  })

  ok(res, { results })
}
