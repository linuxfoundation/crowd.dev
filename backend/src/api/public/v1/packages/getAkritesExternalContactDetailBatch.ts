import type { Request, Response } from 'express'

import {
  type AkritesExternalContactDetailRow,
  getContactDetailsByPurls,
} from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import {
  type ContactDetailBulkEntry,
  toAkritesExternalContactDetail,
} from './akritesExternalContactDetail'
import { normalizePurl, purlsBodySchema } from './purl'

const bodySchema = purlsBodySchema()

export async function getAkritesExternalContactDetailBatch(
  req: Request,
  res: Response,
): Promise<void> {
  const { purls: rawPurls } = validateOrThrow(bodySchema, req.body)
  // Normalize after parsing (not in the schema) so rawPurls keeps the client's
  // original form — echoed back as requestedPurl so callers can self-correlate.
  const normalizedPurls = rawPurls.map(normalizePurl)

  const qx = await getPackagesQx()
  const rows = await getContactDetailsByPurls(qx, normalizedPurls)

  const byPurl = new Map<string, AkritesExternalContactDetailRow>(rows.map((r) => [r.purl, r]))

  const results: ContactDetailBulkEntry[] = rawPurls.map((requestedPurl, i) => {
    const row = byPurl.get(normalizedPurls[i])
    return {
      requestedPurl,
      found: row !== undefined,
      contact: row ? toAkritesExternalContactDetail(row) : null,
    }
  })

  ok(res, { results })
}
