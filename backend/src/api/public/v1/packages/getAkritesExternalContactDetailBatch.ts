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
import { paginatePurls, paginatedPurlsBodySchema } from './purl'

const bodySchema = paginatedPurlsBodySchema()

export async function getAkritesExternalContactDetailBatch(
  req: Request,
  res: Response,
): Promise<void> {
  const { page, pageSize, total, pagedPurls, normalizedPurls } = paginatePurls(
    validateOrThrow(bodySchema, req.body),
  )

  const qx = await getPackagesQx()
  const rows = await getContactDetailsByPurls(qx, normalizedPurls)

  const byPurl = new Map<string, AkritesExternalContactDetailRow>(rows.map((r) => [r.purl, r]))

  const results: ContactDetailBulkEntry[] = pagedPurls.map((requestedPurl, i) => {
    const row = byPurl.get(normalizedPurls[i])
    return {
      requestedPurl,
      found: row !== undefined,
      contact: row ? toAkritesExternalContactDetail(row) : null,
    }
  })

  ok(res, { page, pageSize, total, results })
}
