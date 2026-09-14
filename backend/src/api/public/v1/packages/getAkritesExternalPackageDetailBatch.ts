import type { Request, Response } from 'express'

import {
  type AkritesExternalPackageDetailRow,
  getPackageDetailsByPurls,
} from '@crowd/data-access-layer'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import {
  type PackageDetailBulkEntry,
  toAkritesExternalPackageDetail,
} from './akritesExternalPackageDetail'
import { paginatePurls, paginatedPurlsBodySchema } from './purl'

const bodySchema = paginatedPurlsBodySchema()

export async function getAkritesExternalPackageDetailBatch(
  req: Request,
  res: Response,
): Promise<void> {
  const { page, pageSize, total, pagedPurls, normalizedPurls } = paginatePurls(
    validateOrThrow(bodySchema, req.body),
  )

  const qx = await getPackagesQx()
  const rows = await getPackageDetailsByPurls(qx, normalizedPurls)

  const byPurl = new Map<string, AkritesExternalPackageDetailRow>(rows.map((r) => [r.purl, r]))

  const results: PackageDetailBulkEntry[] = pagedPurls.map((requestedPurl, i) => {
    const row = byPurl.get(normalizedPurls[i])
    return {
      requestedPurl,
      found: row !== undefined,
      package: row ? toAkritesExternalPackageDetail(row) : null,
    }
  })

  ok(res, { page, pageSize, total, results })
}
