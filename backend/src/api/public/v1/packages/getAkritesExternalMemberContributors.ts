import type { Request, Response } from 'express'

import { getPackagesQx } from '@/db/packagesDb'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'
import { NotFoundError } from '@crowd/common'
import {
  findAffiliatedMemberIdentities,
  findMemberContributorsPackage,
  groupAffiliatedMemberIdentities,
  toAkritesExternalMemberContributors,
} from '@crowd/data-access-layer'

import { purlQuerySchema } from './purl'

export async function getAkritesExternalMemberContributors(
  req: Request,
  res: Response,
): Promise<void> {
  const { purl } = validateOrThrow(purlQuerySchema, req.query)

  const qx = await getPackagesQx()
  const pkg = await findMemberContributorsPackage(qx, purl)

  if (!pkg) {
    throw new NotFoundError()
  }

  const { repoId, repoUrl } = pkg
  if (repoId === null || repoUrl === null) {
    throw new NotFoundError('Purl has no linked repository')
  }

  const rows = await findAffiliatedMemberIdentities(qx, { packageId: pkg.packageId, repoId })

  ok(
    res,
    toAkritesExternalMemberContributors({ ...pkg, repoUrl }, groupAffiliatedMemberIdentities(rows)),
  )
}
