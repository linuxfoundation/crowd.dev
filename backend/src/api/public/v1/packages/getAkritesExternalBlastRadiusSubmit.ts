import type { Request, Response } from 'express'

import { NotImplementedError } from '@crowd/common'

import { validateOrThrow } from '@/utils/validation'

import { blastRadiusJobRequestSchema } from './blastRadius'

// The reachability pipeline isn't built yet — see the Blast Radius section of
// the akrites-external draft contract. Still validate the request shape so
// callers get a real 400 for malformed input rather than a blanket 501.
export async function submitAkritesExternalBlastRadiusJob(
  req: Request,
  _res: Response,
): Promise<void> {
  validateOrThrow(blastRadiusJobRequestSchema, req.body)

  throw new NotImplementedError('Blast-radius analysis is not implemented yet')
}
