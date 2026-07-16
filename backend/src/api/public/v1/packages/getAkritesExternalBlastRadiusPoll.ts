import type { Request, Response } from 'express'

import { NotImplementedError } from '@crowd/common'

import { validateOrThrow } from '@/utils/validation'

import { analysisIdParamSchema } from './blastRadius'

// No jobs are ever created (see submitAkritesExternalBlastRadiusJob), so there is
// nothing to poll yet — always 501, regardless of analysisId.
export async function getAkritesExternalBlastRadiusJob(
  req: Request,
  _res: Response,
): Promise<void> {
  validateOrThrow(analysisIdParamSchema, req.params)

  throw new NotImplementedError('Blast-radius analysis is not implemented yet')
}
