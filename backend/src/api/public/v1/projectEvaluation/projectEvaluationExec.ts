import type { Request, Response } from 'express'

import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import { IProjectEvaluationResponse, projectEvaluationRequestSchema } from './types'

export default async (req: Request, res: Response): Promise<void> => {
  validateOrThrow(projectEvaluationRequestSchema, req.body)

  const response: IProjectEvaluationResponse = {
    outcome: 'unsure',
    evaluationResult: 'not_implemented',
    evaluationReason: 'Evaluation decision logic is not implemented yet',
    metrics: null,
  }

  ok(res, response)
}
