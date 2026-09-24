import type { Request, Response } from 'express'

import { optionsQx } from '@/database/sequelizeQueryExecutor'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

import { createDailyLlmCap } from './dailyLlmCap'
import { evaluateProject } from './evaluateProject'
import { IProjectEvaluationRequest, projectEvaluationRequestSchema } from './types'

const reserveDailyLlmCall = createDailyLlmCap()

export default async (req: Request, res: Response): Promise<void> => {
  const parsed = validateOrThrow(projectEvaluationRequestSchema, req.body)
  const input: IProjectEvaluationRequest = {
    id: parsed.id,
    repoUrl: parsed.repoUrl,
    repoName: parsed.repoName,
    projectSlug: parsed.projectSlug,
    lfCriticalityScore: parsed.lfCriticalityScore ?? null,
    source: parsed.source ?? null,
  }

  const response = await evaluateProject(
    input,
    optionsQx(req),
    {
      accessKeyId: process.env.CROWD_AWS_BEDROCK_ACCESS_KEY_ID,
      secretAccessKey: process.env.CROWD_AWS_BEDROCK_SECRET_ACCESS_KEY,
    },
    req.log,
    () => reserveDailyLlmCall(req.actor.apiKeyId ?? req.actor.id, req.actor.id),
  )

  ok(res, response)
}
