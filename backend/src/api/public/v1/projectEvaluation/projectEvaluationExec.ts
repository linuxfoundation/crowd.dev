import type { Request, Response } from 'express'

import { optionsQx } from '@/database/sequelizeQueryExecutor'
import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'
import { ServiceUnavailableError } from '@crowd/common'

import { createDailyLlmCap } from './dailyLlmCap'
import { evaluateProject } from './evaluateProject'
import { precheckSkipResult, runPrecheck } from './runPrecheck'
import {
  IProjectEvaluationRequest,
  IProjectEvaluationResponse,
  projectEvaluationRequestSchema,
} from './types'

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

  const qx = optionsQx(req)

  if (parsed.precheck) {
    const skipReason = await runPrecheck(qx, input.repoUrl)
    if (skipReason) {
      ok(res, precheckSkipResult(skipReason))
      return
    }
  }

  let response: IProjectEvaluationResponse
  try {
    response = await evaluateProject(
      input,
      qx,
      {
        accessKeyId: process.env.CROWD_AWS_BEDROCK_ACCESS_KEY_ID,
        secretAccessKey: process.env.CROWD_AWS_BEDROCK_SECRET_ACCESS_KEY,
      },
      req.log,
      () => reserveDailyLlmCall(req.actor.apiKeyId ?? req.actor.id, req.actor.id),
    )
  } catch (err) {
    if (err instanceof ServiceUnavailableError) {
      respondTransientFailure(req, res, input, err)
      return
    }
    throw err
  }

  ok(res, response)
}

// Answered here instead of the error handler: the worker retries these, so the
// public-API 5xx alert would fire for failures that end well.
function respondTransientFailure(
  req: Request,
  res: Response,
  input: IProjectEvaluationRequest,
  err: ServiceUnavailableError,
): void {
  req.log.warn(
    { id: input.id, repoUrl: input.repoUrl, reason: err.message },
    'Project evaluation hit a transient error, the worker will retry.',
  )
  res.status(err.status).json(err.toJSON())
}
