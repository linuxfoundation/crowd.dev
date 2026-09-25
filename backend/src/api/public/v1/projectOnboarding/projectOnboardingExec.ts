import { randomUUID } from 'crypto'

import type { Request, Response } from 'express'

import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'
import { onboardProject } from '@crowd/project-onboarding'

import { createDailyProjectOnboardingCap } from './dailyRequestCap'
import { projectOnboardingRequestSchema } from './types'

const reserveDailyProjectOnboardingRequest = createDailyProjectOnboardingCap()

export default async (req: Request, res: Response): Promise<void> => {
  reserveDailyProjectOnboardingRequest(
    req.actor ? (req.actor.apiKeyId ?? req.actor.id) : 'anonymous',
    req.actor ? req.actor.id : 'anonymous',
  )

  const parsed = validateOrThrow(projectOnboardingRequestSchema, req.body)

  const result = await onboardProject({
    id: randomUUID(),
    repoUrl: parsed.repoUrl,
    repoName: parsed.repoName,
    projectSlug: parsed.projectSlug,
  })

  ok(res, result)
}
