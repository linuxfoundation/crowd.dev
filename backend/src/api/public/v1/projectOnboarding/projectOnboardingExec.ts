import { randomUUID } from 'crypto'

import type { Request, Response } from 'express'

import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'
import { onboardProject } from '@crowd/project-onboarding'

import { projectOnboardingRequestSchema } from './types'

export default async (req: Request, res: Response): Promise<void> => {
  const parsed = validateOrThrow(projectOnboardingRequestSchema, req.body)

  const result = await onboardProject({
    id: randomUUID(),
    repoUrl: parsed.repoUrl,
    repoName: parsed.repoName,
    projectSlug: parsed.projectSlug,
  })

  ok(res, result)
}
