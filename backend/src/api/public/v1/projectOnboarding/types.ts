import { z } from 'zod'

import type { IOnboardingResult } from '@crowd/project-onboarding'

export const projectOnboardingRequestSchema = z.object({
  repoUrl: z.string().url(),
  repoName: z.string().min(1),
  projectSlug: z.string().min(1),
})

export type IProjectOnboardingRequest = z.infer<typeof projectOnboardingRequestSchema>

export type { IOnboardingResult }
