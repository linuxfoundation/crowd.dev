import { z } from 'zod'

import {
  IProjectEvaluationRequest,
  IProjectEvaluationResponse,
} from '@crowd/data-access-layer/src/project-catalog/types'

export const projectEvaluationRequestSchema = z.object({
  id: z.string().min(1),
  repoUrl: z.string().url(),
  repoName: z.string().min(1),
  projectSlug: z.string().min(1),
  lfCriticalityScore: z.number().nullable(),
  source: z.string().nullable(),
})

export type { IProjectEvaluationRequest, IProjectEvaluationResponse }
