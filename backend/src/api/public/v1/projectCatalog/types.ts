import { z } from 'zod'

export const PROJECT_CATALOG_MANUAL_ACTIONS = ['auto', 'evaluate', 'onboard'] as const

export const projectCatalogUpsertRequestSchema = z.object({
  repoUrl: z.string().url(),
  action: z.enum(PROJECT_CATALOG_MANUAL_ACTIONS),
})

export type IProjectCatalogUpsertRequest = z.infer<typeof projectCatalogUpsertRequestSchema>
