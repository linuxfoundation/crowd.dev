export type ProjectCatalogAction = 'auto' | 'evaluate' | 'onboard' | 'unsure'

export interface IDbProjectCatalog {
  id: string
  projectSlug: string
  repoName: string
  repoUrl: string
  source: string | null
  action: ProjectCatalogAction
  lfCriticalityScore: number | null
  evaluationResult: string | null
  evaluationReason: string | null
  evaluatedAt: string | null
  onboardedAt: string | null
  syncedAt: string | null
  createdAt: string | null
  updatedAt: string | null
}

type ProjectCatalogWritable = Pick<
  IDbProjectCatalog,
  | 'projectSlug'
  | 'repoName'
  | 'repoUrl'
  | 'source'
  | 'action'
  | 'lfCriticalityScore'
  | 'evaluationResult'
  | 'evaluationReason'
>

export type IDbProjectCatalogCreate = Omit<
  ProjectCatalogWritable,
  'source' | 'action' | 'lfCriticalityScore'
> & {
  source?: string | null
  action?: ProjectCatalogAction
  lfCriticalityScore?: number
}

export type IDbProjectCatalogUpdate = Partial<ProjectCatalogWritable> & {
  syncedAt?: string | null
  evaluatedAt?: string | null
  onboardedAt?: string | null
}
