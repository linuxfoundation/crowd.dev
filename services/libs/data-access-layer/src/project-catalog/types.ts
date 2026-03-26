export interface IDbProjectCatalog {
  id: string
  projectSlug: string
  repoName: string
  repoUrl: string
  ossfCriticalityScore: number | null
  lfCriticalityScore: number | null
  syncedAt: string | null
  createdAt: string | null
  updatedAt: string | null
}

type ProjectCatalogWritable = Pick<
  IDbProjectCatalog,
  'projectSlug' | 'repoName' | 'repoUrl' | 'ossfCriticalityScore' | 'lfCriticalityScore'
>

export type IDbProjectCatalogCreate = Omit<
  ProjectCatalogWritable,
  'ossfCriticalityScore' | 'lfCriticalityScore'
> & {
  ossfCriticalityScore?: number
  lfCriticalityScore?: number
}

export type IDbProjectCatalogUpdate = Partial<ProjectCatalogWritable> & {
  syncedAt?: string
}
