export interface IDbProjectCatalog {
  id: string
  projectSlug: string
  repoName: string
  repoUrl: string
  criticalityScore: number | null
  syncedAt: string | null
  createdAt: string | null
  updatedAt: string | null
}

type ProjectCatalogWritable = Pick<
  IDbProjectCatalog,
  'projectSlug' | 'repoName' | 'repoUrl' | 'criticalityScore'
>

export type IDbProjectCatalogCreate = Omit<ProjectCatalogWritable, 'criticalityScore'> & {
  criticalityScore?: number
}

export type IDbProjectCatalogUpdate = Partial<ProjectCatalogWritable> & {
  syncedAt?: string
}
