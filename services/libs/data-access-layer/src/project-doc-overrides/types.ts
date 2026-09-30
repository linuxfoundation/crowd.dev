export interface IDbProjectDocOverride {
  id: string
  projectId: string
  // null means the project has no docs.
  docsUrl: string | null
  submittedBy: string
  submittedAt: string
  active: boolean
  createdAt: string
  updatedAt: string
}

export interface IProjectDocOverrideCreate {
  projectId: string
  docsUrl: string | null
  submittedBy: string
}
