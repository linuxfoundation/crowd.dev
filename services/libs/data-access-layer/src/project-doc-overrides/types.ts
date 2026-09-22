export interface IDbProjectDocOverride {
  id: string
  projectId: string
  docsUrl: string
  submittedBy: string
  submittedAt: string
  active: boolean
  createdAt: string
  updatedAt: string
}

export interface IProjectDocOverrideCreate {
  projectId: string
  docsUrl: string
  submittedBy: string
}
