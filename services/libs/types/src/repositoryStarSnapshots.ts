export interface IRepositoryStarSnapshot {
  id?: string
  repositoryId: string
  starCount: number
  capturedAt: string
}

export interface IRepoForStarSnapshot {
  repositoryId: string
  repoUrl: string
}
