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

export interface IRepositoryStarBackfillStatus {
  repositoryId: string
  consecutiveFailures: number
  lastErrorClass: string | null
  lastErrorMessage: string | null
  deadLetteredAt: string | null
  completedAt: string | null
  createdAt: string
  updatedAt: string
}
