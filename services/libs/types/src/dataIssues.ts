import { DataIssueEntity } from './enums'

export interface IDataIssue {
  id: string
  entity: DataIssueEntity
  profileUrl: string
  dataIssue: string
  dataType: string
  description: string
  issueUrl: string
  createdById: string
  createdAt: Date
  updatedAt: Date
}

export interface IGithubIssueReporterConfiguration {
  appId: string
  privateKey: string
  installationId: string
  webhookSecret: string
  repo: string
  projectNumber: string
  projectTeam: string
  projectStatus: string
}
