import SequelizeRepository from '@/database/repositories/sequelizeRepository'
import { OrganizationField, findOrgById } from '@crowd/data-access-layer'
import { createDataIssue } from '@crowd/data-access-layer/src/data_issues'
import { MemberField, findMemberById } from '@crowd/data-access-layer/src/members'
import { PgPromiseQueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { LoggerBase } from '@crowd/logging'
import { DataIssueEntity } from '@crowd/types'

import { createInternalIssue } from './helpers/githubIssueReporter'
import { IServiceOptions } from './IServiceOptions'

const DATA_ISSUE_LABELS = ['product-support', 'needs-triage', 'data-issue']

export interface IDataIssueCreatePayload {
  entity: DataIssueEntity
  profileUrl: string
  dataIssue: string
  dataType: string
  description: string
  githubIssueUrl: string
  createdById: string
}

export default class DataIssueService extends LoggerBase {
  private readonly qx: PgPromiseQueryExecutor

  options: IServiceOptions

  constructor(options: IServiceOptions) {
    super(options.log)
    this.options = options
  }

  public async createDataIssue(data: IDataIssueCreatePayload, entityId: string) {
    const qx = SequelizeRepository.getQueryExecutor(this.options)
    const user = SequelizeRepository.getCurrentUser(this.options)

    let entityName: string
    let reportedBy: string

    if (data.entity === DataIssueEntity.ORGANIZATION) {
      const organization = await findOrgById(qx, entityId, [
        OrganizationField.ID,
        OrganizationField.DISPLAY_NAME,
      ])
      entityName = organization.displayName
    } else if (data.entity === DataIssueEntity.PERSON) {
      const member = await findMemberById(qx, entityId, [MemberField.ID, MemberField.DISPLAY_NAME])
      entityName = member.displayName
    } else {
      throw new Error(`Unsupported data issue entity ${data.entity}!1`)
    }

    if (user.fullName) {
      reportedBy = `${user.fullName} - ${user.email}`
    } else {
      reportedBy = `${user.email}`
    }

    const title = `[Data Issue] ${entityName} (${data.entity[0].toUpperCase()}${data.entity
      .slice(1)
      .toLowerCase()})`
    const body = [
      `### Entity\n${entityName}`,
      `### Profile\n${data.profileUrl}`,
      `### Data Issue\n${data.dataIssue}`,
      `### Description\n${data.description}`,
      `### Reported by\n${reportedBy}`,
    ].join('\n\n')

    let issueUrl: string
    try {
      const issue = await createInternalIssue(this.log, title, body, DATA_ISSUE_LABELS)
      issueUrl = issue.html_url
    } catch (error) {
      this.log.error(error, 'Failed to create GitHub issue for data issue report!')
      throw new Error('Failed to report the data issue!')
    }

    return createDataIssue(qx, {
      ...data,
      issueUrl,
      createdById: user.id,
    })
  }
}
