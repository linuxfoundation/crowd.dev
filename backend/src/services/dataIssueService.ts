import axios from 'axios'

import { JIRA_ISSUE_REPORTER_CONFIG } from '@/conf'
import SequelizeRepository from '@/database/repositories/sequelizeRepository'
import { createHeading, createParagraph } from '@crowd/common'
import { OrganizationField, findOrgById } from '@crowd/data-access-layer'
import { createDataIssue } from '@crowd/data-access-layer/src/data_issues'
import { MemberField, findMemberById } from '@crowd/data-access-layer/src/members'
import { PgPromiseQueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { LoggerBase } from '@crowd/logging'
import { DataIssueEntity } from '@crowd/types'

import { IServiceOptions } from './IServiceOptions'

export interface IDataIssueCreatePayload {
  entity: DataIssueEntity
  profileUrl: string
  dataIssue: string
  dataType: string
  description: string
  githubIssueUrl: string
  createdById: string
}

interface IJiraCreateIssueResponse {
  id: string
  key: string
  self: string
  transitions: {
    status: string
    errorCollection: {
      errorMessages: string[]
      errors: object
    }
  }
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

    try {
      const result = await axios.post<IJiraCreateIssueResponse>(
        `${JIRA_ISSUE_REPORTER_CONFIG.apiUrl}/issue`,
        {
          fields: {
            project: {
              key: JIRA_ISSUE_REPORTER_CONFIG.projectKey,
            },
            summary: `[Data Issue] ${entityName} (${data.entity[0].toUpperCase()}${data.entity
              .slice(1)
              .toLowerCase()})`,
            description: {
              version: 1,
              type: 'doc',
              content: [
                createHeading('Entity'),
                createParagraph(entityName),
                createHeading('Profile'),
                createParagraph(data.profileUrl, true),
                createHeading('Data Issue'),
                createParagraph(data.dataIssue),
                createHeading('Description'),
                createParagraph(data.description),
                createHeading('Reported by'),
                createParagraph(reportedBy),
              ],
            },
            issuetype: {
              name: 'Task',
            },
            labels: ['data-issue'],
          },
        },
        {
          headers: {
            Authorization: `Basic ${Buffer.from(
              `${JIRA_ISSUE_REPORTER_CONFIG.apiTokenEmail}:${JIRA_ISSUE_REPORTER_CONFIG.token}`,
            ).toString('base64')}`,
          },
        },
      )

      const res = await createDataIssue(qx, {
        ...data,
        issueUrl: result.data.self,
        createdById: user.id,
      })

      return res
    } catch (error) {
      this.log.info(error)
      throw new Error('Error during session create!')
    }
  }
}
