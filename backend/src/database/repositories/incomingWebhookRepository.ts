import { QueryTypes } from 'sequelize'

import { generateUUIDv1 } from '@crowd/common'

import {
  DbIncomingWebhookInsertData,
  ErrorWebhook,
  IncomingWebhookData,
  PendingWebhook,
  WebhookState,
  WebhookType,
} from '../../types/webhooks'

import { IRepositoryOptions } from './IRepositoryOptions'
import { RepositoryBase } from './repositoryBase'

/* eslint-disable class-methods-use-this */

export default class IncomingWebhookRepository extends RepositoryBase<
  IncomingWebhookData,
  string,
  DbIncomingWebhookInsertData,
  unknown,
  unknown
> {
  public constructor(options: IRepositoryOptions) {
    super(options, true)
  }

  async create(data: DbIncomingWebhookInsertData): Promise<IncomingWebhookData> {
    const transaction = this.transaction

    const id = generateUUIDv1()

    const results = await this.seq.query(
      `
    insert into "incomingWebhooks"(id, "tenantId", "integrationId", state, type, payload)
    values(:id, :tenantId, :integrationId, :state, :type, :payload)
    returning "createdAt"
    `,
      {
        replacements: {
          id,
          tenantId: data.tenantId,
          integrationId: data.integrationId,
          type: data.type,
          state: WebhookState.PENDING,
          payload: JSON.stringify(data.payload),
        },
        type: QueryTypes.INSERT,
        transaction,
      },
    )

    return {
      id,
      state: WebhookState.PENDING,
      ...data,
      processedAt: null,
      error: null,
      createdAt: results[0][0].createdAt.toISOString(),
    }
  }

  override async findById(id: string): Promise<IncomingWebhookData> {
    const transaction = this.transaction

    const seq = this.seq

    const results = await seq.query(
      `
      select id,
             "tenantId",
             "integrationId",
             state,
             type,
             payload,
             "processedAt",
             error,
             "createdAt"
      from "incomingWebhooks"
      where id = :id
    `,
      {
        replacements: {
          id,
        },
        type: QueryTypes.SELECT,
        transaction,
      },
    )

    if (results.length === 0) {
      return null
    }

    const data = results[0] as any

    return {
      id: data.id,
      tenantId: data.tenantId,
      integrationId: data.integrationId,
      state: data.state,
      type: data.type,
      payload: data.payload,
      processedAt: data.processedAt ? data.processedAt.toISOString() : null,
      error: data.error,
      createdAt: data.createdAt.toISOString(),
    }
  }

  async markCompleted(id: string): Promise<void> {
    const transaction = this.transaction

    const [, rowCount] = await this.seq.query(
      `
      update "incomingWebhooks"
      set state = :state,
          error = null,
          "processedAt" = now()
      where id = :id
    `,
      {
        replacements: {
          id,
          state: WebhookState.PROCESSED,
        },
        type: QueryTypes.UPDATE,
        transaction,
      },
    )

    if (rowCount !== 1) {
      throw new Error(`Failed to mark webhook '${id}' as completed!`)
    }
  }

  async markAllPending(ids: string[]): Promise<void> {
    const transaction = this.transaction

    await this.seq.query(
      `
      update "incomingWebhooks"
      set state = :state,
          error = null,
          "processedAt" = now()
      where id in (:ids)
    `,
      {
        replacements: {
          ids,
          state: WebhookState.PENDING,
        },
        type: QueryTypes.UPDATE,
        transaction,
      },
    )
  }

  async markError(id: string, error: any): Promise<void> {
    const transaction = this.transaction

    const errorPayload = {
      errorMessage: error.message,
      errorString: JSON.stringify(error),
      errorStack: error.stack,
    }
    const [, rowCount] = await this.seq.query(
      `
      update "incomingWebhooks"
      set state = :state,
          error = :error,
          "processedAt" = now(),
          retries = retries + 1
          where id = :id
    `,
      {
        replacements: {
          id,
          state: WebhookState.ERROR,
          error: JSON.stringify(errorPayload),
        },
        type: QueryTypes.UPDATE,
        transaction,
      },
    )

    if (rowCount !== 1) {
      throw new Error(`Failed to mark webhook '${id}' as error!`)
    }
  }

  async findError(
    page: number,
    perPage: number,
    retryLimit: number = 5,
    type?: WebhookType,
  ): Promise<ErrorWebhook[]> {
    const transaction = this.transaction

    const seq = this.seq

    let query = `
      select iw.id, iw."tenantId"
      from "incomingWebhooks" iw
      left join integrations i on i.id = iw."integrationId"
      where iw.state = :error
      and iw.retries < ${retryLimit}
      and ( not (iw.error::jsonb ? 'originalMessage') or ((iw.error::jsonb ? 'originalMessage') and iw.error->>'originalMessage' <> 'Bad credentials'))
      and i.id is not null
    `

    if (type) {
      query += ` and iw.type = :type `
    }

    query += ` order by iw."createdAt" desc
    limit ${perPage} offset ${(page - 1) * perPage};`

    const results = await seq.query(query, {
      replacements: {
        error: WebhookState.ERROR,
        type,
      },
      type: QueryTypes.SELECT,
      transaction,
    })

    return results as ErrorWebhook[]
  }

  async findPending(page: number, perPage: number): Promise<PendingWebhook[]> {
    const transaction = this.transaction

    const seq = this.seq

    const query = `
      select id, "tenantId"
      from "incomingWebhooks"
      where state = :pending
        and "createdAt" < now() - interval '1 hour'
        and type not in ('GITHUB')
      limit ${perPage} offset ${(page - 1) * perPage};
    `

    const results = await seq.query(query, {
      replacements: {
        pending: WebhookState.PENDING,
      },
      type: QueryTypes.SELECT,
      transaction,
    })

    return results as PendingWebhook[]
  }

  async cleanUpOrphanedWebhooks(): Promise<void> {
    const seq = this.seq

    const cleanQuery = `
    delete from "incomingWebhooks" iw
    where not exists(select 1 from integrations i where (i.id = iw."integrationId" and i."deletedAt" is null));                   
    `

    await seq.query(cleanQuery, {
      type: QueryTypes.DELETE,
    })
  }

  async cleanUpOldWebhooks(months: number): Promise<void> {
    const seq = this.seq

    const cleanQuery = `
        delete from "incomingWebhooks" where state = :processed and "processedAt" < now() - interval '${months} months';                     
    `

    await seq.query(cleanQuery, {
      replacements: {
        processed: WebhookState.PROCESSED,
      },
      type: QueryTypes.DELETE,
    })
  }

  async checkWebhooksExistForIntegration(integrationId: string): Promise<boolean> {
    interface QueryResult {
      count: number
    }

    const transaction = this.transaction

    const results: QueryResult[] = await this.seq.query(
      `
      select count(*)::int as count
      from "incomingWebhooks"
      where "integrationId" = :integrationId
      limit 1
    `,
      {
        replacements: {
          integrationId,
        },
        type: QueryTypes.SELECT,
        transaction,
      },
    )

    return results.length > 0 && results[0].count > 0
  }
}
