import { DEFAULT_TENANT_ID, generateUUIDv1 } from '@crowd/common'
import { DbStore, RepositoryBase } from '@crowd/database'
import { Logger } from '@crowd/logging'
import { WebhookState, WebhookType } from '@crowd/types'

import { IWebhookData } from './incomingWebhook.data'

export default class IncomingWebhookRepository extends RepositoryBase<IncomingWebhookRepository> {
  constructor(dbStore: DbStore, parentLog: Logger) {
    super(dbStore, parentLog)
  }

  public async getWebhookById(id: string): Promise<IWebhookData> {
    const result = await this.db().oneOrNone(
      `
        select
            iw.id,
            iw."integrationId",
            iw.state,
            iw.type,
            iw.payload,
            iw."createdAt" as "createdAt",
            i.platform as "platform"
        from 
            "incomingWebhooks" iw
        join "integrations" i on iw."integrationId" = i.id
        where
             iw.id = $(id)
      `,
      {
        id,
      },
    )

    return result
  }

  public async deleteWebhook(id: string): Promise<void> {
    await this.db().none(`delete from "incomingWebhooks" where id = $(id)`, {
      id,
    })
  }

  public async markWebhookPending(id: string): Promise<void> {
    await this.db().none(
      `
        update "incomingWebhooks"
        set 
          state = $(state),
          error = null,
          "processedAt" = null
        where id = $(id)
      `,
      {
        id,
        state: WebhookState.PENDING,
      },
    )
  }

  public async markWebhooksPendingBatch(ids: string[]): Promise<void> {
    await this.db().none(
      `
        update "incomingWebhooks"
        set 
          state = $(state),
          error = null,
          "processedAt" = null
        where id in ($(ids:csv))
      `,
      {
        ids,
        state: WebhookState.PENDING,
      },
    )
  }

  public async markWebhookError(id: string, error: unknown): Promise<void> {
    await this.db().none(
      `
        update "incomingWebhooks"
        set 
          state = $(state),
          error = $(error),
          "processedAt" = now(),
          retries = retries + 1
        where id = $(id)
      `,
      {
        id,
        state: WebhookState.ERROR,
        error: JSON.stringify(error),
      },
    )
  }

  public async createWebhook(
    integrationId: string,
    type: string,
    payload: unknown,
  ): Promise<string> {
    const id = generateUUIDv1()
    const result = await this.db().result(
      `
        insert into "incomingWebhooks" (
          id,
          "tenantId",
          "integrationId",
          state,
          type,
          payload
        ) values (
          $(id),
          $(tenantId),
          $(integrationId),
          $(state),
          $(type),
          $(payload)
        )
        returning id
      `,
      {
        id,
        tenantId: DEFAULT_TENANT_ID,
        integrationId,
        type,
        payload: JSON.stringify(payload),
        state: WebhookState.PENDING,
      },
    )

    this.checkUpdateRowCount(result.rowCount, 1)

    return id
  }

  public async getFailedWebhooks(limit: number): Promise<
    {
      id: string
      platform: string
    }[]
  > {
    const results = await this.db().manyOrNone(
      `
        select
            iw.id,
            i.platform as "platform"
        from 
            "incomingWebhooks" iw
        join "integrations" i on iw."integrationId" = i.id
        where
             iw.state = $(state)
             and iw.type != $(type)
        limit $(limit)
      `,
      {
        state: WebhookState.ERROR,
        type: WebhookType.DISCOURSE,
        limit,
      },
    )

    return results
  }
}
