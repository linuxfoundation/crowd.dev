"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const sequelize_1 = require("sequelize");
const common_1 = require("@crowd/common");
const webhooks_1 = require("../../types/webhooks");
const repositoryBase_1 = require("./repositoryBase");
/* eslint-disable class-methods-use-this */
class IncomingWebhookRepository extends repositoryBase_1.RepositoryBase {
    constructor(options) {
        super(options, true);
    }
    async create(data) {
        const transaction = this.transaction;
        const id = (0, common_1.generateUUIDv1)();
        const results = await this.seq.query(`
    insert into "incomingWebhooks"(id, "tenantId", "integrationId", state, type, payload)
    values(:id, :tenantId, :integrationId, :state, :type, :payload)
    returning "createdAt"
    `, {
            replacements: {
                id,
                tenantId: data.tenantId,
                integrationId: data.integrationId,
                type: data.type,
                state: webhooks_1.WebhookState.PENDING,
                payload: JSON.stringify(data.payload),
            },
            type: sequelize_1.QueryTypes.INSERT,
            transaction,
        });
        return {
            id,
            state: webhooks_1.WebhookState.PENDING,
            ...data,
            processedAt: null,
            error: null,
            createdAt: results[0][0].createdAt.toISOString(),
        };
    }
    async findById(id) {
        const transaction = this.transaction;
        const seq = this.seq;
        const results = await seq.query(`
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
    `, {
            replacements: {
                id,
            },
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        });
        if (results.length === 0) {
            return null;
        }
        const data = results[0];
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
        };
    }
    async markCompleted(id) {
        const transaction = this.transaction;
        const [, rowCount] = await this.seq.query(`
      update "incomingWebhooks"
      set state = :state,
          error = null,
          "processedAt" = now()
      where id = :id
    `, {
            replacements: {
                id,
                state: webhooks_1.WebhookState.PROCESSED,
            },
            type: sequelize_1.QueryTypes.UPDATE,
            transaction,
        });
        if (rowCount !== 1) {
            throw new Error(`Failed to mark webhook '${id}' as completed!`);
        }
    }
    async markAllPending(ids) {
        const transaction = this.transaction;
        await this.seq.query(`
      update "incomingWebhooks"
      set state = :state,
          error = null,
          "processedAt" = now()
      where id in (:ids)
    `, {
            replacements: {
                ids,
                state: webhooks_1.WebhookState.PENDING,
            },
            type: sequelize_1.QueryTypes.UPDATE,
            transaction,
        });
    }
    async markError(id, error) {
        const transaction = this.transaction;
        const errorPayload = {
            errorMessage: error.message,
            errorString: JSON.stringify(error),
            errorStack: error.stack,
        };
        const [, rowCount] = await this.seq.query(`
      update "incomingWebhooks"
      set state = :state,
          error = :error,
          "processedAt" = now(),
          retries = retries + 1
          where id = :id
    `, {
            replacements: {
                id,
                state: webhooks_1.WebhookState.ERROR,
                error: JSON.stringify(errorPayload),
            },
            type: sequelize_1.QueryTypes.UPDATE,
            transaction,
        });
        if (rowCount !== 1) {
            throw new Error(`Failed to mark webhook '${id}' as error!`);
        }
    }
    async findError(page, perPage, retryLimit = 5, type) {
        const transaction = this.transaction;
        const seq = this.seq;
        let query = `
      select iw.id, iw."tenantId"
      from "incomingWebhooks" iw
      left join integrations i on i.id = iw."integrationId"
      where iw.state = :error
      and iw.retries < ${retryLimit}
      and ( not (iw.error::jsonb ? 'originalMessage') or ((iw.error::jsonb ? 'originalMessage') and iw.error->>'originalMessage' <> 'Bad credentials'))
      and i.id is not null
    `;
        if (type) {
            query += ` and iw.type = :type `;
        }
        query += ` order by iw."createdAt" desc
    limit ${perPage} offset ${(page - 1) * perPage};`;
        const results = await seq.query(query, {
            replacements: {
                error: webhooks_1.WebhookState.ERROR,
                type,
            },
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        });
        return results;
    }
    async findPending(page, perPage) {
        const transaction = this.transaction;
        const seq = this.seq;
        const query = `
      select id, "tenantId"
      from "incomingWebhooks"
      where state = :pending
        and "createdAt" < now() - interval '1 hour'
        and type not in ('GITHUB')
      limit ${perPage} offset ${(page - 1) * perPage};
    `;
        const results = await seq.query(query, {
            replacements: {
                pending: webhooks_1.WebhookState.PENDING,
            },
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        });
        return results;
    }
    async cleanUpOrphanedWebhooks() {
        const seq = this.seq;
        const cleanQuery = `
    delete from "incomingWebhooks" iw
    where not exists(select 1 from integrations i where (i.id = iw."integrationId" and i."deletedAt" is null));                   
    `;
        await seq.query(cleanQuery, {
            type: sequelize_1.QueryTypes.DELETE,
        });
    }
    async cleanUpOldWebhooks(months) {
        const seq = this.seq;
        const cleanQuery = `
        delete from "incomingWebhooks" where state = :processed and "processedAt" < now() - interval '${months} months';                     
    `;
        await seq.query(cleanQuery, {
            replacements: {
                processed: webhooks_1.WebhookState.PROCESSED,
            },
            type: sequelize_1.QueryTypes.DELETE,
        });
    }
    async checkWebhooksExistForIntegration(integrationId) {
        const transaction = this.transaction;
        const results = await this.seq.query(`
      select count(*)::int as count
      from "incomingWebhooks"
      where "integrationId" = :integrationId
      limit 1
    `, {
            replacements: {
                integrationId,
            },
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        });
        return results.length > 0 && results[0].count > 0;
    }
}
exports.default = IncomingWebhookRepository;
//# sourceMappingURL=incomingWebhookRepository.js.map