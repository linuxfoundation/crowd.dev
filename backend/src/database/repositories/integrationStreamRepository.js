"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const lodash_1 = __importDefault(require("lodash"));
const sequelize_1 = require("sequelize");
const common_1 = require("@crowd/common");
const conf_1 = require("../../conf");
const integrationStreamTypes_1 = require("../../types/integrationStreamTypes");
const repositoryBase_1 = require("./repositoryBase");
class IntegrationStreamRepository extends repositoryBase_1.RepositoryBase {
    constructor(options) {
        super(options, true);
    }
    async findById(id) {
        const transaction = this.transaction;
        const seq = this.seq;
        const query = `
    select id,
          "runId",
          "tenantId",
          "integrationId",
          state,
          name,
          metadata,
          "processedAt",
          error,
          retries,
          "createdAt",
          "updatedAt"
      from "integrationStreams" where id = :id;      
    `;
        const result = await seq.query(query, {
            replacements: {
                id,
            },
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        });
        if (result.length !== 1) {
            throw new Error(`Expected 1 row to be selected, got ${result.length} rows instead.`);
        }
        return result[0];
    }
    async findByRunId(runId, page, perPage, states, orderBy, additionalConditions) {
        const transaction = this.transaction;
        const seq = this.seq;
        const replacements = {
            runId,
        };
        let condition = `1=1`;
        if (states && states.length > 0) {
            condition = `"state" in (:states)`;
            replacements.states = states;
        }
        let orderByCondition = 'order by id';
        if (orderBy) {
            orderByCondition = `order by ${orderBy}`;
        }
        if (additionalConditions && additionalConditions.length > 0) {
            condition = `${condition} and ${additionalConditions.join(' and ')}`;
        }
        const query = `
    select id,
          "runId",
          "tenantId",
          "integrationId",
          state,
          name,
          metadata,
          "processedAt",
          error,
          retries,
          "createdAt",
          "updatedAt"
      from "integrationStreams" where "runId" = :runId and ${condition}
      ${orderByCondition}
      limit ${perPage} offset ${(page - 1) * perPage};
    `;
        const result = await seq.query(query, {
            replacements,
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        });
        return result;
    }
    async bulkCreate(data) {
        const transaction = this.transaction;
        const seq = this.seq;
        const batches = lodash_1.default.chunk(data, 999);
        const results = [];
        const query = `
    insert into "integrationStreams"(id, "runId", "tenantId", "integrationId", state, name, metadata)
    values 
    `;
        for (const batch of batches) {
            let i = 0;
            const values = [];
            const replacements = {};
            for (const item of batch) {
                const id = (0, common_1.generateUUIDv1)();
                values.push(`(:id${i}, :runId${i}, :tenantId${i}, :integrationId${i}, :state${i}, :name${i}, :metadata${i})`);
                replacements[`id${i}`] = id;
                replacements[`runId${i}`] = item.runId;
                replacements[`tenantId${i}`] = item.tenantId;
                replacements[`state${i}`] = integrationStreamTypes_1.IntegrationStreamState.PENDING;
                replacements[`integrationId${i}`] = item.integrationId || null;
                replacements[`name${i}`] = item.name;
                replacements[`metadata${i}`] = JSON.stringify(item.metadata || {});
                i++;
            }
            const finalQuery = `${query} ${values.join(', ')} returning "createdAt";`;
            const batchResults = await seq.query(finalQuery, {
                replacements,
                type: sequelize_1.QueryTypes.SELECT,
                transaction,
            });
            if (batchResults.length !== batch.length) {
                throw new Error(`Expected ${batch.length} rows to be inserted, got ${batchResults.length} rows instead.`);
            }
            for (let j = 0; j < batch.length; j++) {
                const item = batch[j];
                const createdAt = batchResults[j].createdAt;
                results.push({
                    id: replacements[`id${j}`],
                    runId: item.runId,
                    tenantId: item.tenantId,
                    state: integrationStreamTypes_1.IntegrationStreamState.PENDING,
                    integrationId: item.integrationId,
                    name: item.name,
                    metadata: item.metadata || {},
                    createdAt,
                    updatedAt: createdAt,
                    processedAt: null,
                    error: null,
                    retries: null,
                });
            }
        }
        return results;
    }
    async create(data) {
        const transaction = this.transaction;
        const seq = this.seq;
        const id = (0, common_1.generateUUIDv1)();
        const query = `
      insert into "integrationStreams"(id, "runId", "tenantId", "integrationId", state, name, metadata)
      values(:id, :runId, :tenantId, :integrationId, :state, :name, :metadata)
      returning "createdAt";
    `;
        const result = await seq.query(query, {
            replacements: {
                id,
                runId: data.runId,
                tenantId: data.tenantId,
                state: integrationStreamTypes_1.IntegrationStreamState.PENDING,
                integrationId: data.integrationId || null,
                name: data.name,
                metadata: JSON.stringify(data.metadata || {}),
            },
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        });
        if (result.length !== 1) {
            throw new Error(`Expected 1 row to be inserted, got ${result.length} rows instead.`);
        }
        return {
            id,
            runId: data.runId,
            tenantId: data.tenantId,
            state: integrationStreamTypes_1.IntegrationStreamState.PENDING,
            integrationId: data.integrationId,
            name: data.name,
            metadata: data.metadata || {},
            createdAt: result[0].createdAt,
            updatedAt: result[0].createdAt,
            processedAt: null,
            error: null,
            retries: null,
        };
    }
    async markProcessing(id) {
        const transaction = this.transaction;
        const seq = this.seq;
        const query = `
      update "integrationStreams"
      set state = :state,
          "updatedAt" = now()
      where id = :id;
    `;
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        const [_, rowCount] = await seq.query(query, {
            replacements: {
                id,
                state: integrationStreamTypes_1.IntegrationStreamState.PROCESSING,
            },
            type: sequelize_1.QueryTypes.UPDATE,
            transaction,
        });
        if (rowCount !== 1) {
            throw new Error(`Expected 1 row to be updated, got ${rowCount} rows instead.`);
        }
    }
    async markProcessed(id) {
        const transaction = this.transaction;
        const seq = this.seq;
        const query = `
      update "integrationStreams"
      set state = :state,
          "processedAt" = now(),
          "updatedAt" = now()
      where id = :id;
    `;
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        const [_, rowCount] = await seq.query(query, {
            replacements: {
                id,
                state: integrationStreamTypes_1.IntegrationStreamState.PROCESSED,
            },
            type: sequelize_1.QueryTypes.UPDATE,
            transaction,
        });
        if (rowCount !== 1) {
            throw new Error(`Expected 1 row to be updated, got ${rowCount} rows instead.`);
        }
    }
    async markError(id, error) {
        const transaction = this.transaction;
        const seq = this.seq;
        const query = `
      update "integrationStreams"
      set state = :state,
          "processedAt" = now(),
          error = :error,
          retries = coalesce(retries, 0) + 1,
          "updatedAt" = now()
      where id = :id
      returning retries;
    `;
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        const result = await seq.query(query, {
            replacements: {
                id,
                error: JSON.stringify(error),
                state: integrationStreamTypes_1.IntegrationStreamState.ERROR,
            },
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        });
        if (result.length !== 1) {
            throw new Error(`Expected 1 row to be updated, got ${result.length} rows instead.`);
        }
        return result[0].retries;
    }
    async reset(id) {
        const transaction = this.transaction;
        const seq = this.seq;
        const query = `
      update "integrationStreams"
      set state = :state,
          "processedAt" = null,
          error = null,
          retries = null,
          "updatedAt" = now()
      where id = :id;
    `;
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        const [_, count] = await seq.query(query, {
            replacements: {
                id,
                state: integrationStreamTypes_1.IntegrationStreamState.PENDING,
            },
            type: sequelize_1.QueryTypes.UPDATE,
            transaction,
        });
        if (count !== 1) {
            throw new Error(`Expected 1 row to be updated, got ${count} rows instead.`);
        }
    }
    async getNextStreamToProcess(runId) {
        const transaction = this.transaction;
        const seq = this.seq;
        const query = `
    select  id,
            "runId",
            "tenantId",
            "integrationId",
            state,
            name,
            metadata,
            "processedAt",
            error,
            retries,
            "createdAt",
            "updatedAt"
        from "integrationStreams" 
        where 
          "runId" = :runId and
          (
            state = :pending or
            (
              state = :error and
              retries < :maxRetriesLimit and
              "updatedAt" < now() - make_interval(mins := 5 * retries)              
            )
          )
        order by "createdAt" asc
        limit 1;
    `;
        const results = await seq.query(query, {
            replacements: {
                runId,
                pending: integrationStreamTypes_1.IntegrationStreamState.PENDING,
                error: integrationStreamTypes_1.IntegrationStreamState.ERROR,
                maxRetriesLimit: conf_1.INTEGRATION_PROCESSING_CONFIG.maxRetries,
            },
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        });
        if (results.length === 0) {
            return null;
        }
        return results[0];
    }
}
exports.default = IntegrationStreamRepository;
//# sourceMappingURL=integrationStreamRepository.js.map