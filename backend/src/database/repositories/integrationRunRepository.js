"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const sequelize_1 = require("sequelize");
const common_1 = require("@crowd/common");
const types_1 = require("@crowd/types");
const conf_1 = require("../../conf");
const integrationStreamTypes_1 = require("../../types/integrationStreamTypes");
const repositoryBase_1 = require("./repositoryBase");
class IntegrationRunRepository extends repositoryBase_1.RepositoryBase {
    constructor(options) {
        super(options, true);
    }
    async findDelayedRuns(page, perPage) {
        const transaction = this.transaction;
        const seq = this.seq;
        const query = `
      select  id,
            "tenantId",
            "integrationId",
            onboarding,
            state,
            "delayedUntil",
            "processedAt",
            error,
            "createdAt",
            "updatedAt"
      from "integrationRuns"
      where state = :delayedState and "delayedUntil" <= now()
      order by "createdAt" desc
      limit ${perPage} offset ${(page - 1) * perPage}
    `;
        const results = await seq.query(query, {
            replacements: {
                delayedState: types_1.IntegrationRunState.DELAYED,
            },
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        });
        return results;
    }
    async findIntegrationsByState(states, page, perPage, lastCreatedAt) {
        const seq = this.seq;
        const replacements = {};
        const stateParams = states.map((state, index) => {
            replacements[`state${index}`] = state;
            return `:state${index}`;
        });
        const conditions = [];
        if (lastCreatedAt) {
            replacements.lastCreatedAt = lastCreatedAt;
            conditions.push('"createdAt" < :lastCreatedAt');
        }
        let conditionString = '';
        if (conditions.length > 0) {
            conditionString = ` and ${conditions.join(' and ')}`;
        }
        const query = `
      select  id,
            "tenantId",
            "integrationId",
            onboarding,
            state,
            "delayedUntil",
            "processedAt",
            error,
            "createdAt",
            "updatedAt"
      from "integrationRuns"
      where state in (${stateParams.join(', ')}) ${conditionString}
      order by "createdAt" desc
      limit ${perPage} offset ${(page - 1) * perPage}
    `;
        const results = await seq.query(query, {
            replacements,
            type: sequelize_1.QueryTypes.SELECT,
        });
        return results;
    }
    async findLastRun(integrationId) {
        const transaction = this.transaction;
        const seq = this.seq;
        const query = `
    select  id,
            "tenantId",
            "integrationId",
            onboarding,
            state,
            "delayedUntil",
            "processedAt",
            error,
            "createdAt",
            "updatedAt"
    from "integrationRuns"
    where "integrationId" = :integrationId
    order by "createdAt" desc
    limit 1
    `;
        const results = await seq.query(query, {
            replacements: {
                integrationId,
            },
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        });
        if (results.length === 0) {
            return undefined;
        }
        return results[0];
    }
    async findStuckIntegrationRuns(integrationId, hoursThreshold = 24) {
        const transaction = this.transaction;
        const seq = this.seq;
        const replacements = {
            delayedState: types_1.IntegrationRunState.DELAYED,
            processingState: types_1.IntegrationRunState.PROCESSING,
            pendingState: types_1.IntegrationRunState.PENDING,
            integrationId,
        };
        const runsWithStuckStreamsQuery = `
    SELECT DISTINCT run.id, run.onboarding
    FROM integration.runs run
    JOIN integration.streams strm ON run."id" = strm."runId"
    WHERE 
      run.state IN (:delayedState, :processingState, :pendingState)
    AND strm.state IN (:delayedState, :pendingState)
    AND run."integrationId" = :integrationId
    AND run."createdAt" < NOW() - INTERVAL '${hoursThreshold} hours'
    `;
        const results = await seq.query(runsWithStuckStreamsQuery, {
            replacements,
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        });
        return results;
    }
    async cleanupOrphanedIntegrationRuns(integrationId, hoursThreshold = 24) {
        const transaction = this.transaction;
        const seq = this.seq;
        const replacements = {
            integrationId,
            delayedState: types_1.IntegrationRunState.DELAYED,
            processingState: types_1.IntegrationRunState.PROCESSING,
            pendingState: types_1.IntegrationRunState.PENDING,
        };
        const query = `
    DELETE FROM integration.runs run
    WHERE NOT EXISTS (
        SELECT 1 
        FROM integration.streams strm 
        WHERE strm."runId" = run."id"
    )
    AND run."integrationId" = :integrationId
    AND run."createdAt" < NOW() - INTERVAL '${hoursThreshold} hours'
    AND run.state IN (:delayedState, :processingState, :pendingState);
    `;
        await seq.query(query, {
            replacements,
            type: sequelize_1.QueryTypes.DELETE,
            transaction,
        });
    }
    async findLastProcessingRunInNewFramework(integrationId) {
        const transaction = this.transaction;
        const seq = this.seq;
        const condition = ` "integrationId" = :integrationId `;
        const replacements = {
            delayedState: types_1.IntegrationRunState.DELAYED,
            processingState: types_1.IntegrationRunState.PROCESSING,
            pendingState: types_1.IntegrationRunState.PENDING,
            integrationId,
        };
        const query = `
    select id
    from integration.runs
    where state in (:delayedState, :processingState, :pendingState) and ${condition}
    order by "createdAt" desc
    limit 1
    `;
        const results = await seq.query(query, {
            replacements,
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        });
        if (results.length === 1) {
            return results[0].id;
        }
        return undefined;
    }
    async findLastProcessingRun(integrationId, ignoreId) {
        const transaction = this.transaction;
        const seq = this.seq;
        let condition = ``;
        const replacements = {
            delayedState: types_1.IntegrationRunState.DELAYED,
            processingState: types_1.IntegrationRunState.PROCESSING,
            pendingState: types_1.IntegrationRunState.PENDING,
        };
        if (integrationId) {
            condition = ` "integrationId" = :integrationId `;
            replacements.integrationId = integrationId;
        }
        else {
            throw new Error(`integrationId must be provided!`);
        }
        if (ignoreId) {
            condition = `${condition} and id <> :ignoreId`;
            replacements.ignoreId = ignoreId;
        }
        const query = `
    select  id,
            "tenantId",
            "integrationId",
            onboarding,
            state,
            "delayedUntil",
            "processedAt",
            error,
            "createdAt",
            "updatedAt"
    from "integrationRuns"
    where state in (:delayedState, :processingState, :pendingState) and ${condition}
    order by "createdAt" desc
    limit 1
    `;
        const results = await seq.query(query, {
            replacements,
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        });
        if (results.length === 1) {
            return results[0];
        }
        return undefined;
    }
    async findById(id) {
        const transaction = this.transaction;
        const seq = this.seq;
        const query = `
    select id,
           "tenantId",
          "integrationId",
          onboarding,
          state,
          "delayedUntil",
          "processedAt",
          error,
          "createdAt",
          "updatedAt"
      from "integrationRuns" where id = :id;      
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
    async create(data) {
        const transaction = this.transaction;
        const seq = this.seq;
        const id = (0, common_1.generateUUIDv1)();
        const query = `
      insert into "integrationRuns"(id, "tenantId", "integrationId", onboarding, state)
      values(:id, :tenantId, :integrationId, :onboarding, :state)
      returning "createdAt";
    `;
        const result = await seq.query(query, {
            replacements: {
                id,
                tenantId: data.tenantId,
                integrationId: data.integrationId || null,
                onboarding: data.onboarding,
                state: data.state,
            },
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        });
        if (result.length !== 1) {
            throw new Error(`Expected 1 row to be inserted, got ${result.length} rows instead.`);
        }
        return {
            id,
            tenantId: data.tenantId,
            integrationId: data.integrationId,
            onboarding: data.onboarding,
            state: data.state,
            delayedUntil: null,
            processedAt: null,
            error: null,
            createdAt: result[0].createdAt,
            updatedAt: result[0].createdAt,
        };
    }
    async markProcessing(id) {
        const transaction = this.transaction;
        const seq = this.seq;
        const query = `
      update "integrationRuns"
      set state = :state,
          "delayedUntil" = null,
          "updatedAt" = now()
      where id = :id
    `;
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        const [_, rowCount] = await seq.query(query, {
            replacements: {
                id,
                state: types_1.IntegrationRunState.PROCESSING,
            },
            type: sequelize_1.QueryTypes.UPDATE,
            transaction,
        });
        if (rowCount !== 1) {
            throw new Error(`Expected 1 row to be updated, got ${rowCount} rows instead.`);
        }
    }
    async restart(id) {
        const transaction = this.transaction;
        const seq = this.seq;
        const query = `
      update "integrationRuns"
        set state = :state,
            "delayedUntil" = null,
            "processedAt" = null,
            error = null,
            "updatedAt" = now()
      where id = :id
    `;
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        const [_, rowCount] = await seq.query(query, {
            replacements: {
                id,
                state: types_1.IntegrationRunState.PENDING,
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
      update "integrationRuns"
      set state = :state,
          error = :error,
          "updatedAt" = now()
      where id = :id
    `;
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        const [_, rowCount] = await seq.query(query, {
            replacements: {
                id,
                state: types_1.IntegrationRunState.ERROR,
                error: JSON.stringify(error),
            },
            type: sequelize_1.QueryTypes.UPDATE,
            transaction,
        });
        if (rowCount !== 1) {
            throw new Error(`Expected 1 row to be updated, got ${rowCount} rows instead.`);
        }
    }
    async delay(id, until) {
        const transaction = this.transaction;
        const seq = this.seq;
        const query = `
      update "integrationRuns"
      set "delayedUntil" = :until,
          state = :state,
          "updatedAt" = now()
      where id = :id
    `;
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        const [_, rowCount] = await seq.query(query, {
            replacements: {
                id,
                until,
                state: types_1.IntegrationRunState.DELAYED,
            },
            type: sequelize_1.QueryTypes.UPDATE,
            transaction,
        });
        if (rowCount !== 1) {
            throw new Error(`Expected 1 row to be updated, got ${rowCount} rows instead.`);
        }
    }
    async touch(id) {
        const transaction = this.transaction;
        const seq = this.seq;
        const query = `
      update "integrationRuns"
      set "updatedAt" = now()
      where id = :id
    `;
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        const [_, rowCount] = await seq.query(query, {
            replacements: {
                id,
            },
            type: sequelize_1.QueryTypes.UPDATE,
            transaction,
        });
        if (rowCount !== 1) {
            throw new Error(`Expected 1 row to be updated, got ${rowCount} rows instead.`);
        }
    }
    async touchState(id) {
        const transaction = this.transaction;
        const seq = this.seq;
        const query = `
    update "integrationRuns"
    set "processedAt" = case
                            when (select count(s.id) =
                                        (count(s.id) filter ( where s.state = :successStreamState ) +
                                          count(s.id)
                                          filter (where s.state = :errorStreamState and s.retries >= :maxRetries))
                                  from "integrationStreams" s
                                  where s."runId" = :id) then now()
        end,
        state         = case
                            when (select (count(s.id) =
                                          (count(s.id) filter ( where s.state = :successStreamState ) +
                                          count(s.id) filter (where s.state = :errorStreamState))) and
                                        (count(s.id)
                                          filter (where s.state = :errorStreamState and s.retries < :maxRetries)) = 0
                                  from "integrationStreams" s
                                  where s."runId" = :id) then :successRunState
                            when (select (count(s.id) =
                                          (count(s.id) filter ( where s.state = :successStreamState ) +
                                          count(s.id) filter (where s.state = :errorStreamState))) and
                                        (count(s.id)
                                          filter (where s.state = :errorStreamState and s.retries >= :maxRetries)) > 0
                                  from "integrationStreams" s
                                  where s."runId" = :id) then :errorRunState
                            else state
            end,
        "updatedAt"   = now()
    where id = :id
    returning state;
    `;
        const result = await seq.query(query, {
            replacements: {
                id,
                successStreamState: integrationStreamTypes_1.IntegrationStreamState.PROCESSED,
                errorStreamState: integrationStreamTypes_1.IntegrationStreamState.ERROR,
                successRunState: types_1.IntegrationRunState.PROCESSED,
                errorRunState: types_1.IntegrationRunState.ERROR,
                maxRetries: conf_1.INTEGRATION_PROCESSING_CONFIG.maxRetries,
            },
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        });
        if (result.length !== 1) {
            throw new Error(`Expected 1 row to be updated, got ${result.length} rows instead.`);
        }
        return result[0].state;
    }
    async cleanupOldRuns(months) {
        const seq = this.seq;
        const cleanQuery = `
        delete from "integrationRuns" where state = :processed and "processedAt" < now() - interval '${months} months';                     
    `;
        await seq.query(cleanQuery, {
            replacements: {
                processed: types_1.IntegrationRunState.PROCESSED,
            },
            type: sequelize_1.QueryTypes.DELETE,
        });
    }
}
exports.default = IntegrationRunRepository;
//# sourceMappingURL=integrationRunRepository.js.map