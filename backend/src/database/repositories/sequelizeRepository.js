"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const lodash_1 = __importDefault(require("lodash"));
const sequelize_1 = require("sequelize");
const common_1 = require("@crowd/common");
const database_1 = require("@crowd/data-access-layer/src/database");
const logging_1 = require("@crowd/logging");
const opensearch_1 = require("@crowd/opensearch");
const redis_1 = require("@crowd/redis");
const temporal_1 = require("@crowd/temporal");
const conf_1 = require("../../conf");
const databaseConnection_1 = require("../databaseConnection");
const sequelizeQueryExecutor_1 = require("../sequelizeQueryExecutor");
/**
 * Abstracts some basic Sequelize operations.
 * See https://sequelize.org/v5/index.html to learn how to customize it.
 */
class SequelizeRepository {
    /**
     * Cleans the database.
     */
    static async cleanDatabase(database) {
        if (!conf_1.IS_TEST_ENV) {
            throw new Error('Clean database only allowed for test!');
        }
        await database.sequelize.sync({ force: true });
    }
    static async getDefaultIRepositoryOptions(user, tenant, segments) {
        let temporal;
        if (conf_1.TEMPORAL_CONFIG.serverUrl) {
            temporal = await (0, temporal_1.getTemporalClient)(conf_1.TEMPORAL_CONFIG);
        }
        let productDb;
        if (conf_1.PRODUCT_DB_CONFIG) {
            productDb = await (0, database_1.getDbConnection)(conf_1.PRODUCT_DB_CONFIG);
        }
        const opensearch = await (0, opensearch_1.getOpensearchClient)(conf_1.OPENSEARCH_CONFIG);
        return {
            log: (0, logging_1.getServiceLogger)(),
            database: await (0, databaseConnection_1.databaseInit)(),
            currentTenant: tenant,
            currentUser: user,
            currentSegments: segments,
            bypassPermissionValidation: true,
            language: 'en',
            redis: await (0, redis_1.getRedisClient)(conf_1.REDIS_CONFIG, true),
            temporal,
            productDb,
            opensearch,
        };
    }
    /**
     * Returns the currentUser if it exists on the options.
     */
    static getCurrentUser(options) {
        return (options && options.currentUser) || { id: null };
    }
    /**
     * Returns the tenant if it exists on the options.
     */
    static getCurrentTenant(options) {
        return (options && options.currentTenant) || { id: null };
    }
    static getCurrentSegments(options) {
        return (options && options.currentSegments) || [];
    }
    static getStrictlySingleActiveSegment(options) {
        if (options.currentSegments.length !== 1) {
            throw new common_1.Error400(`This operation can have exactly one segment. Found ${options.currentSegments.length} segments.`);
        }
        return options.currentSegments[0];
    }
    static getStrictlySingleProjectGroupSegment(options) {
        const segment = this.getStrictlySingleActiveSegment(options);
        if (segment.parentId != null || segment.grandparentId != null) {
            throw new common_1.Error400(`This operation requires a project group segment. Segment ${segment.id} is not a project group.`);
        }
        return segment;
    }
    /**
     * Returns the transaction if it exists on the options.
     */
    static getTransaction(options) {
        return (options && options.transaction) || undefined;
    }
    /**
     * Creates a database transaction.
     */
    static async createTransaction(options) {
        if (options.transaction) {
            if (options.transaction.crowdNestedTransactions !== undefined) {
                options.transaction.crowdNestedTransactions++;
            }
            else {
                options.transaction.crowdNestedTransactions = 1;
            }
            return options.transaction;
        }
        return options.database.sequelize.transaction();
    }
    static async withTx(options, fn) {
        const tx = await this.createTransaction(options);
        try {
            const result = await fn(tx);
            await this.commitTransaction(tx);
            return result;
        }
        catch (error) {
            await this.rollbackTransaction(tx);
            throw error;
        }
    }
    /**
     * Creates a transactional repository options instance
     */
    static async createTransactionalRepositoryOptions(options) {
        const transaction = await this.createTransaction(options);
        return {
            ...options,
            transaction,
        };
    }
    /**
     * Commits a database transaction.
     */
    static async commitTransaction(transaction) {
        if (transaction.crowdNestedTransactions !== undefined &&
            transaction.crowdNestedTransactions > 0) {
            transaction.crowdNestedTransactions--;
            return Promise.resolve();
        }
        return transaction.commit();
    }
    /**
     * Rolls back a database transaction.
     */
    static async rollbackTransaction(transaction) {
        if (transaction.crowdNestedTransactions !== undefined &&
            transaction.crowdNestedTransactions > 0) {
            transaction.crowdNestedTransactions--;
            return Promise.resolve();
        }
        return transaction.rollback();
    }
    static handleUniqueFieldError(error, language, entityName) {
        if (!(error instanceof sequelize_1.UniqueConstraintError)) {
            return;
        }
        const fieldName = lodash_1.default.get(error, 'errors[0].path');
        throw new common_1.Error400(language, `entities.${entityName}.errors.unique.${fieldName}`);
    }
    static getSequelize(options) {
        return options.database.sequelize;
    }
    static getQueryExecutor(options) {
        const seq = this.getSequelize(options);
        const transaction = this.getTransaction(options);
        return transaction
            ? new sequelizeQueryExecutor_1.TransactionalSequelizeQueryExecutor(seq, transaction)
            : new sequelizeQueryExecutor_1.SequelizeQueryExecutor(seq);
    }
    static getSegmentIds(options) {
        return options.currentSegments.map((s) => s.id);
    }
}
exports.default = SequelizeRepository;
//# sourceMappingURL=sequelizeRepository.js.map