"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.TransactionalSequelizeQueryExecutor = exports.SequelizeQueryExecutor = void 0;
exports.optionsQx = optionsQx;
exports.optionsBgQx = optionsBgQx;
/* eslint-disable max-classes-per-file */
const sequelize_1 = require("sequelize");
const queryExecutor_1 = require("@crowd/data-access-layer/src/queryExecutor");
/** Sequelize-backed QueryExecutor for legacy backend repositories. */
class SequelizeQueryExecutor {
    constructor(sequelize, noTransaction = false) {
        this.sequelize = sequelize;
        this.noTransaction = noTransaction;
    }
    prepareOptions(options) {
        // When noTransaction=true, explicitly opt out of any CLS or implicit
        // transaction binding — used for background/fire-and-forget work that
        // must not inherit a parent request's transaction.
        return this.noTransaction ? { ...options, transaction: null } : options;
    }
    select(query, params) {
        return this.sequelize.query((0, queryExecutor_1.formatQuery)(query, params), this.prepareOptions({
            type: sequelize_1.QueryTypes.SELECT,
        }));
    }
    async selectNone(query, params) {
        const result = await this.sequelize.query((0, queryExecutor_1.formatQuery)(query, params), this.prepareOptions({
            type: sequelize_1.QueryTypes.SELECT,
        }));
        if (result.length > 0) {
            throw new Error('Expected no rows');
        }
    }
    async selectOneOrNone(query, params) {
        const result = await this.sequelize.query((0, queryExecutor_1.formatQuery)(query, params), this.prepareOptions({
            type: sequelize_1.QueryTypes.SELECT,
        }));
        if (result.length > 1) {
            throw new Error('Expected at most one row');
        }
        return result[0];
    }
    async selectOne(query, params) {
        const result = await this.sequelize.query((0, queryExecutor_1.formatQuery)(query, params), this.prepareOptions({
            type: sequelize_1.QueryTypes.SELECT,
        }));
        if (result.length !== 1) {
            throw new Error('Expected exactly one row');
        }
        return result[0];
    }
    async result(query, params) {
        const [, result] = await this.sequelize.query((0, queryExecutor_1.formatQuery)(query, params), this.prepareOptions({}));
        if (typeof result === 'number') {
            return result;
        }
        if (typeof result === 'object' && 'rowCount' in result) {
            return result.rowCount;
        }
        return result;
    }
    async tx(fn) {
        const transaction = await this.sequelize.transaction();
        try {
            const res = await fn(new TransactionalSequelizeQueryExecutor(this.sequelize, transaction));
            await transaction.commit();
            return res;
        }
        catch (err) {
            await transaction.rollback();
            throw err;
        }
    }
}
exports.SequelizeQueryExecutor = SequelizeQueryExecutor;
class TransactionalSequelizeQueryExecutor extends SequelizeQueryExecutor {
    constructor(sequelize, transaction) {
        super(sequelize);
        this.transaction = transaction;
    }
    prepareOptions(options) {
        return {
            ...super.prepareOptions(options),
            transaction: this.transaction,
        };
    }
}
exports.TransactionalSequelizeQueryExecutor = TransactionalSequelizeQueryExecutor;
function optionsQx(options) {
    const seq = options.database.sequelize;
    if (options.transaction) {
        return new TransactionalSequelizeQueryExecutor(seq, options.transaction);
    }
    return new SequelizeQueryExecutor(seq);
}
/**
 * Creates a QueryExecutor for fire-and-forget background work.
 * Always runs outside any transaction — safe to use after the caller's
 * request transaction has been committed.
 */
function optionsBgQx(options) {
    return new SequelizeQueryExecutor(options.database.sequelize, true);
}
//# sourceMappingURL=sequelizeQueryExecutor.js.map