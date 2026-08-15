"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const sequelize_1 = __importDefault(require("sequelize"));
const common_1 = require("@crowd/common");
const redis_1 = require("@crowd/redis");
const sequelizeFilterUtils_1 = __importDefault(require("../utils/sequelizeFilterUtils"));
const sequelizeRepository_1 = __importDefault(require("./sequelizeRepository"));
const Op = sequelize_1.default.Op;
class MemberAttributeSettingsRepository {
    static async findById(id, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const currentTenant = sequelizeRepository_1.default.getCurrentTenant(options);
        const record = await options.database.memberAttributeSettings.findOne({
            where: {
                id,
                tenantId: currentTenant.id,
            },
            transaction,
        });
        if (!record) {
            throw new common_1.Error404();
        }
        return this._populateRelations(record);
    }
    static async _populateRelations(record) {
        if (!record) {
            return record;
        }
        const output = record.get({ plain: true });
        return output;
    }
    static async create(data, options) {
        const currentUser = sequelizeRepository_1.default.getCurrentUser(options);
        const tenant = sequelizeRepository_1.default.getCurrentTenant(options);
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        if (Object.keys(options.database.member.rawAttributes).includes(data.name)) {
            throw new common_1.Error400(options.language, 'settings.memberAttributes.errors.reservedField', data.name);
        }
        const record = await options.database.memberAttributeSettings.create({
            type: data.type,
            name: data.name,
            label: data.label,
            canDelete: data.canDelete,
            show: data.show,
            options: data.options,
            tenantId: tenant.id,
            createdById: currentUser.id,
            updatedById: currentUser.id,
        }, {
            transaction,
        });
        const cache = new redis_1.RedisCache('memberAttributes', options.redis, options.log);
        await cache.delete(tenant.id);
        return this.findById(record.id, options);
    }
    static async update(id, data, options) {
        const currentUser = sequelizeRepository_1.default.getCurrentUser(options);
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const currentTenant = sequelizeRepository_1.default.getCurrentTenant(options);
        let record = await options.database.memberAttributeSettings.findOne({
            where: {
                id,
                tenantId: currentTenant.id,
            },
            transaction,
        });
        if (!record) {
            throw new common_1.Error404();
        }
        record = await record.update({
            ...data,
            updatedById: currentUser.id,
        }, {
            transaction,
        });
        const cache = new redis_1.RedisCache('memberAttributes', options.redis, options.log);
        await cache.delete(currentTenant.id);
        return this.findById(record.id, options);
    }
    static async destroy(id, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const currentTenant = sequelizeRepository_1.default.getCurrentTenant(options);
        const record = await options.database.memberAttributeSettings.findOne({
            where: {
                id,
                tenantId: currentTenant.id,
            },
            transaction,
        });
        if (!record) {
            throw new common_1.Error404();
        }
        if (record.canDelete) {
            await record.destroy({
                transaction,
            });
            const cache = new redis_1.RedisCache('memberAttributes', options.redis, options.log);
            await cache.delete(currentTenant.id);
        }
    }
    static async findAndCountAll({ filter, limit = 0, offset = 0, orderBy = '' }, options) {
        const tenant = sequelizeRepository_1.default.getCurrentTenant(options);
        const whereAnd = [];
        whereAnd.push({
            tenantId: tenant.id,
        });
        if (filter) {
            if (filter.id) {
                whereAnd.push({
                    id: sequelizeFilterUtils_1.default.uuid(filter.id),
                });
            }
            if (filter.canDelete === true || filter.canDelete === false) {
                whereAnd.push({
                    canDelete: filter.canDelete === true,
                });
            }
            if (filter.show === true || filter.show === false) {
                whereAnd.push({
                    show: filter.show === true,
                });
            }
            if (filter.type) {
                whereAnd.push({
                    type: filter.type,
                });
            }
            if (filter.label) {
                whereAnd.push({
                    label: filter.label,
                });
            }
            if (filter.name) {
                whereAnd.push({
                    name: filter.name,
                });
            }
            if (filter.createdAtRange) {
                const [start, end] = filter.createdAtRange;
                if (start !== undefined && start !== null && start !== '') {
                    whereAnd.push({
                        createdAt: {
                            [Op.gte]: start,
                        },
                    });
                }
                if (end !== undefined && end !== null && end !== '') {
                    whereAnd.push({
                        createdAt: {
                            [Op.lte]: end,
                        },
                    });
                }
            }
        }
        const where = { [Op.and]: whereAnd };
        // eslint-disable-next-line prefer-const
        let { rows, count } = await options.database.memberAttributeSettings.findAndCountAll({
            where,
            limit: limit ? Number(limit) : undefined,
            offset: offset ? Number(offset) : undefined,
            order: orderBy ? [orderBy.split('_')] : [['createdAt', 'DESC']],
            transaction: sequelizeRepository_1.default.getTransaction(options),
        });
        rows = await this._populateRelationsForRows(rows);
        // TODO add limit and offset
        return { rows, count };
    }
    static async _populateRelationsForRows(rows) {
        if (!rows) {
            return rows;
        }
        return Promise.all(rows.map((record) => this._populateRelations(record)));
    }
}
exports.default = MemberAttributeSettingsRepository;
//# sourceMappingURL=memberAttributeSettingsRepository.js.map