"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const lodash_1 = __importDefault(require("lodash"));
const sequelize_1 = __importDefault(require("sequelize"));
const common_1 = require("@crowd/common");
const sequelizeRepository_1 = __importDefault(require("./sequelizeRepository"));
const Op = sequelize_1.default.Op;
class CustomViewRepository {
    static async create(data, options) {
        const currentUser = sequelizeRepository_1.default.getCurrentUser(options);
        const tenant = sequelizeRepository_1.default.getCurrentTenant(options);
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const record = await options.database.customView.create({
            ...lodash_1.default.pick(data, ['name', 'visibility', 'config', 'placement']),
            tenantId: tenant.id,
            createdById: currentUser.id,
            updatedById: currentUser.id,
        }, {
            transaction,
        });
        await options.database.customViewOrder.create({
            userId: currentUser.id,
            customViewId: record.id,
        }, {
            transaction,
        });
        return this.findById(record.id, options);
    }
    static async update(id, data, options) {
        const currentUser = sequelizeRepository_1.default.getCurrentUser(options);
        const tenant = sequelizeRepository_1.default.getCurrentTenant(options);
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        let record = await options.database.customView.findOne({
            where: {
                id,
                tenantId: tenant.id,
            },
            transaction,
        });
        if (!record) {
            throw new common_1.Error404();
        }
        // don't allow other users private custom views to be updated
        if (record.visibility === 'user' && record.createdById !== currentUser.id) {
            throw new Error('Update not allowed as custom view was not created by user!');
        }
        // we don't allow placement to be updated
        record = await record.update({
            ...lodash_1.default.pick(data, ['name', 'visibility', 'config']),
            updatedById: currentUser.id,
        }, {
            transaction,
        });
        // upsert user's order for the custom view
        if (data.order) {
            await options.database.customViewOrder.upsert({
                userId: currentUser.id,
                customViewId: record.id,
                order: data.order,
            }, {
                transaction,
            });
        }
        return this.findById(record.id, options);
    }
    static async destroy(id, options, force = false) {
        const currentUser = sequelizeRepository_1.default.getCurrentUser(options);
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const currentTenant = sequelizeRepository_1.default.getCurrentTenant(options);
        const record = await options.database.customView.findOne({
            where: {
                id,
                tenantId: currentTenant.id,
            },
            transaction,
        });
        if (!record) {
            throw new common_1.Error404();
        }
        // don't allow other users private custom views to be deleted
        if (record.visibility === 'user' && record.createdById !== currentUser.id) {
            throw new Error('Deletion not allowed as custom view was not created by user!');
        }
        // update who deleted the custom view
        await record.update({
            updatedById: currentUser.id,
        }, { transaction });
        // soft delete the custom view
        await record.destroy({
            transaction,
            force,
        });
        // delete the order of the custom view
        await options.database.customViewOrder.destroy({
            where: {
                customViewId: record.id,
            },
            transaction,
        });
    }
    static async findById(id, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const currentTenant = sequelizeRepository_1.default.getCurrentTenant(options);
        const record = await options.database.customView.findOne({
            where: {
                id,
                tenantId: currentTenant.id,
            },
            transaction,
        });
        if (!record) {
            throw new common_1.Error404();
        }
        return record;
    }
    static async findAll(query, options) {
        const currentUser = sequelizeRepository_1.default.getCurrentUser(options);
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const tenant = sequelizeRepository_1.default.getCurrentTenant(options);
        const where = {
            ...lodash_1.default.pick(query, ['visibility']),
            [Op.or]: [
                {
                    visibility: 'tenant',
                    tenantId: tenant.id,
                },
                {
                    visibility: 'user',
                    createdById: currentUser.id,
                    tenantId: tenant.id,
                },
            ],
        };
        if (query === null || query === void 0 ? void 0 : query.placement) {
            where.placement = {
                [Op.in]: query.placement,
            };
        }
        let customViewRecords = await options.database.customView.findAll({
            where,
            order: [['createdAt', 'ASC']],
            transaction,
        });
        const customViewOrders = await options.database.customViewOrder.findAll({
            where: {
                userId: currentUser.id,
            },
            order: [['order', 'ASC']],
            transaction,
        });
        // sort custom views by user's order
        if (customViewOrders.length > 0) {
            const customViewOrderMap = new Map(customViewOrders.map((order) => [order.customViewId, order.order]));
            customViewRecords = lodash_1.default.orderBy(customViewRecords, (record) => customViewOrderMap.get(record.id) || Infinity);
        }
        return customViewRecords;
    }
}
exports.default = CustomViewRepository;
//# sourceMappingURL=customViewRepository.js.map