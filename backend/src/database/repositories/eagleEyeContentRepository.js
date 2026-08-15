"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const lodash_1 = __importDefault(require("lodash"));
const sequelize_1 = require("sequelize");
const common_1 = require("@crowd/common");
const eagleEyeActionRepository_1 = __importDefault(require("./eagleEyeActionRepository"));
const queryParser_1 = __importDefault(require("./filters/queryParser"));
const sequelizeRepository_1 = __importDefault(require("./sequelizeRepository"));
class EagleEyeContentRepository {
    static async create(data, options) {
        const currentTenant = sequelizeRepository_1.default.getCurrentTenant(options);
        const record = await options.database.eagleEyeContent.create({
            ...lodash_1.default.pick(data, ['platform', 'post', 'url', 'postedAt']),
            tenantId: currentTenant.id,
        });
        if (data.actions) {
            for (const action of data.actions) {
                await eagleEyeActionRepository_1.default.createActionForContent(action, record.id, options);
            }
        }
        return this.findById(record.id, options);
    }
    static async update(id, data, options) {
        const currentUser = sequelizeRepository_1.default.getCurrentUser(options);
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const currentTenant = sequelizeRepository_1.default.getCurrentTenant(options);
        let record = await options.database.eagleEyeContent.findOne({
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
            ...lodash_1.default.pick(data, ['platform', 'post', 'postedAt', 'url']),
            updatedById: currentUser.id,
        }, {
            transaction,
        });
        return this.findById(record.id, options);
    }
    static async findById(id, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const include = [
            {
                model: options.database.eagleEyeAction,
                as: 'actions',
            },
        ];
        const currentTenant = sequelizeRepository_1.default.getCurrentTenant(options);
        const record = await options.database.eagleEyeContent.findOne({
            where: {
                id,
                tenantId: currentTenant.id,
            },
            include,
            transaction,
        });
        if (!record) {
            throw new common_1.Error404();
        }
        return this._populateRelations(record);
    }
    static async destroy(id, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const currentTenant = sequelizeRepository_1.default.getCurrentTenant(options);
        const record = await options.database.eagleEyeContent.findOne({
            where: {
                id,
                tenantId: currentTenant.id,
            },
            transaction,
        });
        if (record) {
            await record.destroy({
                transaction,
                force: true,
            });
        }
    }
    static async findAndCountAll({ advancedFilter = null, limit = 0, offset = 0, orderBy = '' }, options) {
        var _a;
        const actionsSequelizeInclude = {
            model: options.database.eagleEyeAction,
            as: 'actions',
            required: true,
            where: {},
            limit: null,
            offset: 0,
        };
        if (advancedFilter && advancedFilter.action) {
            const actionQueryParser = new queryParser_1.default({
                withSegments: false,
            }, options);
            const parsedActionQuery = actionQueryParser.parse({
                filter: advancedFilter.action,
                orderBy: 'timestamp_DESC',
                limit: 0,
                offset: 0,
            });
            actionsSequelizeInclude.where = (_a = parsedActionQuery.where) !== null && _a !== void 0 ? _a : {};
            delete advancedFilter.action;
        }
        const include = [actionsSequelizeInclude];
        const contentParser = new queryParser_1.default({
            withSegments: false,
        }, options);
        const parsed = contentParser.parse({
            filter: advancedFilter,
            orderBy: orderBy || ['postedAt_DESC'],
            limit,
            offset,
        });
        const hasActionFilter = Object.keys(actionsSequelizeInclude.where).length !== 0;
        let rows = await options.database.eagleEyeContent.findAll({
            include,
            ...(parsed.where ? { where: parsed.where } : {}),
            order: parsed.order,
            limit: hasActionFilter ? null : parsed.limit,
            offset: hasActionFilter ? 0 : parsed.offset,
            transaction: sequelizeRepository_1.default.getTransaction(options),
            subQuery: true,
            distinct: true,
        });
        // count query will group by content id and create a response with action counts
        // ie: it returns a payload similar to this
        // [ contentId1: #ofActionsForContent1, contentId2: #ofActionsForContent2 ]
        // To get the content count, we need to get the length of the response.
        const count = (await options.database.eagleEyeContent.count({
            include,
            ...(parsed.where ? { where: parsed.where } : {}),
            transaction: sequelizeRepository_1.default.getTransaction(options),
            distinct: true,
            group: ['eagleEyeContent.id'],
        })).length;
        // If we have an actions filter, we should query again to eager
        // load the all actions on a content because previous query will
        // omit actions that don't match the given action filter
        if (hasActionFilter) {
            rows = await options.database.eagleEyeContent.findAll({
                include: [
                    { ...actionsSequelizeInclude, where: {}, limit: null, offset: 0, required: true },
                ],
                where: { id: { [sequelize_1.Op.in]: rows.map((i) => i.id) } },
                order: parsed.order,
                transaction: sequelizeRepository_1.default.getTransaction(options),
                subQuery: true,
                limit: parsed.limit,
                offset: parsed.offset,
                distinct: true,
            });
        }
        rows = await this._populateRelationsForRows(rows);
        return { rows: rows !== null && rows !== void 0 ? rows : [], count, limit: parsed.limit, offset: parsed.offset };
    }
    static async findByUrl(url, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const include = [];
        const currentTenant = sequelizeRepository_1.default.getCurrentTenant(options);
        const record = await options.database.eagleEyeContent.findOne({
            where: {
                url,
                tenantId: currentTenant.id,
            },
            include,
            transaction,
        });
        if (!record) {
            return null;
        }
        return this._populateRelations(record);
    }
    static async _populateRelationsForRows(rows) {
        if (!rows) {
            return rows;
        }
        return Promise.all(rows.map((record) => this._populateRelations(record)));
    }
    static async _populateRelations(record) {
        if (!record) {
            return record;
        }
        return record.get({ plain: true });
    }
}
exports.default = EagleEyeContentRepository;
//# sourceMappingURL=eagleEyeContentRepository.js.map