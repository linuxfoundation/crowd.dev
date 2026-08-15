"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const lodash_1 = __importDefault(require("lodash"));
const sequelize_1 = __importStar(require("sequelize"));
const common_1 = require("@crowd/common");
const sequelizeFilterUtils_1 = __importDefault(require("../utils/sequelizeFilterUtils"));
const userTenantUtils_1 = require("../utils/userTenantUtils");
const sequelizeRepository_1 = __importDefault(require("./sequelizeRepository"));
const { Op } = sequelize_1.default;
const forbiddenTenantUrls = ['www'];
class TenantRepository {
    static async create(data, options) {
        const currentUser = sequelizeRepository_1.default.getCurrentUser(options);
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        // name is required
        if (!data.name) {
            throw new common_1.Error400(options.language, 'tenant.errors.nameRequiredOnCreate');
        }
        data.url = data.url || (await TenantRepository.generateTenantUrl(data.name, options));
        const existsUrl = Boolean(await options.database.tenant.count({
            where: { url: data.url },
            transaction,
        }));
        if (forbiddenTenantUrls.includes(data.url) || existsUrl) {
            throw new common_1.Error400(options.language, 'tenant.url.exists');
        }
        const record = await options.database.tenant.create({
            ...lodash_1.default.pick(data, [
                'id',
                'name',
                'url',
                'communitySize',
                'reasonForUsingCrowd',
                'integrationsRequired',
                'importHash',
            ]),
            createdById: currentUser.id,
            updatedById: currentUser.id,
        }, {
            transaction,
        });
        return this.findById(record.id, {
            ...options,
        });
    }
    /**
     * Generates hyphen concataned tenant url from the tenant name
     * If url already exists, appends a incremental number to the url
     * @param name tenant name
     * @param options repository options
     * @returns slug like tenant name to be used in tenant.url
     */
    static async generateTenantUrl(name, options) {
        const cleanedName = (0, common_1.getCleanString)(name);
        const nameWordsArray = cleanedName.split(' ');
        let cleanedTenantUrl = '';
        for (let i = 0; i < nameWordsArray.length; i++) {
            cleanedTenantUrl += `${nameWordsArray[i]}-`;
        }
        // remove trailing dash
        cleanedTenantUrl = cleanedTenantUrl.replace(/-$/gi, '');
        const filterUser = false;
        const checkTenantUrl = await TenantRepository.findAndCountAll({ filter: { url: cleanedTenantUrl } }, options, filterUser);
        if (checkTenantUrl.count > 0) {
            cleanedTenantUrl += `-${checkTenantUrl.count}`;
        }
        return cleanedTenantUrl;
    }
    static async update(id, data, options, force = false) {
        const currentUser = sequelizeRepository_1.default.getCurrentUser(options);
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        let record = await options.database.tenant.findByPk(id, {
            transaction,
        });
        if (!force && !(0, userTenantUtils_1.isUserInTenant)(currentUser, record)) {
            throw new common_1.Error404();
        }
        // When not multi-with-subdomain, the
        // from passes the URL as undefined.
        // This way it's ensured that the URL will
        // remain the old one
        data.url = data.url || record.url;
        const existsUrl = Boolean(await options.database.tenant.count({
            where: {
                url: data.url,
                id: { [Op.ne]: id },
            },
            transaction,
        }));
        if (forbiddenTenantUrls.includes(data.url) || existsUrl) {
            throw new common_1.Error400(options.language, 'tenant.url.exists');
        }
        record = await record.update({
            ...lodash_1.default.pick(data, [
                'id',
                'name',
                'url',
                'communitySize',
                'reasonForUsingCrowd',
                'integrationsRequired',
                'onboardedAt',
                'hasSampleData',
                'importHash',
                'plan',
                'isTrialPlan',
                'trialEndsAt',
            ]),
            updatedById: currentUser.id,
        }, {
            transaction,
        });
        return this.findById(record.id, options);
    }
    static async destroy(id, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const currentUser = sequelizeRepository_1.default.getCurrentUser(options);
        const record = await options.database.tenant.findByPk(id, {
            transaction,
        });
        if (!(0, userTenantUtils_1.isUserInTenant)(currentUser, record)) {
            throw new common_1.Error404();
        }
        await record.destroy({
            transaction,
        });
    }
    static async findById(id, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const include = ['settings'];
        const record = await options.database.tenant.findByPk(id, {
            include,
            transaction,
        });
        if (record && record.settings && record.settings[0] && record.settings[0].dataValues) {
            record.settings[0].dataValues.slackWebHook = !!record.settings[0].dataValues.slackWebHook;
        }
        return record;
    }
    static async findByUrl(url, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const include = ['settings'];
        const record = await options.database.tenant.findOne({
            where: { url },
            include,
            transaction,
        });
        if (record && record.settings && record.settings[0] && record.settings[0].dataValues) {
            record.settings[0].dataValues.slackWebHook = !!record.settings[0].dataValues.slackWebHook;
        }
        return record;
    }
    static async count(filter, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        return options.database.tenant.count({
            where: filter,
            transaction,
        });
    }
    static async findDefault(options) {
        return options.database.tenant.findOne({
            transaction: sequelizeRepository_1.default.getTransaction(options),
        });
    }
    /**
     * Finds and counts all tenants with given filter options
     * @param param0 object representation of filter, limit, offset and order
     * @param options IRepositoryOptions to filter out results by tenant
     * @param filterUser set to false if default user filter is not needed
     * @returns rows and total found count of found tenants
     */
    static async findAndCountAll({ filter, limit = 0, offset = 0, orderBy = '' }, options, filterUser = true) {
        const whereAnd = [];
        const include = [];
        if (filterUser) {
            const currentUser = sequelizeRepository_1.default.getCurrentUser(options);
            // Fetch only tenant that the current user has access
            whereAnd.push({
                id: {
                    [Op.in]: currentUser.tenants.map((tenantUser) => tenantUser.tenant.id),
                },
            });
        }
        if (filter) {
            if (filter.id) {
                whereAnd.push({
                    id: filter.id,
                });
            }
            if (filter.name) {
                whereAnd.push(sequelizeFilterUtils_1.default.ilikeIncludes('tenant', 'name', filter.name));
            }
            if (filter.url) {
                whereAnd.push(sequelizeFilterUtils_1.default.ilikeIncludes('tenant', 'url', filter.url));
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
        const { rows, count } = await options.database.tenant.findAndCountAll({
            where,
            include,
            limit: limit ? Number(limit) : undefined,
            offset: offset ? Number(offset) : undefined,
            order: orderBy ? [orderBy.split('_')] : [['name', 'ASC']],
            transaction: sequelizeRepository_1.default.getTransaction(options),
        });
        return { rows, count, limit: false, offset: 0 };
    }
    static async findAllAutocomplete(query, limit, options) {
        const whereAnd = [];
        const currentUser = sequelizeRepository_1.default.getCurrentUser(options);
        // Fetch only tenant that the current user has access
        whereAnd.push({
            id: {
                [Op.in]: currentUser.tenants.map((tenantUser) => tenantUser.tenant.id),
            },
        });
        if (query) {
            whereAnd.push({
                [Op.or]: [
                    { id: query.id },
                    {
                        [Op.and]: sequelizeFilterUtils_1.default.ilikeIncludes('tenant', 'name', query.name),
                    },
                ],
            });
        }
        const where = { [Op.and]: whereAnd };
        const records = await options.database.tenant.findAll({
            attributes: ['id', 'name'],
            where,
            limit: limit ? Number(limit) : undefined,
            order: [['name', 'ASC']],
        });
        return records.map((record) => ({
            id: record.id,
            label: record.name,
        }));
    }
    /**
     * Get current tenant
     * @param options Repository options
     * @returns Current tenant
     */
    static getCurrentTenant(options) {
        return sequelizeRepository_1.default.getCurrentTenant(options);
    }
    static async getAvailablePlatforms(options) {
        const query = `
      SELECT platform
      FROM "memberIdentities"
      WHERE "deletedAt" is null
      GROUP BY 1
    `;
        const parameters = {};
        const platforms = await options.database.sequelize.query(query, {
            replacements: parameters,
            type: sequelize_1.QueryTypes.SELECT,
        });
        return platforms;
    }
    static async getTenantInfo(id, options) {
        const query = `
        select name, plan, "isTrialPlan", "trialEndsAt" from tenants where "id" = :tenantId
    `;
        const parameters = {
            tenantId: id,
        };
        const info = await options.database.sequelize.query(query, {
            replacements: parameters,
            type: sequelize_1.QueryTypes.SELECT,
        });
        return info;
    }
}
exports.default = TenantRepository;
//# sourceMappingURL=tenantRepository.js.map