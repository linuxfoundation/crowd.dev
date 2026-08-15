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
const audit_logs_1 = require("@crowd/audit-logs");
const common_1 = require("@crowd/common");
const integrations_1 = require("@crowd/data-access-layer/src/integrations");
const repositories_1 = require("@crowd/data-access-layer/src/repositories");
const segments_1 = require("@crowd/data-access-layer/src/segments");
const types_1 = require("@crowd/types");
const sequelizeFilterUtils_1 = __importDefault(require("../utils/sequelizeFilterUtils"));
const queryParser_1 = __importDefault(require("./filters/queryParser"));
const sequelizeRepository_1 = __importDefault(require("./sequelizeRepository"));
const { Op } = sequelize_1.default;
class IntegrationRepository {
    static async create(data, options) {
        const currentUser = sequelizeRepository_1.default.getCurrentUser(options);
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const segment = sequelizeRepository_1.default.getStrictlySingleActiveSegment(options);
        const toInsert = {
            ...lodash_1.default.pick(data, [
                'platform',
                'status',
                'token',
                'refreshToken',
                'settings',
                'integrationIdentifier',
            ]),
            segmentId: segment.id,
            tenantId: common_1.DEFAULT_TENANT_ID,
            createdById: currentUser.id,
            updatedById: currentUser.id,
            id: data.id || undefined,
        };
        const record = await options.database.integration.create(toInsert, {
            transaction,
        });
        await (0, audit_logs_1.captureApiChange)(options, (0, audit_logs_1.integrationConnectAction)(record.id, async (captureState) => {
            captureState(toInsert);
        }));
        return this.findById(record.id, options);
    }
    static async update(id, data, options) {
        const currentUser = sequelizeRepository_1.default.getCurrentUser(options);
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const qx = sequelizeRepository_1.default.getQueryExecutor(options);
        const currentSegments = sequelizeRepository_1.default.getSegmentIds(options);
        const subprojectIds = await (0, segments_1.getSegmentSubprojectIds)(qx, currentSegments);
        let record = await options.database.integration.findOne({
            where: {
                id,
                segmentId: subprojectIds,
            },
            transaction,
        });
        if (!record) {
            throw new common_1.Error404();
        }
        record = await record.update({
            ...lodash_1.default.pick(data, [
                'platform',
                'status',
                'token',
                'refreshToken',
                'settings',
                'integrationIdentifier',
            ]),
            updatedById: currentUser.id,
        }, {
            transaction,
        });
        return this.findById(record.id, options);
    }
    static async destroy(id, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const record = await options.database.integration.findOne({
            where: {
                id,
            },
            transaction,
        });
        if (!record) {
            throw new common_1.Error404();
        }
        await record.destroy({
            transaction,
        });
        // also mark integration runs as deleted
        const seq = sequelizeRepository_1.default.getSequelize(options);
        await seq.query(`update integration.runs set state = :newState
     where "integrationId" = :integrationId and state in (:delayed, :pending, :processing)`, {
            replacements: {
                newState: types_1.IntegrationRunState.INTEGRATION_DELETED,
                delayed: types_1.IntegrationRunState.DELAYED,
                pending: types_1.IntegrationRunState.PENDING,
                processing: types_1.IntegrationRunState.PROCESSING,
                integrationId: id,
            },
            transaction,
        });
    }
    static async findAllByPlatform(platform, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const include = [];
        const records = await options.database.integration.findAll({
            where: {
                platform,
            },
            include,
            transaction,
        });
        return records.map((record) => record.get({ plain: true }));
    }
    static async findByPlatform(platform, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const segment = sequelizeRepository_1.default.getStrictlySingleActiveSegment(options);
        const include = [];
        const record = await options.database.integration.findOne({
            where: {
                platform,
                segmentId: segment.id,
            },
            include,
            transaction,
        });
        if (!record) {
            throw new common_1.Error404();
        }
        return this._populateRelations(record, sequelizeRepository_1.default.getQueryExecutor(options));
    }
    static async findActiveIntegrationByPlatform(platform) {
        const options = await sequelizeRepository_1.default.getDefaultIRepositoryOptions();
        const record = await options.database.integration.findOne({
            where: {
                platform,
            },
        });
        if (!record) {
            throw new common_1.Error404();
        }
        return this._populateRelations(record, sequelizeRepository_1.default.getQueryExecutor(options));
    }
    /**
     * Find all active integrations for a platform
     * @param platform The platform we want to find all active integrations for
     * @returns All active integrations for the platform
     */
    static async findAllActive(platform, page, perPage) {
        const options = await sequelizeRepository_1.default.getDefaultIRepositoryOptions();
        const records = await options.database.integration.findAll({
            where: {
                status: 'done',
                platform,
            },
            limit: perPage,
            offset: (page - 1) * perPage,
            order: [['id', 'ASC']],
        });
        if (!records) {
            throw new common_1.Error404();
        }
        return this._populateRelationsForRows(records, sequelizeRepository_1.default.getQueryExecutor(options));
    }
    static async findByStatus(status, page, perPage, options) {
        const query = `
      select * from integrations where status = :status
      limit ${perPage} offset ${(page - 1) * perPage}
    `;
        const seq = sequelizeRepository_1.default.getSequelize(options);
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const integrations = await seq.query(query, {
            replacements: {
                status,
            },
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        });
        return integrations;
    }
    /**
     * Find an integration using the integration identifier and a platform.
     * @param identifier The integration identifier
     * @returns The integration object
     */
    // TODO: Test
    static async findByIdentifier(identifier, platform) {
        const options = await sequelizeRepository_1.default.getDefaultIRepositoryOptions();
        const record = await options.database.integration.findOne({
            where: {
                integrationIdentifier: identifier,
                platform,
                deletedAt: null,
            },
        });
        if (!record) {
            throw new common_1.Error404();
        }
        return this._populateRelations(record, sequelizeRepository_1.default.getQueryExecutor(options));
    }
    static async findById(id, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const include = [];
        const record = await options.database.integration.findOne({
            where: {
                id,
            },
            include,
            transaction,
        });
        if (!record) {
            throw new common_1.Error404();
        }
        return this._populateRelations(record, sequelizeRepository_1.default.getQueryExecutor(options));
    }
    static async count(filter, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        return options.database.integration.count({
            where: {
                ...filter,
            },
            transaction,
        });
    }
    /**
     * Finds global integrations based on the provided parameters.
     *
     * @param {Object} filters - An object containing various filter options.
     * @param {string} [filters.platform=null] - The platform to filter integrations by.
     * @param {string | string[]} [filters.status=['done']] - The status of the integrations to be filtered. Can be a single status or array of statuses.
     * @param {string} [filters.query=''] - The search query to filter integrations.
     * @param {number} [filters.limit=20] - The maximum number of integrations to return.
     * @param {number} [filters.offset=0] - The offset for pagination.
     * @param {string} [filters.segment=null] - The segment to filter integrations by.
     * @param {IRepositoryOptions} options - The repository options for querying.
     * @returns {Promise<Object>} The result containing the rows of integrations and metadata about the query.
     */
    static async findGlobalIntegrations(filters, options) {
        const { platform = null, status = ['done'], query = '', limit = 20, offset = 0, segment = null, } = filters;
        const qx = sequelizeRepository_1.default.getQueryExecutor(options);
        const statusArray = Array.isArray(status) ? status : [status];
        const isNotConnectedQuery = statusArray.includes('not-connected');
        // Execute data fetch and count in parallel for better performance
        const [rows, [countObj]] = await Promise.all([
            isNotConnectedQuery
                ? (0, integrations_1.fetchGlobalNotConnectedIntegrations)(qx, platform, query, limit, offset, segment)
                : (0, integrations_1.fetchGlobalIntegrations)(qx, statusArray, platform, query, limit, offset, segment),
            isNotConnectedQuery
                ? (0, integrations_1.fetchGlobalNotConnectedIntegrationsCount)(qx, platform, query, segment)
                : (0, integrations_1.fetchGlobalIntegrationsCount)(qx, statusArray, platform, query, segment),
        ]);
        // Both functions return an array with count objects, so we take the first element
        const count = countObj === null || countObj === void 0 ? void 0 : countObj.count;
        return {
            rows,
            count: +count || 0,
            limit: +limit,
            offset: +offset,
        };
    }
    /**
     * Retrieves the count of global integrations statuses for a specified platform.
     * This method aggregates the count of different integration statuses including a 'not-connected' status.
     *
     * @param {Object} param1 - The optional parameters.
     * @param {string|null} [param1.platform=null] - The platform to filter the integrations. Default is null.
     * @param {string|null} [param1.segment=null] - The segment to filter the integrations. Default is null.
     * @param {IRepositoryOptions} options - The options for the repository operations.
     * @return {Promise<Array<Object>>} A promise that resolves to an array of objects containing the statuses and their counts.
     */
    static async findGlobalIntegrationsStatusCount(filters, options) {
        const { platform = null, segment = null } = filters;
        const qx = sequelizeRepository_1.default.getQueryExecutor(options);
        // Execute both queries in parallel for better performance
        const [statusCounts, [notConnectedResult]] = await Promise.all([
            (0, integrations_1.fetchGlobalIntegrationsStatusCount)(qx, platform, segment),
            (0, integrations_1.fetchGlobalNotConnectedIntegrationsCount)(qx, platform, '', segment),
        ]);
        return [
            ...statusCounts,
            {
                status: 'not-connected',
                count: Number(notConnectedResult === null || notConnectedResult === void 0 ? void 0 : notConnectedResult.count) || 0,
            },
        ];
    }
    static async findAndCountAll({ filter = {}, advancedFilter = null, limit = 0, offset = 0, orderBy = '' }, options) {
        const include = [];
        // If the advanced filter is empty, we construct it from the query parameter filter
        if (!advancedFilter) {
            advancedFilter = { and: [] };
            if (filter.id) {
                advancedFilter.and.push({
                    id: filter.id,
                });
            }
            if (filter.platform) {
                advancedFilter.and.push({
                    platform: filter.platform,
                });
            }
            if (filter.status) {
                advancedFilter.and.push({
                    status: filter.status,
                });
            }
            if (filter.integrationIdentifier) {
                advancedFilter.and.push({
                    integrationIdentifier: filter.integrationIdentifier,
                });
            }
            if (filter.createdAtRange) {
                const [start, end] = filter.createdAtRange;
                if (start !== undefined && start !== null && start !== '') {
                    advancedFilter.and.push({
                        createdAt: {
                            gte: start,
                        },
                    });
                }
                if (end !== undefined && end !== null && end !== '') {
                    advancedFilter.and.push({
                        createdAt: {
                            lte: end,
                        },
                    });
                }
            }
        }
        const parser = new queryParser_1.default({
            nestedFields: {
                sentiment: 'sentiment.sentiment',
            },
            // QueryParser filters on req.currentSegments directly (e.g., projectGroupId).
            // Since integrations are stored per subproject, segment filtering is applied manually below
            // after expanding to subprojectIds.
            withSegments: false,
        }, options);
        const parsed = parser.parse({
            filter: advancedFilter,
            orderBy: orderBy || ['createdAt_DESC'],
            limit,
            offset,
        });
        const qx = sequelizeRepository_1.default.getQueryExecutor(options);
        const currentSegments = sequelizeRepository_1.default.getSegmentIds(options);
        const subprojectIds = await (0, segments_1.getSegmentSubprojectIds)(qx, currentSegments);
        const segmentWhere = { segmentId: subprojectIds };
        const where = parsed.where ? { [Op.and]: [parsed.where, segmentWhere] } : segmentWhere;
        let { rows, count, // eslint-disable-line prefer-const
         } = await options.database.integration.findAndCountAll({
            where,
            ...(parsed.having ? { having: parsed.having } : {}),
            order: parsed.order,
            limit: limit ? parsed.limit : undefined,
            offset: offset ? parsed.offset : undefined,
            include,
            transaction: sequelizeRepository_1.default.getTransaction(options),
        });
        rows = await this._populateRelationsForRows(rows, sequelizeRepository_1.default.getQueryExecutor(options));
        // Some integrations (i.e GitHub, Discord, Discourse, Groupsio) receive new data via webhook post-onboarding.
        // We track their last processedAt separately, and not using updatedAt.
        const seq = sequelizeRepository_1.default.getSequelize(options);
        const integrationIds = rows.map((row) => row.id);
        if (integrationIds.length > 0) {
            const webhookQuery = `
        SELECT "integrationId", MAX("processedAt") AS "webhookProcessedAt"
        FROM "incomingWebhooks"
        WHERE "integrationId" IN (:integrationIds) AND state = 'PROCESSED'
        GROUP BY "integrationId"
      `;
            const runQuery = `
        SELECT "integrationId", MAX("processedAt") AS "runProcessedAt"
        FROM integration.runs
        WHERE "integrationId" IN (:integrationIds)
        GROUP BY "integrationId"
      `;
            const [webhookResults, runResults] = await Promise.all([
                seq.query(webhookQuery, {
                    replacements: { integrationIds },
                    type: sequelize_1.QueryTypes.SELECT,
                    transaction: sequelizeRepository_1.default.getTransaction(options),
                }),
                seq.query(runQuery, {
                    replacements: { integrationIds },
                    type: sequelize_1.QueryTypes.SELECT,
                    transaction: sequelizeRepository_1.default.getTransaction(options),
                }),
            ]);
            const processedAtMap = integrationIds.reduce((map, id) => {
                const webhookResult = webhookResults.find((r) => r.integrationId === id);
                const runResult = runResults.find((r) => r.integrationId === id);
                map[id] = {
                    webhookProcessedAt: webhookResult ? webhookResult.webhookProcessedAt : null,
                    runProcessedAt: runResult ? runResult.runProcessedAt : null,
                };
                return map;
            }, {});
            rows.forEach((row) => {
                const processedAt = processedAtMap[row.id];
                // Use the latest processedAt from either webhook or run, or fall back to updatedAt
                row.lastProcessedAt = processedAt
                    ? new Date(Math.max(processedAt.webhookProcessedAt
                        ? new Date(processedAt.webhookProcessedAt).getTime()
                        : 0, processedAt.runProcessedAt ? new Date(processedAt.runProcessedAt).getTime() : 0, new Date(row.updatedAt).getTime()))
                    : row.updatedAt;
            });
        }
        return { rows, count, limit: parsed.limit, offset: parsed.offset };
    }
    static async findAllAutocomplete(query, limit, options) {
        const whereAnd = [{}];
        if (query) {
            whereAnd.push({
                [Op.or]: [
                    { id: sequelizeFilterUtils_1.default.uuid(query) },
                    {
                        [Op.and]: sequelizeFilterUtils_1.default.ilikeIncludes('integration', 'platform', query),
                    },
                ],
            });
        }
        const where = { [Op.and]: whereAnd };
        const records = await options.database.integration.findAll({
            attributes: ['id', 'platform'],
            where,
            limit: limit ? Number(limit) : undefined,
            order: [['platform', 'ASC']],
        });
        return records.map((record) => ({
            id: record.id,
            label: record.platform,
        }));
    }
    static async _populateRelationsForRows(rows, qx) {
        if (!rows) {
            return rows;
        }
        const records = rows.map((record) => record.get({ plain: true }));
        const nangoIntegrationIds = records
            .filter((r) => r.platform === types_1.PlatformType.GITHUB_NANGO)
            .map((r) => r.id);
        const githubIntegrationIds = records
            .filter((r) => {
            var _a, _b;
            return (r.platform === types_1.PlatformType.GITHUB || r.platform === types_1.PlatformType.GITHUB_NANGO) &&
                ((_b = (_a = r.settings) === null || _a === void 0 ? void 0 : _a.orgs) === null || _b === void 0 ? void 0 : _b.length) > 0;
        })
            .map((r) => r.id);
        const [allNangoMappings, allReposByOrg] = await Promise.all([
            (0, integrations_1.getNangoMappingsForIntegrations)(qx, nangoIntegrationIds),
            (0, repositories_1.getReposGroupedByOrgForIntegrations)(qx, githubIntegrationIds),
        ]);
        return records.map((output) => {
            var _a, _b;
            if (output.platform === types_1.PlatformType.GITHUB_NANGO) {
                const nangoMapping = allNangoMappings[output.id];
                if (nangoMapping && Object.keys(nangoMapping).length > 0) {
                    output.settings = { ...output.settings, nangoMapping };
                }
            }
            if ((output.platform === types_1.PlatformType.GITHUB ||
                output.platform === types_1.PlatformType.GITHUB_NANGO) &&
                ((_b = (_a = output.settings) === null || _a === void 0 ? void 0 : _a.orgs) === null || _b === void 0 ? void 0 : _b.length) > 0) {
                const reposByOrg = allReposByOrg[output.id];
                if (reposByOrg && Object.keys(reposByOrg).length > 0) {
                    output.settings = {
                        ...output.settings,
                        orgs: output.settings.orgs.map((org) => ({
                            ...org,
                            repos: (reposByOrg[org.name] || []).map((r) => ({
                                url: r.url,
                                name: r.name,
                                owner: r.owner,
                                forkedFrom: r.forkedFrom,
                                updatedAt: r.updatedAt,
                            })),
                        })),
                    };
                }
                delete output.settings.repos;
                delete output.settings.unavailableRepos;
            }
            return output;
        });
    }
    static async _populateRelations(record, qx) {
        var _a, _b;
        if (!record) {
            return record;
        }
        const output = record.get({ plain: true });
        // For github-nango integrations, populate settings.nangoMapping from dedicated table
        if (output.platform === types_1.PlatformType.GITHUB_NANGO) {
            const allNangoMappings = await (0, integrations_1.getNangoMappingsForIntegrations)(qx, [output.id]);
            const nangoMapping = allNangoMappings[output.id] || {};
            if (Object.keys(nangoMapping).length > 0) {
                output.settings = { ...output.settings, nangoMapping };
            }
        }
        // For both github and github-nango, populate orgs[].repos from repositories table
        if ((output.platform === types_1.PlatformType.GITHUB || output.platform === types_1.PlatformType.GITHUB_NANGO) &&
            ((_b = (_a = output.settings) === null || _a === void 0 ? void 0 : _a.orgs) === null || _b === void 0 ? void 0 : _b.length) > 0) {
            const allReposByOrg = await (0, repositories_1.getReposGroupedByOrgForIntegrations)(qx, [output.id]);
            const reposByOrg = allReposByOrg[output.id] || {};
            // Only overwrite orgs[].repos from the repositories table if there are rows.
            // During the 'mapping' phase (legacy github connect), repos live in settings
            // before being written to the repositories table via mapGithubRepos.
            if (Object.keys(reposByOrg).length > 0) {
                output.settings = {
                    ...output.settings,
                    orgs: output.settings.orgs.map((org) => ({
                        ...org,
                        repos: (reposByOrg[org.name] || []).map((r) => ({
                            url: r.url,
                            name: r.name,
                            owner: r.owner,
                            forkedFrom: r.forkedFrom,
                            updatedAt: r.updatedAt,
                        })),
                    })),
                };
            }
            // Strip legacy top-level keys that may still exist in the DB column
            delete output.settings.repos;
            delete output.settings.unavailableRepos;
        }
        return output;
    }
}
exports.default = IntegrationRepository;
//# sourceMappingURL=integrationRepository.js.map