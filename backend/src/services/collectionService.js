"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.CollectionService = void 0;
const lodash_1 = require("lodash");
const common_1 = require("@crowd/common");
const common_services_1 = require("@crowd/common_services");
const data_access_layer_1 = require("@crowd/data-access-layer");
const categories_1 = require("@crowd/data-access-layer/src/categories");
const collections_1 = require("@crowd/data-access-layer/src/collections");
const integrations_1 = require("@crowd/data-access-layer/src/integrations");
const repositories_1 = require("@crowd/data-access-layer/src/repositories");
const repositoryGroups_1 = require("@crowd/data-access-layer/src/repositoryGroups");
const segments_1 = require("@crowd/data-access-layer/src/segments");
const logging_1 = require("@crowd/logging");
const types_1 = require("@crowd/types");
const conf_1 = require("@/conf");
const sequelizeRepository_1 = __importDefault(require("@/database/repositories/sequelizeRepository"));
class CollectionService extends logging_1.LoggerBase {
    constructor(options) {
        super();
        this.options = options;
    }
    async createCollection(collection) {
        return sequelizeRepository_1.default.withTx(this.options, async (tx) => {
            var _a;
            const qx = sequelizeRepository_1.default.getQueryExecutor({ ...this.options, transaction: tx });
            const slug = (_a = collection.slug) !== null && _a !== void 0 ? _a : (0, common_1.getCleanString)(collection.name).replace(/\s+/g, '-');
            const createdCollection = await (0, collections_1.createCollection)(qx, {
                ...collection,
                slug,
            });
            if (collection.projects) {
                await (0, collections_1.connectProjectsAndCollections)(qx, collection.projects.map((p) => ({
                    insightsProjectId: p.id,
                    collectionId: createdCollection.id,
                    starred: p.starred,
                })));
            }
            const txSvc = new CollectionService({
                ...this.options,
                transaction: tx,
            });
            return txSvc.findById(createdCollection.id);
        });
    }
    async updateCollection(id, collection) {
        return sequelizeRepository_1.default.withTx(this.options, async (tx) => {
            const qx = sequelizeRepository_1.default.getQueryExecutor({ ...this.options, transaction: tx });
            await (0, collections_1.updateCollection)(qx, id, collection);
            if (collection.projects) {
                await (0, collections_1.disconnectProjectsAndCollections)(qx, { collectionId: id });
                await (0, collections_1.connectProjectsAndCollections)(qx, collection.projects.map((p) => ({
                    insightsProjectId: p.id,
                    collectionId: id,
                    starred: p.starred,
                })));
            }
            const txSvc = new CollectionService({
                ...this.options,
                transaction: tx,
            });
            return txSvc.findById(id);
        });
    }
    async findById(id) {
        return sequelizeRepository_1.default.withTx(this.options, async (tx) => {
            const qx = sequelizeRepository_1.default.getQueryExecutor({ ...this.options, transaction: tx });
            const collection = await (0, collections_1.queryCollectionById)(qx, id, Object.values(collections_1.CollectionField));
            const connections = await (0, collections_1.findCollectionProjectConnections)(qx, {
                collectionIds: [id],
            });
            const projects = connections.length
                ? await (0, collections_1.queryInsightsProjects)(qx, {
                    filter: {
                        id: {
                            in: connections.map((c) => c.insightsProjectId),
                        },
                    },
                    fields: Object.values(collections_1.InsightsProjectField),
                })
                : [];
            return {
                ...collection,
                projects: projects.map((p) => {
                    const connection = connections.find((c) => c.insightsProjectId === p.id);
                    if (!connection) {
                        throw new Error(`Connection not found for project ${p.id}`);
                    }
                    return {
                        ...p,
                        connectionId: connection.id,
                        starred: connection.starred,
                    };
                }),
            };
        });
    }
    async destroy(id) {
        await sequelizeRepository_1.default.withTx(this.options, async (tx) => {
            const qx = sequelizeRepository_1.default.getQueryExecutor({ ...this.options, transaction: tx });
            await (0, collections_1.disconnectProjectsAndCollections)(qx, { collectionId: id });
            await (0, collections_1.deleteCollection)(qx, id);
        });
    }
    async query({ limit, offset, filter }) {
        if (!limit)
            limit = 10;
        if (!offset)
            offset = 0;
        const qx = sequelizeRepository_1.default.getQueryExecutor(this.options);
        const collections = await (0, collections_1.queryCollections)(qx, {
            limit,
            offset,
            fields: Object.values(collections_1.CollectionField),
            filter,
            orderBy: '"name" ASC',
        });
        if (collections.length === 0) {
            return {
                rows: [],
                total: 0,
                limit,
                offset,
            };
        }
        const connections = await (0, collections_1.findCollectionProjectConnections)(qx, {
            collectionIds: (0, lodash_1.uniq)(collections.map((c) => c.id)),
        });
        const projects = connections.length > 0
            ? await (0, collections_1.queryInsightsProjects)(qx, {
                filter: {
                    id: { in: (0, lodash_1.uniq)(connections.map((c) => c.insightsProjectId)) },
                },
                fields: Object.values(collections_1.InsightsProjectField),
            })
            : [];
        const total = await (0, collections_1.countCollections)(qx, filter);
        const categoryIds = (0, lodash_1.uniq)(collections.map((c) => c.categoryId));
        const categories = await (0, categories_1.listCategoriesByIds)(qx, categoryIds);
        const categoryById = Object.fromEntries(categories.map((c) => [c.id, c]));
        return {
            rows: collections.map((c) => {
                const collectionConnections = connections.filter((cp) => cp.collectionId === c.id);
                return {
                    ...c,
                    category: categoryById[c.categoryId],
                    projects: projects
                        .filter((p) => collectionConnections.some((cp) => cp.insightsProjectId === p.id))
                        .map((p) => {
                        const connection = collectionConnections.find((cp) => cp.insightsProjectId === p.id);
                        return {
                            ...p,
                            connectionId: connection === null || connection === void 0 ? void 0 : connection.id,
                            starred: connection === null || connection === void 0 ? void 0 : connection.starred,
                        };
                    }),
                };
            }),
            total,
            limit,
            offset,
        };
    }
    async createInsightsProject(project) {
        return sequelizeRepository_1.default.withTx(this.options, async (tx) => {
            var _a, _b;
            const qx = sequelizeRepository_1.default.getQueryExecutor({ ...this.options, transaction: tx });
            const slug = (_a = project.slug) !== null && _a !== void 0 ? _a : (0, common_1.getCleanString)(project.name).replace(/\s+/g, '-');
            const segment = project.segmentId ? await (0, segments_1.findSegmentById)(qx, project.segmentId) : null;
            const isLF = (_b = segment === null || segment === void 0 ? void 0 : segment.isLF) !== null && _b !== void 0 ? _b : false;
            const createdProject = await (0, collections_1.createInsightsProject)(qx, {
                ...project,
                isLF,
                slug,
            });
            // Automatically manage Linux Foundation collection connection (linked to task: automatically add/update to collection)
            const managedCollections = await this.manageLfCollectionConnection(qx, createdProject.id, isLF, project.collections || []);
            if (managedCollections.length > 0) {
                await (0, collections_1.connectProjectsAndCollections)(qx, managedCollections.map((c) => {
                    var _a;
                    return ({
                        insightsProjectId: createdProject.id,
                        collectionId: c,
                        starred: (_a = project.starred) !== null && _a !== void 0 ? _a : true,
                    });
                }));
            }
            await this.syncRepositoryGroupsWithDb(qx, createdProject.id, project.repositoryGroups);
            const txSvc = new CollectionService({ ...this.options, transaction: tx });
            return txSvc.findInsightsProjectById(createdProject.id);
        });
    }
    async destroyInsightsProject(id) {
        await sequelizeRepository_1.default.withTx(this.options, async (tx) => {
            const qx = sequelizeRepository_1.default.getQueryExecutor({ ...this.options, transaction: tx });
            await (0, collections_1.disconnectProjectsAndCollections)(qx, { insightsProjectId: id });
            await (0, collections_1.deleteInsightsProject)(qx, id);
        });
    }
    async findInsightsProjectById(id) {
        return sequelizeRepository_1.default.withTx(this.options, async (tx) => {
            const qx = sequelizeRepository_1.default.getQueryExecutor({ ...this.options, transaction: tx });
            const project = await (0, collections_1.queryInsightsProjectById)(qx, id, Object.values(collections_1.InsightsProjectField));
            const connections = await (0, collections_1.findCollectionProjectConnections)(qx, {
                insightsProjectIds: [id],
            });
            const segment = project.segmentId ? await (0, segments_1.findSegmentById)(qx, project.segmentId) : null;
            const organization = project.organizationId
                ? await (0, data_access_layer_1.findOrgById)(qx, project.organizationId, [
                    data_access_layer_1.OrganizationField.ID,
                    data_access_layer_1.OrganizationField.DISPLAY_NAME,
                    data_access_layer_1.OrganizationField.LOGO,
                ])
                : null;
            const collections = connections.length > 0
                ? await (0, collections_1.queryCollections)(qx, {
                    filter: {
                        id: { in: (0, lodash_1.uniq)(connections.map((c) => c.collectionId)) },
                    },
                    fields: Object.values(collections_1.CollectionField),
                })
                : [];
            const repositoryGroups = await (0, repositoryGroups_1.listRepositoryGroups)(qx, { insightsProjectId: id });
            return {
                ...project,
                collections,
                segment: {
                    id: segment === null || segment === void 0 ? void 0 : segment.id,
                    name: segment === null || segment === void 0 ? void 0 : segment.name,
                    slug: segment === null || segment === void 0 ? void 0 : segment.slug,
                    logo: segment === null || segment === void 0 ? void 0 : segment.url,
                },
                organization: {
                    id: organization === null || organization === void 0 ? void 0 : organization.id,
                    displayName: organization === null || organization === void 0 ? void 0 : organization.displayName,
                    logo: organization === null || organization === void 0 ? void 0 : organization.logo,
                },
                repositoryGroups,
            };
        });
    }
    async queryInsightsProjects({ limit, offset, filter, }) {
        const qx = sequelizeRepository_1.default.getQueryExecutor(this.options);
        const projects = await (0, collections_1.queryInsightsProjects)(qx, {
            filter,
            limit,
            offset,
            fields: Object.values(collections_1.InsightsProjectField),
            orderBy: '"name" ASC',
        });
        if (projects.length === 0) {
            return {
                rows: [],
                total: 0,
                limit,
                offset,
            };
        }
        const connections = await (0, collections_1.findCollectionProjectConnections)(qx, {
            insightsProjectIds: projects.map((p) => p.id),
        });
        const organizations = await (0, data_access_layer_1.queryOrgs)(qx, {
            filter: {
                id: { in: (0, lodash_1.uniq)(projects.map((p) => p.organizationId)) },
            },
            fields: [data_access_layer_1.OrganizationField.ID, data_access_layer_1.OrganizationField.DISPLAY_NAME, data_access_layer_1.OrganizationField.LOGO],
        });
        const collections = connections.length > 0
            ? await (0, collections_1.queryCollections)(qx, {
                filter: {
                    id: { in: (0, lodash_1.uniq)(connections.map((c) => c.collectionId)) },
                },
                fields: Object.values(collections_1.CollectionField),
            })
            : [];
        const total = await (0, collections_1.countInsightsProjects)(qx, filter);
        return {
            rows: projects.map((p) => {
                const collectionConnections = connections.filter((cp) => cp.insightsProjectId === p.id);
                return {
                    ...p,
                    collections: collections.filter((c) => collectionConnections.some((cp) => cp.collectionId === c.id)),
                    organization: organizations.find((o) => o.id === p.organizationId),
                };
            }),
            total,
            limit,
            offset,
        };
    }
    async updateInsightsProject(insightsProjectId, project) {
        return sequelizeRepository_1.default.withTx(this.options, async (tx) => {
            var _a, _b;
            const qx = sequelizeRepository_1.default.getQueryExecutor({ ...this.options, transaction: tx });
            // Get current project data to check for isLF changes
            const currentProject = await (0, collections_1.queryInsightsProjectById)(qx, insightsProjectId, [
                collections_1.InsightsProjectField.IS_LF,
            ]);
            const previousIsLF = (_a = currentProject === null || currentProject === void 0 ? void 0 : currentProject.isLF) !== null && _a !== void 0 ? _a : false;
            // If segmentId is being updated, fetch the new segment's isLF value
            if (project.segmentId) {
                const segment = await (0, segments_1.findSegmentById)(qx, project.segmentId);
                project.isLF = (_b = segment === null || segment === void 0 ? void 0 : segment.isLF) !== null && _b !== void 0 ? _b : false;
            }
            await (0, collections_1.updateInsightsProject)(qx, insightsProjectId, project);
            // Determine final isLF value (either from project update or current value)
            const finalIsLF = project.isLF !== undefined ? project.isLF : previousIsLF;
            // Check what updates need to be performed
            const collectionsExplicitlyProvided = project.collections !== undefined;
            const isLFValueChanged = project.isLF !== undefined &&
                project.isLF !== previousIsLF &&
                conf_1.ENABLE_LF_COLLECTION_MANAGEMENT;
            // Automatically manage Linux Foundation collection connection (linked to task: automatically add/update to collection)
            let managedCollections = project.collections || [];
            let currentConnections = null;
            // Always manage LF collection when collections are explicitly provided OR when isLF changes AND feature is enabled
            const shouldManageLfCollection = collectionsExplicitlyProvided || isLFValueChanged;
            if (shouldManageLfCollection) {
                // If collections weren't explicitly provided, fetch current connections to preserve them
                let currentCollections = project.collections;
                if (currentCollections === undefined) {
                    currentConnections = await (0, collections_1.findCollectionProjectConnections)(qx, {
                        insightsProjectIds: [insightsProjectId],
                    });
                    currentCollections = currentConnections.map((c) => c.collectionId);
                }
                managedCollections = await this.manageLfCollectionConnection(qx, insightsProjectId, finalIsLF, currentCollections || []);
            }
            // Update collection connections if either:
            // 1. Collections were explicitly provided in the update
            // 2. isLF value changed (which affects LF collection connection)
            // Note: shouldManageLfCollection handles both cases above
            if (shouldManageLfCollection) {
                await (0, collections_1.disconnectProjectsAndCollections)(qx, { insightsProjectId });
                if (managedCollections.length > 0) {
                    await (0, collections_1.connectProjectsAndCollections)(qx, managedCollections.map((c) => {
                        var _a;
                        return ({
                            collectionId: c,
                            insightsProjectId,
                            starred: (_a = project.starred) !== null && _a !== void 0 ? _a : true,
                        });
                    }));
                }
            }
            if (project.repositoryGroups !== undefined) {
                await this.syncRepositoryGroupsWithDb(qx, insightsProjectId, project.repositoryGroups);
            }
            const txSvc = new CollectionService({
                ...this.options,
                transaction: tx,
            });
            return txSvc.findInsightsProjectById(insightsProjectId);
        });
    }
    /**
     * Synchronizes repository groups with the database by creating, updating, or deleting groups based on the provided input.
     *
     * @param {QueryExecutor} qx - The query executor used to perform database operations.
     * @param {string} insightsProjectId - The ID of the insights project to which the repository groups belong.
     * @param {ICreateRepositoryGroup[]} repositoryGroups - The array of repository group objects to be synchronized with the database.
     * @return {Promise<IRepositoryGroup[]>} A promise that resolves to the list of repository groups currently in the database after synchronization.
     */
    // eslint-disable-next-line class-methods-use-this
    async syncRepositoryGroupsWithDb(qx, insightsProjectId, repositoryGroups) {
        const rg = repositoryGroups || [];
        // Get existing repository groups for the given insights project
        const existingRepositoryGroups = await (0, repositoryGroups_1.listRepositoryGroups)(qx, { insightsProjectId });
        // Extract IDs of existing repository groups
        const existingIds = existingRepositoryGroups.map((rg) => rg.id);
        // Extract IDs of repository groups to be synchronized
        const repositoryGroupIds = rg.map((rg) => rg.id);
        // Find repository groups that need to be updated (exist in both lists)
        const toUpdate = rg.filter((rg) => existingIds.includes(rg.id));
        // Find repository groups that need to be created (don't exist or have no ID)
        const toCreate = rg.filter((rg) => !rg.id || !existingIds.includes(rg.id));
        // Find repository groups that need to be deleted (exist but not in new list)
        const toDelete = existingIds.filter((id) => !repositoryGroupIds.includes(id));
        // Create new repository groups
        if (toCreate.length > 0) {
            for (const rg of toCreate) {
                const slug = (0, common_1.getCleanString)(rg.name).replace(/\s+/g, '-');
                await (0, repositoryGroups_1.createRepositoryGroup)(qx, {
                    ...rg,
                    slug,
                    insightsProjectId,
                });
            }
        }
        // Delete repository groups that are no longer needed
        if (toDelete.length > 0) {
            for (const id of toDelete) {
                await (0, repositoryGroups_1.deleteRepositoryGroup)(qx, id);
            }
        }
        // Update existing repository groups with new data
        if (toUpdate.length > 0) {
            for (const rg of toUpdate) {
                const slug = (0, common_1.getCleanString)(rg.name).replace(/\s+/g, '-');
                await (0, repositoryGroups_1.updateRepositoryGroup)(qx, rg.id, {
                    ...rg,
                    slug,
                });
            }
        }
        // Return the updated list of repository groups from the database
        return (0, repositoryGroups_1.listRepositoryGroups)(qx, { insightsProjectId });
    }
    static isSingleRepoOrg(orgs, repoCount) {
        return Array.isArray(orgs) && orgs.length === 1 && repoCount === 1;
    }
    /**
     * Manages Linux Foundation collection connections based on isLF flag.
     * Automatically adds/removes projects from LF collection when isLF changes.
     * (linked to task: automatically add/update to collection)
     *
     * @param {QueryExecutor} qx - The query executor for database operations
     * @param {string} insightsProjectId - The ID of the insights project being managed
     * @param {boolean} isLF - Whether the project is a Linux Foundation project
     * @param {string[]} desiredCollections - Array of collection IDs that the project should be connected to (excluding LF auto-management)
     * @returns {Promise<string[]>} Promise resolving to the final list of collection IDs the project should be connected to, including or excluding the LF collection based on isLF flag
     */
    async manageLfCollectionConnection(qx, insightsProjectId, isLF, desiredCollections = []) {
        if (!conf_1.ENABLE_LF_COLLECTION_MANAGEMENT) {
            this.log.debug(`Skipping LF collection management for project ${insightsProjectId} (feature disabled)`);
            return desiredCollections;
        }
        // Get LF collection ID from configuration
        const linuxFoundationCollectionId = conf_1.LINUX_FOUNDATION_CONFIG === null || conf_1.LINUX_FOUNDATION_CONFIG === void 0 ? void 0 : conf_1.LINUX_FOUNDATION_CONFIG.collectionId;
        if (!linuxFoundationCollectionId) {
            this.log.warn(`Linux Foundation collection ID not configured, skipping LF collection management for project ${insightsProjectId}`);
            return desiredCollections;
        }
        let updatedCollections = [...desiredCollections];
        if (isLF && !updatedCollections.includes(linuxFoundationCollectionId)) {
            // Add to Linux Foundation collection if isLF=true and not already in desired collections
            updatedCollections.push(linuxFoundationCollectionId);
            this.log.info(`Auto-adding project ${insightsProjectId} to Linux Foundation collection (isLF=true)`);
        }
        else if (!isLF && updatedCollections.includes(linuxFoundationCollectionId)) {
            // Remove from Linux Foundation collection if isLF=false and currently in desired collections
            updatedCollections = updatedCollections.filter((id) => id !== linuxFoundationCollectionId);
            this.log.info(`Auto-removing project ${insightsProjectId} from Linux Foundation collection (isLF=false) - overriding user selection`);
        }
        return updatedCollections;
    }
    async findGithubInsightsForSegment(segmentId) {
        return sequelizeRepository_1.default.withTx(this.options, async (tx) => {
            const qx = sequelizeRepository_1.default.getQueryExecutor({ ...this.options, transaction: tx });
            const integrations = await (0, integrations_1.fetchIntegrationsForSegment)(qx, segmentId);
            const segment = await (0, segments_1.findSegmentById)(qx, segmentId);
            const [githubIntegration] = integrations.filter((integration) => integration.platform === types_1.PlatformType.GITHUB ||
                integration.platform === types_1.PlatformType.GITHUB_NANGO);
            if (!githubIntegration) {
                return null;
            }
            const settings = githubIntegration.settings;
            // The orgs must have at least one repo
            if (!(settings === null || settings === void 0 ? void 0 : settings.orgs) || !Array.isArray(settings.orgs) || settings.orgs.length === 0) {
                return null;
            }
            const repos = await (0, repositories_1.getReposForGithubIntegration)(qx, githubIntegration.id);
            if (repos.length === 0) {
                return null;
            }
            const mainOrg = await common_services_1.GithubIntegrationService.findMainGithubOrganizationWithLLM(qx, segment.name, settings.orgs);
            if (!mainOrg) {
                return null;
            }
            const details = CollectionService.isSingleRepoOrg(settings.orgs, repos.length)
                ? await common_services_1.GithubIntegrationService.findRepoDetails(mainOrg.name, repos[0].name)
                : {
                    ...(await common_services_1.GithubIntegrationService.findOrgDetails(mainOrg.name)),
                    topics: mainOrg.topics,
                };
            if (!details) {
                return null;
            }
            return {
                description: mainOrg.description,
                github: details.github,
                logoUrl: details.logoUrl,
                name: segment.name,
                topics: details.topics,
                twitter: details.twitter,
                website: details.website,
            };
        });
    }
    static isValidPlatform(value) {
        return typeof value === 'string' && Object.values(types_1.PlatformType).includes(value);
    }
    async findSegmentsWidgetsById(segmentId) {
        return sequelizeRepository_1.default.withTx(this.options, async (tx) => {
            const qx = sequelizeRepository_1.default.getQueryExecutor({ ...this.options, transaction: tx });
            const widgets = new Set();
            const integrations = await (0, integrations_1.fetchIntegrationsForSegment)(qx, segmentId);
            const platforms = [
                ...new Set(integrations
                    .map((integration) => integration.platform)
                    .filter(CollectionService.isValidPlatform)),
            ];
            // Check for mapped repositories and add GitHub if there are any
            const hasGithubMappedRepos = await (0, segments_1.hasMappedRepos)(qx, segmentId, [
                types_1.PlatformType.GITHUB,
                types_1.PlatformType.GITHUB_NANGO,
            ]);
            if (hasGithubMappedRepos && !platforms.includes(types_1.PlatformType.GITHUB)) {
                platforms.push(types_1.PlatformType.GITHUB);
            }
            for (const platform of platforms) {
                Object.entries(types_1.DEFAULT_WIDGET_VALUES).forEach(([key, config]) => {
                    if (config.enabled &&
                        config.platform.some((p) => p.toLowerCase() === platform.toLowerCase())) {
                        widgets.add(key);
                    }
                });
            }
            return {
                platforms,
                widgets: [...widgets],
            };
        });
    }
    async findInsightsProjectsBySegmentId(segmentId) {
        const qx = sequelizeRepository_1.default.getQueryExecutor(this.options);
        const result = await (0, collections_1.queryInsightsProjects)(qx, {
            filter: {
                segmentId: { eq: segmentId },
            },
            fields: Object.values(collections_1.InsightsProjectField),
        });
        return result;
    }
    async findInsightsProjectsBySlug(slug) {
        const qx = sequelizeRepository_1.default.getQueryExecutor(this.options);
        const normalizedSlug = slug.replace(/^nonlf_/, '');
        const result = await (0, collections_1.queryInsightsProjects)(qx, {
            filter: {
                slug: { eq: normalizedSlug },
                segmentId: { eq: null },
            },
            fields: Object.values(collections_1.InsightsProjectField),
        });
        return result;
    }
}
exports.CollectionService = CollectionService;
//# sourceMappingURL=collectionService.js.map