"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
/* eslint-disable no-promise-executor-return */
const auth_app_1 = require("@octokit/auth-app");
const request_1 = require("@octokit/request");
const axios_1 = __importDefault(require("axios"));
const lodash_1 = __importDefault(require("lodash"));
const moment_1 = __importDefault(require("moment"));
const sequelize_1 = require("sequelize");
const common_1 = require("@crowd/common");
const common_services_1 = require("@crowd/common_services");
const mailinglist_1 = require("@crowd/data-access-layer/src/mailinglist");
const repositories_1 = require("@crowd/data-access-layer/src/repositories");
const segments_1 = require("@crowd/data-access-layer/src/segments");
const nango_1 = require("@crowd/nango");
const redis_1 = require("@crowd/redis");
const temporal_1 = require("@crowd/temporal");
const types_1 = require("@crowd/types");
const githubInstallationsRepository_1 = __importDefault(require("@/database/repositories/githubInstallationsRepository"));
const integrationProgressRepository_1 = __importDefault(require("@/database/repositories/integrationProgressRepository"));
const segmentRepository_1 = __importDefault(require("@/database/repositories/segmentRepository"));
const getProjects_1 = require("@/serverless/integrations/usecases/gitlab/getProjects");
const removeWebhooks_1 = require("@/serverless/integrations/usecases/gitlab/removeWebhooks");
const setupWebhooks_1 = require("@/serverless/integrations/usecases/gitlab/setupWebhooks");
const getUserSubscriptions_1 = require("@/serverless/integrations/usecases/groupsio/getUserSubscriptions");
const index_1 = require("../conf/index");
const integrationRepository_1 = __importDefault(require("../database/repositories/integrationRepository"));
const sequelizeRepository_1 = __importDefault(require("../database/repositories/sequelizeRepository"));
const telemetryTrack_1 = __importDefault(require("../segment/telemetryTrack"));
const track_1 = __importDefault(require("../segment/track"));
const getInstalledRepositories_1 = require("../serverless/integrations/usecases/github/rest/getInstalledRepositories");
const getRemoteStats_1 = require("../serverless/integrations/usecases/github/rest/getRemoteStats");
const getOrganizations_1 = require("../serverless/integrations/usecases/linkedin/getOrganizations");
const getToken_1 = __importDefault(require("../serverless/integrations/usecases/nango/getToken"));
const queueService_1 = require("../serverless/utils/queueService");
const collectionService_1 = require("./collectionService");
const discordToken = index_1.DISCORD_CONFIG.token || index_1.DISCORD_CONFIG.token2;
class IntegrationService {
    constructor(options) {
        this.options = options;
    }
    async createOrUpdate(data, transaction, options) {
        try {
            const record = await integrationRepository_1.default.findByPlatform(data.platform, {
                ...(options || this.options),
                transaction,
            });
            const updatedRecord = await this.update(record.id, data, transaction, options);
            if (!index_1.IS_TEST_ENV) {
                (0, track_1.default)('Integration Updated', {
                    id: data.id,
                    platform: data.platform,
                    status: data.status,
                }, { ...this.options });
            }
            return updatedRecord;
        }
        catch (error) {
            this.options.log.error(error);
            if (error.code === 404) {
                const record = await this.create(data, transaction, options);
                if (!index_1.IS_TEST_ENV) {
                    (0, track_1.default)('Integration Created', {
                        id: data.id,
                        platform: data.platform,
                        status: data.status,
                    }, { ...this.options });
                    (0, telemetryTrack_1.default)('Integration created', {
                        id: record.id,
                        createdAt: record.createdAt,
                        platform: record.platform,
                    }, this.options);
                }
                return record;
            }
            throw error;
        }
    }
    /**
     * Find all active integrations for a tenant
     * @returns The active integrations for a tenant
     */
    async getAllActiveIntegrations() {
        return integrationRepository_1.default.findAndCountAll({ filter: { status: 'done' } }, this.options);
    }
    async findByPlatform(platform) {
        return integrationRepository_1.default.findByPlatform(platform, this.options);
    }
    async findAllByPlatform(platform) {
        return integrationRepository_1.default.findAllByPlatform(platform, this.options);
    }
    static isCodePlatform(value) {
        return [
            types_1.PlatformType.GITHUB,
            types_1.PlatformType.GITHUB_NANGO,
            types_1.PlatformType.GITLAB,
            types_1.PlatformType.GIT,
            types_1.PlatformType.GERRIT,
        ].includes(value);
    }
    async create(data, transaction, options) {
        try {
            const txOptions = {
                ...(options || this.options),
                transaction,
            };
            const integration = await integrationRepository_1.default.create(data, txOptions);
            const collectionService = new collectionService_1.CollectionService(txOptions);
            const [insightsProject] = await collectionService.findInsightsProjectsBySegmentId(integration.segmentId);
            if (!insightsProject) {
                this.options.log.info(`The segmentId: ${integration.segmentId} does not have any InsightsProject related`);
                return integration;
            }
            const { segmentId, id: insightsProjectId } = insightsProject;
            const { platform } = data;
            // Skip for GITHUB_NANGO: repos aren't available at create time.
            // - GITHUB_NANGO: repos stripped from settings; githubNangoConnect calls updateInsightsProject after mapGithubRepos populates public.repositories.
            if (platform !== types_1.PlatformType.GITHUB_NANGO) {
                await this.updateInsightsProject({
                    insightsProjectId,
                    isFirstUpdate: true,
                    platform,
                    segmentId,
                    transaction,
                });
            }
            return integration;
        }
        catch (error) {
            sequelizeRepository_1.default.handleUniqueFieldError(error, this.options.language, 'integration');
            throw error;
        }
    }
    async update(id, data, transaction, options) {
        try {
            const txOptions = {
                ...(options || this.options),
                transaction,
            };
            const integration = await integrationRepository_1.default.update(id, data, txOptions);
            const collectionService = new collectionService_1.CollectionService(txOptions);
            const [insightsProject] = await collectionService.findInsightsProjectsBySegmentId(integration.segmentId);
            const { platform } = data;
            if (insightsProject) {
                const { segmentId, id: insightsProjectId } = insightsProject;
                await this.updateInsightsProject({
                    insightsProjectId,
                    platform,
                    segmentId,
                    transaction,
                });
            }
            return integration;
        }
        catch (err) {
            this.options.log.error(err);
            sequelizeRepository_1.default.handleUniqueFieldError(err, this.options.language, 'integration');
            throw err;
        }
    }
    async updateInsightsProject({ insightsProjectId, isFirstUpdate = false, platform, segmentId, transaction, }) {
        const collectionService = new collectionService_1.CollectionService({ ...this.options, transaction });
        const data = {};
        const { widgets } = await collectionService.findSegmentsWidgetsById(segmentId);
        data.widgets = widgets;
        if ((platform === types_1.PlatformType.GITHUB || platform === types_1.PlatformType.GITHUB_NANGO) &&
            isFirstUpdate) {
            const githubInsights = await collectionService.findGithubInsightsForSegment(segmentId);
            if (githubInsights) {
                this.options.log.info(`Static Insights found: ${JSON.stringify(githubInsights)}`);
                await this.options.temporal.workflow.start('automaticCategorization', {
                    taskQueue: 'categorization',
                    workflowId: `categorization/${segmentId}`,
                    workflowIdReusePolicy: temporal_1.WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING,
                    retry: {
                        maximumAttempts: 10,
                    },
                    args: [
                        {
                            description: githubInsights.description,
                            github: githubInsights.github,
                            topics: githubInsights.topics,
                            website: githubInsights.website,
                            segmentId,
                        },
                    ],
                });
                data.description = githubInsights.description;
                data.github = githubInsights.github;
                data.keywords = githubInsights.topics;
                data.logoUrl = githubInsights.logoUrl;
                data.name = githubInsights.name;
                data.twitter = githubInsights.twitter;
                data.website = githubInsights.website;
            }
        }
        this.options.log.info(`Insight Project updated: ${insightsProjectId}`);
        await collectionService.updateInsightsProject(insightsProjectId, data);
    }
    async destroyAll(ids) {
        var _a, _b, _c, _d;
        const toRemoveRepo = new Set();
        let segmentId;
        // Collect GitLab webhook info before opening the transaction so external HTTP calls
        // don't hold the DB connection idle long enough to trigger a connection timeout.
        const gitlabWebhookRemovals = [];
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        try {
            for (const id of ids) {
                let integration;
                try {
                    integration = await this.findById(id);
                    if (integration.segmentId) {
                        segmentId = integration.segmentId;
                    }
                }
                catch (err) {
                    throw new common_1.Error404();
                }
                // remove github/gitlab/gerrit remotes from git integration
                if (integration.platform === types_1.PlatformType.GITHUB ||
                    integration.platform === types_1.PlatformType.GITLAB ||
                    integration.platform === types_1.PlatformType.GITHUB_NANGO ||
                    integration.platform === types_1.PlatformType.GERRIT) {
                    let repos = {};
                    // Get repos based on platform
                    if (integration.platform === types_1.PlatformType.GERRIT) {
                        if (((_b = (_a = integration.settings) === null || _a === void 0 ? void 0 : _a.remote) === null || _b === void 0 ? void 0 : _b.enableGit) &&
                            ((_d = (_c = integration.settings) === null || _c === void 0 ? void 0 : _c.remote) === null || _d === void 0 ? void 0 : _d.repoNames)) {
                            const stripGit = (url) => {
                                if (url.endsWith('.git')) {
                                    return url.slice(0, -4);
                                }
                                return url;
                            };
                            const gerritUrls = integration.settings.remote.repoNames.map((repoName) => stripGit(`${integration.settings.remote.orgURL}/${repoName}`));
                            repos[integration.segmentId] = gerritUrls;
                        }
                    }
                    else {
                        // Use public.repositories to get repos owned by this integration
                        const qx = sequelizeRepository_1.default.getQueryExecutor({
                            ...this.options,
                            transaction,
                        });
                        const integrationRepos = await (0, repositories_1.getRepositoriesBySourceIntegrationId)(qx, id);
                        repos = integrationRepos.reduce((acc, repo) => {
                            if (!acc[repo.segmentId]) {
                                acc[repo.segmentId] = [];
                            }
                            acc[repo.segmentId].push(repo.url);
                            return acc;
                        }, {});
                    }
                    for (const [segmentId, urls] of Object.entries(repos)) {
                        urls.forEach((url) => toRemoveRepo.add(url));
                        const segmentOptions = {
                            ...this.options,
                            currentSegments: [
                                {
                                    ...this.options.currentSegments[0],
                                    id: segmentId,
                                },
                            ],
                        };
                        const gitIntegration = await integrationRepository_1.default.findByPlatform(types_1.PlatformType.GIT, segmentOptions);
                        // Get all repos for this git integration from public.repositories
                        const qxForGit = sequelizeRepository_1.default.getQueryExecutor({
                            ...this.options,
                            transaction,
                        });
                        const allGitRepos = await (0, repositories_1.getIntegrationReposMapping)(qxForGit, gitIntegration.id);
                        // Filter to get repos NOT owned by the source integration being deleted
                        const remainingRepos = allGitRepos.filter((repo) => repo.sourceIntegrationId !== id);
                        if (remainingRepos.length === 0) {
                            // If no repos left, delete the Git integration entirely
                            await integrationRepository_1.default.destroy(gitIntegration.id, {
                                ...this.options,
                                transaction,
                            });
                        }
                        else {
                            // Update git integration settings with remaining remotes
                            const remainingRemotes = remainingRepos.map((r) => r.url);
                            await this.gitConnectOrUpdate({
                                remotes: remainingRemotes.map((url) => ({ url, forkedFrom: null })),
                            }, segmentOptions, integration.platform);
                        }
                    }
                    if (integration.platform === types_1.PlatformType.GITHUB ||
                        integration.platform === types_1.PlatformType.GITHUB_NANGO) {
                        // Soft delete from public.repositories only repos owned by this GitHub integration
                        // This preserves native Git repos that aren't mirrored from GitHub
                        const qx = sequelizeRepository_1.default.getQueryExecutor({
                            ...this.options,
                            transaction,
                        });
                        const reposToDelete = await (0, repositories_1.getRepositoriesBySourceIntegrationId)(qx, integration.id);
                        if (reposToDelete.length > 0) {
                            const urlsToDelete = reposToDelete.map((r) => r.url);
                            await (0, repositories_1.softDeleteRepositories)(qx, urlsToDelete, integration.id);
                            this.options.log.info(`Soft deleted ${urlsToDelete.length} repos from public.repositories for GitHub integration ${integration.id}`);
                        }
                    }
                }
                if (integration.platform === types_1.PlatformType.GITLAB && integration.settings.webhooks) {
                    gitlabWebhookRemovals.push({
                        token: integration.token,
                        projectIds: integration.settings.webhooks.map((hook) => hook.projectId),
                        hookIds: integration.settings.webhooks.map((hook) => hook.hookId),
                    });
                }
                if (integration.platform === types_1.PlatformType.GIT) {
                    await this.validateGitIntegrationDeletion(integration.id, {
                        ...this.options,
                        transaction,
                    });
                }
                if (integration.platform === types_1.PlatformType.MAILINGLIST) {
                    const qx = sequelizeRepository_1.default.getQueryExecutor({
                        ...this.options,
                        transaction,
                    });
                    await (0, mailinglist_1.softDeleteMailingListsByIntegrationId)(qx, integration.id);
                }
                // Soft delete from public.repositories for code integrations
                if (IntegrationService.isCodePlatform(integration.platform)) {
                    const txService = new IntegrationService({ ...this.options, transaction });
                    // When destroying, don't skip mirrored repos - delete all
                    await txService.mapUnifiedRepositories(integration.platform, integration.id, {}, false);
                }
                await integrationRepository_1.default.destroy(id, {
                    ...this.options,
                    transaction,
                });
            }
            const collectionService = new collectionService_1.CollectionService({ ...this.options, transaction });
            let insightsProject = null;
            let widgets = [];
            if (segmentId) {
                const [project] = await collectionService.findInsightsProjectsBySegmentId(segmentId);
                insightsProject = project;
                const widgetsResult = await collectionService.findSegmentsWidgetsById(segmentId);
                widgets = widgetsResult.widgets;
                // Note: Repos are soft-deleted in public.repositories via mapUnifiedRepositories above
            }
            if (insightsProject) {
                await collectionService.updateInsightsProject(insightsProject.id, { widgets });
            }
            await sequelizeRepository_1.default.commitTransaction(transaction);
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw error;
        }
        // Remove GitLab webhooks after the transaction commits — these are external HTTP calls
        // and must not hold a DB connection open.
        await Promise.all(gitlabWebhookRemovals.map(({ token, projectIds, hookIds }) => (0, removeWebhooks_1.removeGitlabWebhooks)(token, projectIds, hookIds)));
    }
    async findById(id) {
        const record = await integrationRepository_1.default.findById(id, this.options);
        if (record) {
            const segmentRepository = new segmentRepository_1.default(this.options);
            const segment = await segmentRepository.findById(record.segmentId);
            return {
                ...record,
                segment,
            };
        }
        return record;
    }
    async findAllAutocomplete(search, limit) {
        return integrationRepository_1.default.findAllAutocomplete(search, limit, this.options);
    }
    async findAndCountAll(args) {
        return integrationRepository_1.default.findAndCountAll(args, this.options);
    }
    /**
     * Retrieves global integrations for the specified tenant.
     *
     * @param {any} args - Additional arguments that define search criteria or constraints.
     * @return {Promise<any>} A promise that resolves to the list of global integrations matching the criteria.
     */
    async findGlobalIntegrations(args) {
        return integrationRepository_1.default.findGlobalIntegrations(args, this.options);
    }
    /**
     * Fetches the global count of integration statuses for a given tenant.
     *
     * @param {Object} args - Additional arguments to refine the query.
     * @return {Promise<number>} A promise that resolves to the count of global integration statuses.
     */
    async findGlobalIntegrationsStatusCount(args) {
        return integrationRepository_1.default.findGlobalIntegrationsStatusCount(args, this.options);
    }
    async query(data) {
        const advancedFilter = data.filter;
        const orderBy = data.orderBy;
        const limit = data.limit;
        const offset = data.offset;
        const result = await integrationRepository_1.default.findAndCountAll({ advancedFilter, orderBy, limit, offset }, this.options);
        // Decrypt encrypted values for Confluence and Jira integrations
        if (result.rows) {
            result.rows = result.rows.map((integration) => ({
                ...integration,
                settings: common_services_1.CommonIntegrationService.decryptIntegrationSettings(integration.platform, integration.settings),
            }));
        }
        return result;
    }
    /**
     * Returns installation access token for a Github App installation
     * @param installId Install id of the Github app
     * @returns Installation authentication token
     */
    static async getInstallToken(installId) {
        let privateKey = index_1.GITHUB_CONFIG.privateKey;
        if (index_1.KUBE_MODE) {
            privateKey = Buffer.from(privateKey, 'base64').toString('ascii');
        }
        const auth = (0, auth_app_1.createAppAuth)({
            appId: index_1.GITHUB_CONFIG.appId,
            privateKey,
            clientId: index_1.GITHUB_CONFIG.clientId,
            clientSecret: index_1.GITHUB_CONFIG.clientSecret,
        });
        // Retrieve installation access token
        const installationAuthentication = await auth({
            type: 'installation',
            installationId: installId,
        });
        return installationAuthentication.token;
    }
    static extractOwner(repos, options) {
        const owners = lodash_1.default.countBy(repos, 'owner');
        if (Object.keys(owners).length === 1) {
            return Object.keys(owners)[0];
        }
        options.log.warn('Multiple owners found in GitHub repos!', owners);
        // return the owner with the most repos
        return lodash_1.default.maxBy(Object.keys(owners), (owner) => owners[owner]);
    }
    async connectGithub(code, installId, setupAction = 'install') {
        if (setupAction === 'request') {
            return this.createOrUpdate({
                platform: types_1.PlatformType.GITHUB,
                status: 'waiting-approval',
            }, await sequelizeRepository_1.default.createTransaction(this.options));
        }
        const GITHUB_AUTH_ACCESSTOKEN_URL = 'https://github.com/login/oauth/access_token';
        const CLIENT_ID = index_1.GITHUB_CONFIG.clientId;
        const CLIENT_SECRET = index_1.GITHUB_CONFIG.clientSecret;
        const tokenResponse = await (0, axios_1.default)({
            method: 'post',
            url: GITHUB_AUTH_ACCESSTOKEN_URL,
            data: {
                client_id: CLIENT_ID,
                client_secret: CLIENT_SECRET,
                code,
            },
        });
        let token = tokenResponse.data;
        token = token.slice(token.search('=') + 1, token.search('&'));
        try {
            const requestWithAuth = request_1.request.defaults({
                headers: {
                    authorization: `token ${token}`,
                },
            });
            await requestWithAuth('GET /user');
        }
        catch (_a) {
            throw new common_1.Error542(`Invalid token for GitHub integration. Code: ${code}, setupAction: ${setupAction}. Token: ${token}`);
        }
        const installToken = await IntegrationService.getInstallToken(installId);
        const repos = await (0, getInstalledRepositories_1.getInstalledRepositories)(installToken);
        const githubOwner = IntegrationService.extractOwner(repos, this.options);
        let orgAvatar;
        try {
            const response = await (0, request_1.request)('GET /users/{user}', {
                user: githubOwner,
            });
            orgAvatar = response.data.avatar_url;
        }
        catch (err) {
            this.options.log.warn(err, 'Error while fetching GitHub user!');
        }
        const integration = await this.createOrUpdateGithubIntegration({
            platform: types_1.PlatformType.GITHUB,
            token,
            settings: { updateMemberAttributes: true, orgAvatar },
            integrationIdentifier: installId,
            status: 'mapping',
        }, repos);
        return integration;
    }
    async connectGithubInstallation(installId) {
        const installToken = await IntegrationService.getInstallToken(installId);
        const repos = await (0, getInstalledRepositories_1.getInstalledRepositories)(installToken);
        const githubOwner = IntegrationService.extractOwner(repos, this.options);
        let orgAvatar;
        try {
            const response = await (0, request_1.request)('GET /users/{user}', {
                user: githubOwner,
            });
            orgAvatar = response.data.avatar_url;
        }
        catch (err) {
            this.options.log.warn(err, 'Error while fetching GitHub user!');
        }
        const integration = await this.createOrUpdateGithubIntegration({
            platform: types_1.PlatformType.GITHUB,
            token: installToken,
            settings: { updateMemberAttributes: true, orgAvatar },
            integrationIdentifier: installId,
            status: 'mapping',
        }, repos);
        return integration;
    }
    async getGithubInstallations() {
        return githubInstallationsRepository_1.default.getInstallations(this.options);
    }
    /**
     * Creates or updates a GitHub integration, handling large repos data
     * @param integrationData The integration data to create or update
     * @param repos The repositories data
     */
    async createOrUpdateGithubIntegration(integrationData, repos) {
        var _a;
        let integration;
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        try {
            // Get the first repo's owner since we know all repos are from same installation
            const orgName = (_a = repos[0]) === null || _a === void 0 ? void 0 : _a.owner;
            // Create initial integration with org structure but empty repos
            const initialOrg = {
                name: orgName,
                logo: integrationData.settings.orgAvatar,
                url: `https://github.com/${orgName}`,
                fullSync: true,
                updatedAt: new Date().toISOString(),
                repos: [],
            };
            integration = await this.createOrUpdate({
                ...integrationData,
                settings: {
                    ...integrationData.settings,
                    orgs: [initialOrg],
                },
            }, transaction);
            await sequelizeRepository_1.default.commitTransaction(transaction);
            // Transform repos into the new format
            const transformedRepos = repos.map((repo) => ({
                name: repo.name,
                url: repo.url,
                updatedAt: repo.createdAt || new Date().toISOString(),
                forkedFrom: repo.forkedFrom || null,
            }));
            // Add repos in chunks
            const chunkSize = 100; // Process 100 repos at a time
            for (let i = 0; i < transformedRepos.length; i += chunkSize) {
                const reposChunk = transformedRepos.slice(i, i + chunkSize);
                await this.appendGitHubReposToOrg(integration.id, reposChunk);
            }
            return integration;
        }
        catch (err) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw err;
        }
    }
    async appendGitHubReposToOrg(integrationId, repos) {
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        const sequelize = sequelizeRepository_1.default.getSequelize(this.options);
        try {
            // Append repos to the first (and only) org's repos array
            const query = `
        UPDATE integrations
        SET settings = jsonb_set(
          settings,
          '{orgs,0,repos}',
          COALESCE(settings->'orgs'->0->'repos', '[]'::jsonb) || ?::jsonb
        )
        WHERE id = ?
      `;
            const values = [JSON.stringify(repos), integrationId];
            await sequelize.query(query, {
                replacements: values,
                transaction,
            });
            await sequelizeRepository_1.default.commitTransaction(transaction);
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw error;
        }
    }
    async githubNangoConnect(settings, mapping, integrationId) {
        const existingTransaction = sequelizeRepository_1.default.getTransaction(this.options);
        const transaction = existingTransaction || (await sequelizeRepository_1.default.createTransaction(this.options));
        const txOptions = {
            ...this.options,
            transaction,
        };
        const txService = new IntegrationService(txOptions);
        try {
            // Extract repos from orgs and build forkedFrom map, then store settings without repos
            const forkedFromMap = new Map();
            if (settings === null || settings === void 0 ? void 0 : settings.orgs) {
                for (const org of settings.orgs) {
                    for (const repo of org.repos || []) {
                        if (repo.url) {
                            forkedFromMap.set(repo.url, repo.forkedFrom || null);
                        }
                    }
                }
            }
            // Strip repos from orgs before storing in settings
            const settingsToStore = {
                ...settings,
                orgs: ((settings === null || settings === void 0 ? void 0 : settings.orgs) || []).map(({ repos: _repos, ...org }) => org),
            };
            let integration;
            if (!integrationId) {
                // create new integration
                integration = await txService.createOrUpdate({
                    platform: types_1.PlatformType.GITHUB_NANGO,
                    settings: settingsToStore,
                    status: 'done',
                }, transaction);
                // create github mapping - this also creates git integration
                await txService.mapGithubRepos(integration.id, mapping, false, forkedFromMap);
                // Re-run updateInsightsProject now that repos are mapped, so metadata can be fetched.
                // This is a best-effort enrichment step: failures here should not roll back the core
                // GitHub Nango connection or repo mapping.
                try {
                    const collectionService = new collectionService_1.CollectionService(txOptions);
                    const [insightsProject] = await collectionService.findInsightsProjectsBySegmentId(integration.segmentId);
                    if (insightsProject) {
                        await txService.updateInsightsProject({
                            insightsProjectId: insightsProject.id,
                            isFirstUpdate: true,
                            platform: types_1.PlatformType.GITHUB_NANGO,
                            segmentId: insightsProject.segmentId,
                            transaction,
                        });
                    }
                }
                catch (err) {
                    // Log and continue; metadata enrichment is non-critical and should not block connection.
                    // eslint-disable-next-line no-console
                    console.error('Failed to update insights project metadata after GitHub Nango connection', {
                        integrationId: integration === null || integration === void 0 ? void 0 : integration.id,
                        segmentId: integration === null || integration === void 0 ? void 0 : integration.segmentId,
                        error: err,
                    });
                }
            }
            else {
                // update existing integration
                integration = await txService.findById(integrationId);
                // create github mapping - this also creates git integration
                await txService.mapGithubRepos(integrationId, mapping, false, forkedFromMap);
                integration = await txService.createOrUpdate({
                    id: integrationId,
                    platform: types_1.PlatformType.GITHUB_NANGO,
                    settings: settingsToStore,
                }, transaction);
            }
            if (!existingTransaction) {
                await sequelizeRepository_1.default.commitTransaction(transaction);
            }
            await this.options.temporal.workflow.start('syncGithubIntegration', {
                taskQueue: 'nango',
                workflowId: `github-nango-sync/${integration.id}`,
                workflowIdReusePolicy: temporal_1.WorkflowIdReusePolicy.ALLOW_DUPLICATE,
                workflowIdConflictPolicy: temporal_1.WorkflowIdConflictPolicy.USE_EXISTING,
                retry: {
                    maximumAttempts: 10,
                },
                args: [{ integrationId: integration.id }],
            });
            return await this.findById(integration.id);
        }
        catch (err) {
            this.options.log.error(err, 'Error while creating or updating GitHub integration!');
            if (!existingTransaction) {
                await sequelizeRepository_1.default.rollbackTransaction(transaction);
            }
            throw err;
        }
    }
    async mapGithubRepos(integrationId, mapping, fireOnboarding = true, forkedFromMap) {
        var _a;
        this.options.log.info(`Mapping GitHub repos for integration ${integrationId}!`);
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        const txOptions = {
            ...this.options,
            transaction,
        };
        let onboardingIntegration;
        try {
            // add the repos to the git integration
            const repos = Object.entries(mapping).reduce((acc, [url, segmentId]) => {
                if (!acc[segmentId]) {
                    acc[segmentId] = [];
                }
                acc[segmentId].push(url);
                return acc;
            }, {});
            // Note: Repos are synced to public.repositories via mapUnifiedRepositories at the end of this method
            const integration = await integrationRepository_1.default.findById(integrationId, txOptions);
            // Build forkedFrom map from repositories table if not provided
            if (!forkedFromMap) {
                forkedFromMap = new Map();
                const qx = sequelizeRepository_1.default.getQueryExecutor(txOptions);
                const existingRepos = await (0, repositories_1.getReposForGithubIntegration)(qx, integrationId);
                if (existingRepos.length > 0) {
                    for (const repo of existingRepos) {
                        forkedFromMap.set(repo.url, repo.forkedFrom);
                    }
                }
                else {
                    // On first mapping, repositories table is empty — read forkedFrom from settings
                    const orgs = ((_a = integration.settings) === null || _a === void 0 ? void 0 : _a.orgs) || [];
                    for (const org of orgs) {
                        for (const repo of org.repos || []) {
                            if (repo.url) {
                                forkedFromMap.set(repo.url, repo.forkedFrom || null);
                            }
                        }
                    }
                }
            }
            for (const [segmentId, urls] of Object.entries(repos)) {
                let isGitintegrationConfigured;
                const segmentOptions = {
                    ...txOptions,
                    currentSegments: [
                        {
                            ...this.options.currentSegments[0],
                            id: segmentId,
                        },
                    ],
                };
                try {
                    this.options.log.info(`Finding Git integration for segment ${segmentId}!`);
                    await integrationRepository_1.default.findByPlatform(types_1.PlatformType.GIT, segmentOptions);
                    isGitintegrationConfigured = true;
                }
                catch (err) {
                    isGitintegrationConfigured = false;
                }
                const buildRemotes = (urlList) => urlList.map((url) => ({ url, forkedFrom: forkedFromMap.get(url) || null }));
                if (isGitintegrationConfigured) {
                    this.options.log.info(`Finding Git integration for segment ${segmentId}!`);
                    const gitInfo = await this.gitGetRemotes(segmentOptions);
                    const gitRemotes = gitInfo[segmentId].remotes;
                    const allUrls = Array.from(new Set([...gitRemotes, ...urls]));
                    this.options.log.info(`Updating Git integration for segment ${segmentId}!`);
                    await this.gitConnectOrUpdate({ remotes: buildRemotes(allUrls) }, segmentOptions, types_1.PlatformType.GITHUB);
                }
                else {
                    this.options.log.info(`Updating Git integration for segment ${segmentId}!`);
                    await this.gitConnectOrUpdate({ remotes: buildRemotes(urls) }, segmentOptions, types_1.PlatformType.GITHUB);
                }
            }
            // sync to public.repositories
            const txService = new IntegrationService(txOptions);
            await txService.mapUnifiedRepositories(integration.platform, integrationId, mapping, true, forkedFromMap);
            // Now that repos are in the repositories table, strip them from settings
            const qxTx = sequelizeRepository_1.default.getQueryExecutor(txOptions);
            await (0, repositories_1.stripReposFromGithubSettings)(qxTx, integrationId);
            if (fireOnboarding) {
                this.options.log.info('Updating integration status to in-progress!');
                onboardingIntegration = await integrationRepository_1.default.update(integrationId, { status: 'in-progress' }, txOptions);
            }
            await sequelizeRepository_1.default.commitTransaction(transaction);
        }
        catch (err) {
            this.options.log.error(err, 'Error while mapping GitHub repos!');
            try {
                await sequelizeRepository_1.default.rollbackTransaction(transaction);
            }
            catch (rErr) {
                this.options.log.error(rErr, 'Error while rolling back transaction!');
            }
            throw err;
        }
        // Trigger the run AFTER commit so the worker can see the repositories rows
        if (onboardingIntegration) {
            this.options.log.info('Sending GitHub message to int-run-worker!');
            const emitter = await (0, queueService_1.getIntegrationRunWorkerEmitter)();
            await emitter.triggerIntegrationRun(onboardingIntegration.platform, onboardingIntegration.id, true);
        }
    }
    /**
     * Get repository mappings for an integration
     * Uses the unified public.repositories table instead of legacy githubRepos table
     * @param integrationId - The source integration ID to filter by
     * @returns Array of repositories with segment info and integration IDs
     */
    async getIntegrationRepositories(integrationId) {
        const qx = sequelizeRepository_1.default.getQueryExecutor(this.options);
        return (0, repositories_1.getIntegrationReposMapping)(qx, integrationId);
    }
    /**
     * Adds discord integration to a tenant
     * @param guildId Guild id of the discord server
     * @returns integration object
     */
    async discordConnect(guildId) {
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        let integration;
        try {
            this.options.log.info('Creating Discord integration!');
            integration = await this.createOrUpdate({
                platform: types_1.PlatformType.DISCORD,
                integrationIdentifier: guildId,
                token: discordToken,
                settings: { channels: [], updateMemberAttributes: true },
                status: 'in-progress',
            }, transaction);
            await sequelizeRepository_1.default.commitTransaction(transaction);
        }
        catch (err) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw err;
        }
        this.options.log.info('Sending Discord message to int-run-worker!');
        const emitter = await (0, queueService_1.getIntegrationRunWorkerEmitter)();
        await emitter.triggerIntegrationRun(integration.platform, integration.id, true);
        return integration;
    }
    async linkedinOnboard(organizationId) {
        let integration;
        try {
            integration = await integrationRepository_1.default.findByPlatform(types_1.PlatformType.LINKEDIN, {
                ...this.options,
            });
        }
        catch (err) {
            this.options.log.error(err, 'Error while fetching LinkedIn integration from DB!');
            throw new common_1.Error404();
        }
        let valid = false;
        for (const org of integration.settings.organizations) {
            if (org.id === organizationId) {
                org.inUse = true;
                valid = true;
                break;
            }
        }
        if (!valid) {
            this.options.log.error(`No organization with id ${organizationId} found!`);
            throw new common_1.Error404(this.options.language, 'errors.linkedin.noOrganizationFound');
        }
        if (integration.status === 'pending-action') {
            const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
            try {
                integration = await this.createOrUpdate({
                    platform: types_1.PlatformType.LINKEDIN,
                    status: 'in-progress',
                    settings: integration.settings,
                }, transaction);
                await sequelizeRepository_1.default.commitTransaction(transaction);
            }
            catch (err) {
                await sequelizeRepository_1.default.rollbackTransaction(transaction);
                throw err;
            }
            const emitter = await (0, queueService_1.getIntegrationRunWorkerEmitter)();
            await emitter.triggerIntegrationRun(integration.platform, integration.id, true);
            return integration;
        }
        this.options.log.error('LinkedIn integration is not in pending-action status!');
        throw new common_1.Error404(this.options.language, 'errors.linkedin.cantOnboardWrongStatus');
    }
    async linkedinConnect(segmentId) {
        const nangoId = `${segmentId}-${types_1.PlatformType.LINKEDIN}`;
        let token;
        try {
            token = await (0, getToken_1.default)(nangoId, types_1.PlatformType.LINKEDIN, this.options.log);
        }
        catch (err) {
            this.options.log.error(err, 'Error while verifying LinkedIn tenant token in Nango!');
            throw new common_1.Error400(this.options.language, 'errors.noNangoToken.message');
        }
        if (!token) {
            throw new common_1.Error400(this.options.language, 'errors.noNangoToken.message');
        }
        // fetch organizations
        let organizations;
        try {
            organizations = await (0, getOrganizations_1.getOrganizations)(nangoId, this.options.log);
        }
        catch (err) {
            this.options.log.error(err, 'Error while fetching LinkedIn organizations!');
            throw new common_1.Error400(this.options.language, 'errors.linkedin.noOrganization');
        }
        if (organizations.length === 0) {
            this.options.log.error('No organization found for LinkedIn integration!');
            throw new common_1.Error400(this.options.language, 'errors.linkedin.noOrganization');
        }
        let status = 'pending-action';
        if (organizations.length === 1) {
            status = 'in-progress';
            organizations[0].inUse = true;
        }
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        let integration;
        try {
            integration = await this.createOrUpdate({
                platform: types_1.PlatformType.LINKEDIN,
                settings: { organizations, updateMemberAttributes: true, nangoId },
                status,
            }, transaction);
            await sequelizeRepository_1.default.commitTransaction(transaction);
        }
        catch (err) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw err;
        }
        if (status === 'in-progress') {
            const emitter = await (0, queueService_1.getIntegrationRunWorkerEmitter)();
            await emitter.triggerIntegrationRun(integration.platform, integration.id, true);
        }
        return integration;
    }
    /**
     * Creates the Reddit integration and starts the onboarding
     * @param subreddits Subreddits to track
     * @returns integration object
     */
    async redditOnboard(subreddits, segmentId) {
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        let integration;
        try {
            this.options.log.info('Creating reddit integration!');
            integration = await this.createOrUpdate({
                platform: types_1.PlatformType.REDDIT,
                settings: {
                    subreddits,
                    updateMemberAttributes: true,
                    nangoId: `${segmentId}-${types_1.PlatformType.REDDIT}`,
                },
                status: 'in-progress',
            }, transaction);
            await sequelizeRepository_1.default.commitTransaction(transaction);
        }
        catch (err) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw err;
        }
        this.options.log.info('Sending reddit message to int-run-worker!');
        const emitter = await (0, queueService_1.getIntegrationRunWorkerEmitter)();
        await emitter.triggerIntegrationRun(integration.platform, integration.id, true);
        return integration;
    }
    /**
     * Adds/updates Dev.to integration
     * @param integrationData  to create the integration object
     * @returns integration object
     */
    async devtoConnectOrUpdate(integrationData) {
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        let integration;
        try {
            this.options.log.info('Creating devto integration!');
            integration = await this.createOrUpdate({
                platform: types_1.PlatformType.DEVTO,
                token: integrationData.apiKey,
                settings: {
                    users: integrationData.users,
                    organizations: integrationData.organizations,
                    articles: [],
                    updateMemberAttributes: true,
                },
                status: 'in-progress',
            }, transaction);
            await sequelizeRepository_1.default.commitTransaction(transaction);
        }
        catch (err) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw err;
        }
        this.options.log.info('Sending devto message to int-run-worker!');
        const emitter = await (0, queueService_1.getIntegrationRunWorkerEmitter)();
        await emitter.triggerIntegrationRun(integration.platform, integration.id, true);
        return integration;
    }
    /**
     * Adds/updates Git integration and syncs repositories to repositories table
     *
     * @param integrationData.remotes - Repository objects with url and optional forkedFrom (parent repo URL).
     *                                   If forkedFrom is null, existing DB value is preserved.
     * @param options - Optional repository options
     * @param sourcePlatform - If provided, mapUnifiedRepositories is skipped (caller handles it)
     * @returns Integration object or null if no remotes
     */
    async gitConnectOrUpdate(integrationData, options, sourcePlatform) {
        const stripGit = (url) => {
            if (url.endsWith('.git')) {
                return url.slice(0, -4);
            }
            return url;
        };
        const remotes = integrationData.remotes.map((remote) => ({
            url: stripGit(remote.url),
            forkedFrom: remote.forkedFrom || null,
        }));
        // Early return if no remotes to avoid unnecessary processing and SQL errors
        if (!remotes || remotes.length === 0) {
            this.options.log.warn('No remotes provided - skipping git integration update');
            return null;
        }
        const currentOptions = options || this.options;
        const existingTransaction = currentOptions.transaction || sequelizeRepository_1.default.getTransaction(currentOptions);
        const transaction = existingTransaction || (await sequelizeRepository_1.default.createTransaction(options || this.options));
        let integration;
        try {
            integration = await this.createOrUpdate({
                platform: types_1.PlatformType.GIT,
                settings: {
                    remotes: remotes.map((r) => r.url), // Store only URLs in settings for backward compatibility
                },
                status: 'done',
            }, transaction, options);
            // Check for repositories already mapped to other integrations
            const seq = sequelizeRepository_1.default.getSequelize({ ...(options || this.options), transaction });
            const urls = remotes.map((r) => r.url);
            const existingRows = await seq.query(`
          SELECT url, "gitIntegrationId" AS "integrationId" FROM repositories 
          WHERE url IN (:urls) AND "deletedAt" IS NULL
        `, {
                replacements: { urls },
                type: sequelize_1.QueryTypes.SELECT,
                transaction,
            });
            for (const row of existingRows) {
                if (row.integrationId !== integration.id) {
                    this.options.log.warn(`Trying to update git repo ${row.url} mapping with integrationId ${integration.id} but it is already mapped to integration ${row.integrationId}!`);
                    throw new common_1.Error400((options || this.options).language, 'errors.integrations.repoAlreadyMapped', row.url, integration.id, row.integrationId);
                }
            }
            const currentSegmentId = (options || this.options).currentSegments[0].id;
            // sync to public.repositories (only for direct GIT connections, other platforms handle it themselves)
            if (!sourcePlatform) {
                const mapping = remotes.reduce((acc, remote) => {
                    acc[remote.url] = currentSegmentId;
                    return acc;
                }, {});
                // Use service with transaction context so mapUnifiedRepositories joins this transaction
                const txOptions = { ...(options || this.options), transaction };
                const txService = new IntegrationService(txOptions);
                await txService.mapUnifiedRepositories(types_1.PlatformType.GIT, integration.id, mapping);
            }
            // Only commit if we created the transaction ourselves
            if (!existingTransaction) {
                await sequelizeRepository_1.default.commitTransaction(transaction);
            }
        }
        catch (err) {
            // Only rollback if we created the transaction ourselves
            if (!existingTransaction) {
                await sequelizeRepository_1.default.rollbackTransaction(transaction);
            }
            this.options.log.error(`gitConnectOrUpdate failed with error: ${err}`);
            throw err;
        }
        return integration;
    }
    /**
     * Adds/updates a mailing list (public-inbox/lore) integration and onboards
     * its lists for processing by the mailing_list_integration worker.
     *
     * @param integrationData.lists - Mailing lists to onboard (name + sourceUrl)
     * @param options - Optional repository options
     */
    async mailingListConnectOrUpdate(integrationData, options) {
        const lists = integrationData.lists || [];
        // Both current callers (mailingListAuthenticate.ts, create-mailing-list-integration.ts)
        // validate against bodySchema's `.min(1)` before reaching here, so this is an invariant
        // check, not user-facing validation — fail loudly rather than silently no-op.
        if (lists.length === 0) {
            throw new common_1.Error400(this.options.language, 'errors.validation.message');
        }
        const currentOptions = options || this.options;
        const existingTransaction = currentOptions.transaction || sequelizeRepository_1.default.getTransaction(currentOptions);
        const transaction = existingTransaction || (await sequelizeRepository_1.default.createTransaction(options || this.options));
        let integration;
        try {
            const qx = sequelizeRepository_1.default.getQueryExecutor({ ...(options || this.options), transaction });
            integration = await this.createOrUpdate({
                platform: types_1.PlatformType.MAILINGLIST,
                settings: { lists },
                status: 'done',
            }, transaction, options);
            // Serialize concurrent connects touching the same sourceUrl(s) so the
            // ownership check below and the upsert that follows it can't be
            // straddled by another transaction re-pointing ownership in between.
            await (0, mailinglist_1.lockMailingListSourceUrls)(qx, lists.map((l) => l.sourceUrl));
            const conflicts = await (0, mailinglist_1.findMailingListsOwnedByOtherIntegration)(qx, integration.id, lists);
            if (conflicts.length > 0) {
                throw new common_1.Error400(this.options.language, 'errors.mailingList.alreadyConnected', conflicts.join(', '));
            }
            const currentSegmentId = (options || this.options).currentSegments[0].id;
            await (0, mailinglist_1.upsertMailingLists)(qx, currentSegmentId, integration.id, lists);
            if (!existingTransaction) {
                await sequelizeRepository_1.default.commitTransaction(transaction);
            }
        }
        catch (err) {
            if (!existingTransaction) {
                await sequelizeRepository_1.default.rollbackTransaction(transaction);
            }
            this.options.log.error(`mailingListConnectOrUpdate failed with error: ${err}`);
            throw err;
        }
        return integration;
    }
    async atlassianAdminConnect(adminApi, organizationId) {
        const nangoPayload = {
            params: {
                organizationId,
            },
            credentials: {
                apiKey: adminApi,
            },
        };
        const adminConnectionId = await (0, nango_1.connectNangoIntegration)(nango_1.NangoIntegration.ATLASSIAN_ADMIN, nangoPayload);
        this.options.log.info(`Admin api connection created successfully ${adminConnectionId}`);
        return adminConnectionId;
    }
    /**
     * Constructs Nango connection payload for Confluence integration
     * @param integrationData: ConfluenceIntegrationData
     * @returns Object with confluenceIntegrationType and nangoPayload
     */
    static constructNangoConnectionPayload(integrationData) {
        const ATLASSIAN_CLOUD_SUFFIX = '.atlassian.net';
        const baseUrl = integrationData.settings.url.trim();
        const hostname = new URL(baseUrl).hostname;
        const isCloudUrl = hostname.endsWith(ATLASSIAN_CLOUD_SUFFIX);
        const subdomain = isCloudUrl ? hostname.split(ATLASSIAN_CLOUD_SUFFIX)[0] : null;
        if (isCloudUrl) {
            return {
                confluenceIntegrationType: nango_1.NangoIntegration.CONFLUENCE_BASIC,
                nangoPayload: {
                    params: {
                        subdomain,
                    },
                    credentials: {
                        username: integrationData.settings.username,
                        password: integrationData.settings.apiToken,
                    },
                },
            };
        }
        return {
            confluenceIntegrationType: nango_1.NangoIntegration.CONFLUENCE_DATA_CENTER,
            nangoPayload: {
                params: {
                    baseUrl,
                },
                credentials: {
                    // TODO: double check if this works for DC instance, once we have creds
                    apiKey: integrationData.settings.apiToken,
                },
            },
        };
    }
    /**
     * Updates Confluence integration
     * @param integrationData: ConfluenceIntegrationData
     * @returns integration object
     */
    async updateConfluenceIntegration(integrationData) {
        if (!integrationData.id) {
            throw new Error('Integration ID is required for update');
        }
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        let integration;
        let connectionId;
        try {
            const existingIntegration = await integrationRepository_1.default.findById(integrationData.id, this.options);
            if (!existingIntegration) {
                throw new common_1.Error404(this.options.language, 'errors.integration.notFound');
            }
            const existingSettings = existingIntegration.settings || {};
            const newSettings = integrationData.settings;
            const hasEncryptedTokenChanged = (newValue, existingEncryptedValue) => {
                if (!newValue && !existingEncryptedValue)
                    return false;
                if (!newValue || !existingEncryptedValue)
                    return true;
                return existingEncryptedValue !== (0, common_1.encryptData)(newValue);
            };
            const changes = {
                url: existingSettings.url !== newSettings.url,
                username: existingSettings.username !== newSettings.username,
                apiToken: hasEncryptedTokenChanged(newSettings.apiToken, existingSettings.apiToken),
                orgAdminApiToken: hasEncryptedTokenChanged(newSettings.orgAdminApiToken, existingSettings.orgAdminApiToken),
                orgAdminId: existingSettings.orgAdminId !== newSettings.orgAdminId,
                spaces: JSON.stringify((existingSettings.spaces || []).sort()) !==
                    JSON.stringify((newSettings.spaces || []).sort()),
            };
            // Early return if nothing changed
            const hasAnyChanges = Object.values(changes).some((changed) => changed);
            if (!hasAnyChanges) {
                await sequelizeRepository_1.default.commitTransaction(transaction);
                return existingIntegration;
            }
            connectionId = existingIntegration.id;
            let adminConnectionId = existingSettings.adminConnectionId || undefined;
            const confluenceIntegrationType = existingSettings.nangoIntegrationName;
            if (changes.orgAdminApiToken || changes.orgAdminId || !adminConnectionId) {
                adminConnectionId = await this.atlassianAdminConnect(newSettings.orgAdminApiToken, newSettings.orgAdminId);
            }
            if (changes.url || changes.username || changes.apiToken) {
                const { confluenceIntegrationType, nangoPayload } = IntegrationService.constructNangoConnectionPayload(integrationData);
                connectionId = await (0, nango_1.connectNangoIntegration)(confluenceIntegrationType, nangoPayload);
                // Delete old integration record since we have a new connectionId
                // (integration.id must match Nango connectionId for nango integrations other than GitHub)
                this.options.log.info(`Deleting old integration ${existingIntegration.id} and creating new one with ${connectionId}`);
                await integrationRepository_1.default.destroy(existingIntegration.id, {
                    ...this.options,
                    transaction,
                });
                await (0, nango_1.deleteNangoConnection)(confluenceIntegrationType, existingIntegration.id);
            }
            await (0, nango_1.setNangoMetadata)(nango_1.NangoIntegration.CONFLUENCE_BASIC, connectionId, {
                spaceKeysToSync: newSettings.spaces,
                adminApiConnection: adminConnectionId,
            });
            integration = await this.createOrUpdate({
                id: connectionId,
                platform: types_1.PlatformType.CONFLUENCE,
                settings: {
                    ...newSettings,
                    // NOTE: If you add/remove/modify encrypted fields here, remember to update
                    // decryptIntegrationSettings() in the query() method to decrypt them
                    apiToken: (0, common_1.encryptData)(newSettings.apiToken),
                    orgAdminApiToken: (0, common_1.encryptData)(newSettings.orgAdminApiToken),
                    orgAdminId: newSettings.orgAdminId,
                    nangoIntegrationName: confluenceIntegrationType,
                    adminConnectionId,
                },
                status: 'done',
            }, transaction);
            await (0, nango_1.startNangoSync)(confluenceIntegrationType, connectionId);
            await sequelizeRepository_1.default.commitTransaction(transaction);
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            if (error instanceof TypeError && error.message.includes('Invalid URL')) {
                this.options.log.error(`Invalid url: ${integrationData.settings.url}`);
                throw new common_1.Error400(this.options.language, 'errors.confluence.invalidUrl');
            }
            if (error && error.message.includes('credentials')) {
                throw new common_1.Error400(this.options.language, 'errors.confluence.invalidCredentials');
            }
            throw error;
        }
        return integration;
    }
    /**
     * Connects a new Confluence integration
     * @param integrationData: ConfluenceIntegrationData
     * @returns integration object
     */
    async connectConfluenceIntegration(integrationData) {
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        let integration;
        let connectionId;
        try {
            const adminConnectionId = await this.atlassianAdminConnect(integrationData.settings.orgAdminApiToken, integrationData.settings.orgAdminId);
            const { confluenceIntegrationType, nangoPayload } = IntegrationService.constructNangoConnectionPayload(integrationData);
            this.options.log.info(`conflunece integration type determined: ${confluenceIntegrationType}, starting nango connection...`);
            connectionId = await (0, nango_1.connectNangoIntegration)(confluenceIntegrationType, nangoPayload);
            await (0, nango_1.setNangoMetadata)(nango_1.NangoIntegration.CONFLUENCE_BASIC, connectionId, {
                spaceKeysToSync: integrationData.settings.spaces,
                adminApiConnection: adminConnectionId,
            });
            integration = await this.createOrUpdate({
                id: connectionId,
                platform: types_1.PlatformType.CONFLUENCE,
                settings: {
                    ...integrationData.settings,
                    // NOTE: If you add/remove/modify encrypted fields here, remember to update
                    // decryptIntegrationSettings() in the query() method to decrypt them
                    apiToken: (0, common_1.encryptData)(integrationData.settings.apiToken),
                    orgAdminApiToken: (0, common_1.encryptData)(integrationData.settings.orgAdminApiToken),
                    orgAdminId: integrationData.settings.orgAdminId,
                    nangoIntegrationName: confluenceIntegrationType,
                    adminConnectionId,
                },
                status: 'done',
            }, transaction);
            await (0, nango_1.startNangoSync)(nango_1.NangoIntegration.CONFLUENCE_BASIC, connectionId);
            await sequelizeRepository_1.default.commitTransaction(transaction);
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            if (error instanceof TypeError && error.message.includes('Invalid URL')) {
                this.options.log.error(`Invalid url: ${integrationData.settings.url}`);
                throw new common_1.Error400(this.options.language, 'errors.confluence.invalidUrl');
            }
            if (error && error.message.includes('credentials')) {
                throw new common_1.Error400(this.options.language, 'errors.confluence.invalidCredentials');
            }
            throw error;
        }
        return integration;
    }
    /**
     * Adds/updates Gerrit integration
     * @param integrationData  to create the integration object
     * @returns integration object
     */
    async gerritConnectOrUpdate(integrationData) {
        var _a, _b, _c;
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        let integration;
        let connectionId;
        try {
            const orgUrl = integrationData.remote.orgURL;
            let host;
            if (orgUrl.startsWith('https://')) {
                host = orgUrl.slice(8);
            }
            else if (orgUrl.startsWith('http://')) {
                host = orgUrl.slice(7);
            }
            else {
                host = orgUrl;
            }
            const stripGit = (url) => {
                if (url.endsWith('.git')) {
                    return url.slice(0, -4);
                }
                return url;
            };
            // Build full repository URLs from orgURL and repo names
            const currentSegmentId = this.options.currentSegments[0].id;
            let remotes = integrationData.remote.repoNames.map((repoName) => {
                const fullUrl = stripGit(`${integrationData.remote.orgURL}/${repoName}`);
                return { url: fullUrl, forkedFrom: null };
            });
            // Check for conflicts with existing Gerrit integrations
            for (const remote of remotes) {
                const existingGerritIntegrations = await this.options.database.sequelize.query(`SELECT id, settings FROM integrations 
           WHERE platform = 'gerrit' AND "deletedAt" IS NULL`, {
                    type: sequelize_1.QueryTypes.SELECT,
                    transaction,
                });
                for (const existingIntegration of existingGerritIntegrations) {
                    const settings = existingIntegration.settings;
                    if (((_a = settings === null || settings === void 0 ? void 0 : settings.remote) === null || _a === void 0 ? void 0 : _a.repoNames) && ((_b = settings === null || settings === void 0 ? void 0 : settings.remote) === null || _b === void 0 ? void 0 : _b.orgURL)) {
                        const existingRemotes = settings.remote.repoNames.map((repoName) => stripGit(`${settings.remote.orgURL}/${repoName}`));
                        if (existingRemotes.includes(remote.url)) {
                            this.options.log.warn(`Trying to map Gerrit repository ${remote.url} with integrationId ${(integration === null || integration === void 0 ? void 0 : integration.id) || connectionId} but it is already mapped to integration ${existingIntegration.id}!`);
                            throw new common_1.Error400(this.options.language, 'errors.integrations.repoAlreadyMapped', remote.url, (integration === null || integration === void 0 ? void 0 : integration.id) || connectionId, existingIntegration.id);
                        }
                    }
                }
            }
            const res = await IntegrationService.getGerritServerRepos(orgUrl);
            if (integrationData.remote.enableAllRepos) {
                integrationData.remote.repoNames = res;
            }
            // Rebuild remotes after enableAllRepos may have updated repoNames
            remotes = integrationData.remote.repoNames.map((repoName) => {
                const fullUrl = stripGit(`${integrationData.remote.orgURL}/${repoName}`);
                return { url: fullUrl, forkedFrom: null };
            });
            connectionId = await (0, nango_1.createNangoConnection)(nango_1.NangoIntegration.GERRIT, {
                params: {
                    host,
                },
            });
            if (integrationData.remote.repoNames.length > 0) {
                await (0, nango_1.setNangoMetadata)(nango_1.NangoIntegration.GERRIT, connectionId, {
                    repos: integrationData.remote.repoNames,
                });
            }
            integration = await this.createOrUpdate({
                id: connectionId,
                platform: types_1.PlatformType.GERRIT,
                settings: {
                    remote: integrationData.remote,
                },
                status: 'done',
            }, transaction);
            if (integrationData.remote.enableGit) {
                const segmentOptions = {
                    ...this.options,
                    transaction,
                    currentSegments: [
                        {
                            ...this.options.currentSegments[0],
                        },
                    ],
                };
                // Check if git integration already exists and merge remotes
                let isGitIntegrationConfigured = false;
                try {
                    await integrationRepository_1.default.findByPlatform(types_1.PlatformType.GIT, segmentOptions);
                    isGitIntegrationConfigured = true;
                }
                catch (err) {
                    isGitIntegrationConfigured = false;
                }
                if (isGitIntegrationConfigured) {
                    const gitInfo = await this.gitGetRemotes(segmentOptions);
                    const gitRemotes = ((_c = gitInfo[currentSegmentId]) === null || _c === void 0 ? void 0 : _c.remotes) || [];
                    const allUrls = Array.from(new Set([...gitRemotes, ...remotes.map((r) => r.url)]));
                    await this.gitConnectOrUpdate({
                        remotes: allUrls.map((url) => ({ url, forkedFrom: null })),
                    }, segmentOptions, types_1.PlatformType.GERRIT);
                }
                else {
                    await this.gitConnectOrUpdate({
                        remotes,
                    }, segmentOptions, types_1.PlatformType.GERRIT);
                }
            }
            // sync to public.repositories
            const mapping = remotes.reduce((acc, remote) => {
                acc[remote.url] = currentSegmentId;
                return acc;
            }, {});
            const txOptions = { ...this.options, transaction };
            const txService = new IntegrationService(txOptions);
            await txService.mapUnifiedRepositories(types_1.PlatformType.GERRIT, integration.id, mapping);
            await (0, nango_1.startNangoSync)(nango_1.NangoIntegration.GERRIT, connectionId);
            await sequelizeRepository_1.default.commitTransaction(transaction);
        }
        catch (err) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            if (connectionId) {
                await (0, nango_1.deleteNangoConnection)(nango_1.NangoIntegration.GERRIT, connectionId);
            }
            throw err;
        }
        return integration;
    }
    static async getGerritServerRepos(serverURL) {
        try {
            const result = await axios_1.default.get(`${serverURL}/projects/`, {});
            const str = result.data.replace(")]}'\n", '');
            const data = JSON.parse(str);
            const repos = Object.keys(data).filter((key) => key !== '.github' && key !== 'All-Projects' && key !== 'All-Users');
            return repos;
        }
        catch (error) {
            if (error.response && error.response.status !== 404) {
                throw new common_1.Error404('Error in getGerritServerRepos:', error);
            }
        }
        return [];
    }
    /**
     * Get all remotes for the Git integration, by segment
     * @returns Remotes for the Git integration
     */
    async gitGetRemotes(options) {
        try {
            const integrations = await integrationRepository_1.default.findAllByPlatform(types_1.PlatformType.GIT, options || this.options);
            return integrations.reduce((acc, integration) => {
                const { id, segmentId, settings: { remotes }, } = integration;
                acc[segmentId] = { remotes, integrationId: id };
                return acc;
            }, {});
        }
        catch (err) {
            throw new common_1.Error400(this.options.language, 'errors.git.noIntegration');
        }
    }
    /**
     * Adds/updates Hacker News integration
     * @param integrationData  to create the integration object
     * @returns integration object
     */
    async hackerNewsConnectOrUpdate(integrationData) {
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        let integration;
        try {
            integration = await this.createOrUpdate({
                platform: types_1.PlatformType.HACKERNEWS,
                settings: {
                    keywords: integrationData.keywords,
                    urls: integrationData.urls,
                    updateMemberAttributes: true,
                },
                status: 'in-progress',
            }, transaction);
            await sequelizeRepository_1.default.commitTransaction(transaction);
        }
        catch (err) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw err;
        }
        this.options.log.info('Sending HackerNews message to int-run-worker!');
        const emitter = await (0, queueService_1.getIntegrationRunWorkerEmitter)();
        this.options.log.info('Got emmiter succesfully! Triggering integration run!');
        await emitter.triggerIntegrationRun(integration.platform, integration.id, true);
        return integration;
    }
    /**
     * Adds/updates slack integration
     * @param integrationData to create the integration object
     * @returns integration object
     */
    async slackCallback(integrationData) {
        integrationData.settings = integrationData.settings || {};
        integrationData.settings.updateMemberAttributes = true;
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        let integration;
        try {
            this.options.log.info('Creating Slack integration!');
            integration = await this.createOrUpdate({
                platform: types_1.PlatformType.SLACK,
                ...integrationData,
                status: 'in-progress',
            }, transaction);
            await sequelizeRepository_1.default.commitTransaction(transaction);
        }
        catch (err) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw err;
        }
        this.options.log.info('Sending Slack message to int-run-worker!');
        const isOnboarding = !('channels' in integration.settings);
        const emitter = await (0, queueService_1.getIntegrationRunWorkerEmitter)();
        await emitter.triggerIntegrationRun(integration.platform, integration.id, isOnboarding);
        return integration;
    }
    /**
     * Adds/updates twitter integration
     * @param integrationData to create the integration object
     * @returns integration object
     */
    async twitterCallback(integrationData) {
        const { profileId, token, refreshToken } = integrationData;
        const hashtags = !integrationData.hashtags || integrationData.hashtags === '' ? [] : integrationData.hashtags;
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        let integration;
        try {
            integration = await this.createOrUpdate({
                platform: types_1.PlatformType.TWITTER,
                integrationIdentifier: profileId,
                token,
                refreshToken,
                status: 'in-progress',
                settings: {
                    followers: [],
                    hashtags: typeof hashtags === 'string' ? hashtags.split(',') : hashtags,
                    updateMemberAttributes: true,
                },
            }, transaction);
            await sequelizeRepository_1.default.commitTransaction(transaction);
        }
        catch (err) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw err;
        }
        this.options.log.info('Sending Twitter message to int-run-worker!');
        const emitter = await (0, queueService_1.getIntegrationRunWorkerEmitter)();
        await emitter.triggerIntegrationRun(integration.platform, integration.id, true);
        return integration;
    }
    /**
     * Adds/updates Stack Overflow integration
     * @param integrationData  to create the integration object
     * @returns integration object
     */
    async stackOverflowConnectOrUpdate(integrationData) {
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        let integration;
        try {
            this.options.log.info('Creating Stack Overflow integration!');
            integration = await this.createOrUpdate({
                platform: types_1.PlatformType.STACKOVERFLOW,
                settings: {
                    tags: integrationData.tags,
                    keywords: integrationData.keywords,
                    updateMemberAttributes: true,
                    nangoId: `${integrationData.segments[0]}-${types_1.PlatformType.STACKOVERFLOW}`,
                },
                status: 'in-progress',
            }, transaction);
            await sequelizeRepository_1.default.commitTransaction(transaction);
        }
        catch (err) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw err;
        }
        this.options.log.info('Sending StackOverflow message to int-run-worker!');
        const emitter = await (0, queueService_1.getIntegrationRunWorkerEmitter)();
        await emitter.triggerIntegrationRun(integration.platform, integration.id, true);
        return integration;
    }
    /**
     * Adds/updates Discourse integration
     * @param integrationData  to create the integration object
     * @returns integration object
     */
    async discourseConnectOrUpdate(integrationData) {
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        let integration;
        try {
            integration = await this.createOrUpdate({
                platform: types_1.PlatformType.DISCOURSE,
                settings: {
                    apiKey: integrationData.apiKey,
                    apiUsername: integrationData.apiUsername,
                    forumHostname: integrationData.forumHostname,
                    webhookSecret: integrationData.webhookSecret,
                    updateMemberAttributes: true,
                },
                status: 'in-progress',
            }, transaction);
            await sequelizeRepository_1.default.commitTransaction(transaction);
        }
        catch (err) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw err;
        }
        this.options.log.info('Sending Discourse message to int-run-worker!');
        const emitter = await (0, queueService_1.getIntegrationRunWorkerEmitter)();
        await emitter.triggerIntegrationRun(integration.platform, integration.id, true);
        return integration;
    }
    async groupsioConnectOrUpdate(integrationData) {
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        let integration;
        // integration data should have the following fields
        // email, token, array of groups
        // we shouldn't store password and 2FA token in the database
        // user should update them every time thety change something
        try {
            this.options.log.info('Creating Groups.io integration!');
            const encryptedPassword = (0, common_1.encryptData)(integrationData.password);
            integration = await this.createOrUpdate({
                platform: types_1.PlatformType.GROUPSIO,
                settings: {
                    email: integrationData.email,
                    token: integrationData.token,
                    tokenExpiry: integrationData.tokenExpiry,
                    password: encryptedPassword,
                    groups: integrationData.groups,
                    autoImports: integrationData.autoImports,
                    updateMemberAttributes: true,
                },
                status: 'in-progress',
            }, transaction);
            await sequelizeRepository_1.default.commitTransaction(transaction);
        }
        catch (err) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw err;
        }
        this.options.log.info('Sending Groups.io message to int-run-worker!');
        const emitter = await (0, queueService_1.getIntegrationRunWorkerEmitter)();
        await emitter.triggerIntegrationRun(integration.platform, integration.id, true);
        return integration;
    }
    // we need to get all user groups and subgroups he has access to
    // groups all sub groups based on a group name
    // also we would need to autoimport new groups and add them to settings - either cron job or during incremental sync
    // we might need to change settings structure of already existing integrations
    async groupsioGetToken(data) {
        const config = {
            method: 'post',
            url: 'https://groups.io/api/v1/login',
            params: {
                email: data.email,
                password: data.password,
                twofactor: data.twoFactorCode,
            },
            headers: {
                'Content-Type': 'application/json',
            },
        };
        let response;
        try {
            response = await (0, axios_1.default)(config);
            // we need to get cookie from the response
            const cookie = response.headers['set-cookie'][0].split(';')[0];
            const cookieExpiryString = response.headers['set-cookie'][0]
                .split(';')[3]
                .split('=')[1];
            const cookieExpiry = (0, moment_1.default)(cookieExpiryString).format('YYYY-MM-DD HH:mm:ss.sss Z');
            return {
                groupsioCookie: cookie,
                groupsioCookieExpiry: cookieExpiry,
            };
        }
        catch (error) {
            this.options.log.error(error.response.data, 'Error while login into GroupsIo!');
            const errorType = String(error.response.data.type);
            const isTwoFactorRequired = errorType.includes('two_factor_required') || errorType.includes('2nd_factor_required');
            if (isTwoFactorRequired) {
                throw new common_1.Error400(this.options.language, 'errors.groupsio.isTwoFactorRequired');
            }
            const invalidCredentials = errorType.includes('invalid password') || errorType.includes('invalid email');
            if (invalidCredentials)
                throw new common_1.Error400(this.options.language, 'errors.groupsio.invalidCredentials');
            const invalid2FA = errorType.includes('2nd_factor_wrong');
            if (invalid2FA)
                throw new common_1.Error400(this.options.language, 'errors.groupsio.invalid2FA');
            throw error;
        }
    }
    async groupsioVerifyGroup(data) {
        var _a, _b;
        const groupName = data.groupName;
        const config = {
            method: 'post',
            url: `https://groups.io/api/v1/gettopics?group_name=${encodeURIComponent(groupName)}`,
            headers: {
                'Content-Type': 'application/json',
                Cookie: data.cookie,
            },
        };
        let response;
        try {
            response = await (0, axios_1.default)(config);
            return {
                group: (_b = (_a = response === null || response === void 0 ? void 0 : response.data) === null || _a === void 0 ? void 0 : _a.data) === null || _b === void 0 ? void 0 : _b.group_id,
            };
        }
        catch (err) {
            this.options.log.error('Error verifying groups.io group.', err);
            throw new common_1.Error400(this.options.language, 'errors.groupsio.invalidGroup');
        }
    }
    async groupsioGetUserSubscriptions({ cookie }) {
        try {
            const subscriptions = await (0, getUserSubscriptions_1.getUserSubscriptions)(cookie);
            return subscriptions;
        }
        catch (error) {
            this.options.log.error('Error fetching groups.io user subscriptions:', error);
            throw new common_1.Error400(this.options.language, 'errors.groupsio.fetchSubscriptionsFailed');
        }
    }
    /**
     * Constructs Nango connection payload for Jira integration
     * @param integrationData: JiraIntegrationData
     * @returns Object with jiraIntegrationType and nangoPayload
     */
    static constructJiraNangoConnectionPayload(integrationData) {
        const ATLASSIAN_CLOUD_SUFFIX = '.atlassian.net';
        const baseUrl = integrationData.url.trim();
        const hostname = new URL(baseUrl).hostname;
        const isCloudUrl = hostname.endsWith(ATLASSIAN_CLOUD_SUFFIX);
        const subdomain = isCloudUrl ? hostname.split(ATLASSIAN_CLOUD_SUFFIX)[0] : null;
        if (isCloudUrl && integrationData.username && integrationData.apiToken) {
            return {
                jiraIntegrationType: nango_1.NangoIntegration.JIRA_CLOUD_BASIC,
                nangoPayload: {
                    params: {
                        subdomain,
                    },
                    credentials: {
                        username: integrationData.username,
                        password: integrationData.apiToken,
                    },
                },
            };
        }
        if (!isCloudUrl && integrationData.username && integrationData.apiToken) {
            return {
                jiraIntegrationType: nango_1.NangoIntegration.JIRA_DATA_CENTER_BASIC,
                nangoPayload: {
                    params: {
                        baseUrl,
                    },
                    credentials: {
                        username: integrationData.username,
                        password: integrationData.apiToken,
                    },
                },
            };
        }
        return {
            jiraIntegrationType: nango_1.NangoIntegration.JIRA_DATA_CENTER_API_KEY,
            nangoPayload: {
                params: {
                    baseUrl,
                },
                credentials: {
                    apiKey: integrationData.personalAccessToken,
                },
            },
        };
    }
    /**
     * Updates Jira integration
     * @param integrationData: JiraIntegrationData
     * @returns integration object
     */
    async updateJiraIntegration(integrationData) {
        var _a;
        if (!integrationData.id) {
            throw new Error('Integration ID is required for update');
        }
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        let integration;
        let connectionId;
        try {
            const existingIntegration = await integrationRepository_1.default.findById(integrationData.id, this.options);
            if (!existingIntegration) {
                throw new common_1.Error404(this.options.language, 'errors.integration.notFound');
            }
            const existingSettings = existingIntegration.settings || {};
            const existingAuth = existingSettings.auth || {};
            const newAuth = {
                username: integrationData.username,
                personalAccessToken: integrationData.personalAccessToken,
                apiToken: integrationData.apiToken,
            };
            const hasEncryptedTokenChanged = (newValue, existingEncryptedValue) => {
                if (!newValue && !existingEncryptedValue)
                    return false;
                if (!newValue || !existingEncryptedValue)
                    return true;
                return existingEncryptedValue !== (0, common_1.encryptData)(newValue);
            };
            const changes = {
                url: existingSettings.url !== integrationData.url,
                username: existingAuth.username !== newAuth.username,
                apiToken: hasEncryptedTokenChanged(newAuth.apiToken, existingAuth.apiToken),
                personalAccessToken: hasEncryptedTokenChanged(newAuth.personalAccessToken, existingAuth.personalAccessToken),
                projects: JSON.stringify((existingSettings.projects || []).sort()) !==
                    JSON.stringify((integrationData.projects || []).sort()),
            };
            // Early return if nothing changed
            const hasAnyChanges = Object.values(changes).some((changed) => changed);
            if (!hasAnyChanges) {
                await sequelizeRepository_1.default.commitTransaction(transaction);
                return existingIntegration;
            }
            connectionId = existingIntegration.id;
            let jiraIntegrationType = existingSettings.nangoIntegrationName;
            const credentialsChanged = changes.url || changes.username || changes.apiToken || changes.personalAccessToken;
            if (credentialsChanged) {
                // credentials changed, need to create a new nango connection
                const { jiraIntegrationType: newType, nangoPayload } = IntegrationService.constructJiraNangoConnectionPayload(integrationData);
                jiraIntegrationType = newType;
                this.options.log.info(`jira integration type determined: ${jiraIntegrationType}, starting nango connection...`);
                connectionId = await (0, nango_1.connectNangoIntegration)(jiraIntegrationType, nangoPayload);
                // Delete old integration record since we have a new connectionId
                // (integration.id must match Nango connectionId for nango integrations other than GitHub)
                this.options.log.info(`Deleting old integration ${existingIntegration.id} and creating new one with ${connectionId}`);
                await integrationRepository_1.default.destroy(existingIntegration.id, {
                    ...this.options,
                    transaction,
                });
                await (0, nango_1.deleteNangoConnection)(jiraIntegrationType, existingIntegration.id);
            }
            await (0, nango_1.setNangoMetadata)(jiraIntegrationType, connectionId, {
                projectIdsToSync: integrationData.projects.map((project) => project.toUpperCase()),
            });
            integration = await this.createOrUpdate({
                id: connectionId,
                platform: types_1.PlatformType.JIRA,
                settings: {
                    url: integrationData.url,
                    auth: {
                        username: integrationData.username,
                        // NOTE: If you add/remove/modify encrypted fields here, remember to update
                        // decryptIntegrationSettings() in the query() method to decrypt them
                        personalAccessToken: integrationData.personalAccessToken
                            ? (0, common_1.encryptData)(integrationData.personalAccessToken)
                            : null,
                        apiToken: integrationData.apiToken ? (0, common_1.encryptData)(integrationData.apiToken) : null,
                    },
                    nangoIntegrationName: jiraIntegrationType,
                    projects: ((_a = integrationData.projects) === null || _a === void 0 ? void 0 : _a.map((project) => project.toUpperCase())) || [],
                },
                status: 'done',
            }, transaction);
            await (0, nango_1.startNangoSync)(jiraIntegrationType, connectionId);
            await sequelizeRepository_1.default.commitTransaction(transaction);
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            if (error instanceof TypeError && error.message.includes('Invalid URL')) {
                this.options.log.error(`Invalid url: ${integrationData.url}`);
                throw new common_1.Error400(this.options.language, 'errors.jira.invalidUrl');
            }
            if (error && error.message.includes('credentials')) {
                throw new common_1.Error400(this.options.language, 'errors.jira.invalidCredentials');
            }
            throw error;
        }
        return integration;
    }
    /**
     * Connects a new Jira integration
     * @param integrationData: JiraIntegrationData
     * @returns integration object
     * @remarks
     * Supports the following authentication methods:
     * 1. Jira Cloud (basic auth): Requires URL, username, and password (API key)
     * 2. Jira Data Center (PAT): Requires URL and optionally a Personal Access Token
     * 3. Jira Data Center (basic auth): Requires URL, username, and password (API key)
     */
    async connectJiraIntegration(integrationData) {
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        let integration;
        let connectionId;
        try {
            const { jiraIntegrationType, nangoPayload } = IntegrationService.constructJiraNangoConnectionPayload(integrationData);
            this.options.log.info(`jira integration type determined: ${jiraIntegrationType}, starting nango connection...`);
            connectionId = await (0, nango_1.connectNangoIntegration)(jiraIntegrationType, nangoPayload);
            if (integrationData.projects && integrationData.projects.length > 0) {
                await (0, nango_1.setNangoMetadata)(jiraIntegrationType, connectionId, {
                    projectIdsToSync: integrationData.projects.map((project) => project.toUpperCase()),
                });
            }
            integration = await this.createOrUpdate({
                id: connectionId,
                platform: types_1.PlatformType.JIRA,
                settings: {
                    url: integrationData.url,
                    auth: {
                        username: integrationData.username,
                        // NOTE: If you add/remove/modify encrypted fields here, remember to update
                        // decryptIntegrationSettings() in the query() method to decrypt them
                        personalAccessToken: integrationData.personalAccessToken
                            ? (0, common_1.encryptData)(integrationData.personalAccessToken)
                            : null,
                        apiToken: integrationData.apiToken ? (0, common_1.encryptData)(integrationData.apiToken) : null,
                    },
                    nangoIntegrationName: jiraIntegrationType,
                    projects: integrationData.projects.map((project) => project.toUpperCase()),
                },
                status: 'done',
            }, transaction);
            await (0, nango_1.startNangoSync)(jiraIntegrationType, connectionId);
            await sequelizeRepository_1.default.commitTransaction(transaction);
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            if (error instanceof TypeError && error.message.includes('Invalid URL')) {
                this.options.log.error(`Invalid url: ${integrationData.url}`);
                throw new common_1.Error400(this.options.language, 'errors.jira.invalidUrl');
            }
            if (error && error.message.includes('credentials')) {
                throw new common_1.Error400(this.options.language, 'errors.jira.invalidCredentials');
            }
            throw error;
        }
        return integration;
    }
    async getIntegrationProgress(integrationId) {
        var _a, _b, _c, _d, _e;
        const integration = await this.findById(integrationId);
        const segments = sequelizeRepository_1.default.getCurrentSegments(this.options);
        // special case for github
        if (integration.platform === types_1.PlatformType.GITHUB ||
            integration.platform === types_1.PlatformType.GITHUB_NANGO) {
            if (integration.status !== 'in-progress') {
                return {
                    type: 'github',
                    segmentId: integration.segmentId,
                    segmentName: (_a = segments.find((s) => s.id === integration.segmentId)) === null || _a === void 0 ? void 0 : _a.name,
                    platform: integration.platform,
                    reportStatus: 'integration-is-not-in-progress',
                };
            }
            const githubToken = await (0, common_services_1.getGithubInstallationToken)();
            const qx = sequelizeRepository_1.default.getQueryExecutor(this.options);
            const repos = await (0, repositories_1.getReposForGithubIntegration)(qx, integrationId);
            const githubRepos = await (0, repositories_1.getRepositoriesBySourceIntegrationId)(qx, integrationId);
            const mappedSegments = githubRepos.map((repo) => repo.segmentId);
            const cacheRemote = new redis_1.RedisCache('github-progress-remote', this.options.redis, this.options.log);
            const cacheDb = new redis_1.RedisCache('github-progress-db', this.options.redis, this.options.log);
            const getRemoteCachedStats = async (key) => {
                let cachedStats;
                cachedStats = await cacheRemote.get(key);
                if (!cachedStats) {
                    cachedStats = await (0, getRemoteStats_1.getGitHubRemoteStats)(githubToken, repos);
                    // cache for 2 hours
                    await cacheRemote.set(key, JSON.stringify(cachedStats), 2 * 60 * 60);
                }
                else {
                    cachedStats = JSON.parse(cachedStats);
                }
                return cachedStats;
            };
            const getRemoteStatsOrExitEarly = async (key, maxSeconds = 1) => {
                const result = await Promise.race([
                    getRemoteCachedStats(key),
                    new Promise((resolve) => setTimeout(() => resolve(-1), maxSeconds * 1000)),
                ]);
                if (result === -1) {
                    return undefined;
                }
                return result;
            };
            const getDbCachedStats = async (key) => {
                let cachedStats;
                cachedStats = await cacheDb.get(key);
                if (!cachedStats) {
                    const segments = Array.from(new Set([...(integration.segmentId ? [integration.segmentId] : []), ...mappedSegments]));
                    this.options.log.debug(`Evaluating cache for repos: ${repos.map((r) => r.name).join(',')} and segments: ${segments}`);
                    cachedStats = await integrationProgressRepository_1.default.getDbStatsForGithub();
                    this.options.log.debug(`Caching data: ${JSON.stringify(cachedStats)}`);
                    // cache for 1 minute
                    await cacheDb.set(key, JSON.stringify(cachedStats), 60);
                }
                else {
                    cachedStats = JSON.parse(cachedStats);
                }
                return cachedStats;
            };
            const getDbStatsOrExitEarly = async (key, maxSeconds = 1) => {
                const result = await Promise.race([
                    getDbCachedStats(key),
                    new Promise((resolve) => setTimeout(() => resolve(-1), maxSeconds * 1000)),
                ]);
                if (result === -1) {
                    return undefined;
                }
                return result;
            };
            const [remoteStats, dbStats] = await Promise.all([
                getRemoteStatsOrExitEarly(integrationId),
                getDbStatsOrExitEarly(integrationId),
            ]);
            this.options.log.debug('Remote stats:', remoteStats);
            this.options.log.debug('DB stats:', dbStats);
            // this to prevent too long waiting time
            if (remoteStats === undefined || dbStats === undefined) {
                return {
                    type: 'github',
                    segmentId: integration.segmentId,
                    segmentName: (_b = segments.find((s) => s.id === integration.segmentId)) === null || _b === void 0 ? void 0 : _b.name,
                    platform: integration.platform,
                    reportStatus: 'calculating',
                };
            }
            const normailzeStats = (db, remote) => {
                if (remote === 0)
                    return 100;
                return Math.max(Math.min(Math.round((db / remote) * 100), 100), 0);
            };
            const calculateStatus = (db, remote) => {
                if (remote === 0)
                    return 'ok';
                if (db >= remote)
                    return 'ok';
                if (Math.abs(db - remote) / remote <= 0.02)
                    return 'ok';
                return 'in-progress';
            };
            const calculateMessage = (db, remote, entity) => {
                if (remote === 0)
                    return `0 ${entity} synced`;
                if (db >= remote)
                    return `${remote.toLocaleString()} ${entity} synced`;
                if (Math.abs(db - remote) / remote <= 0.02)
                    return `${db.toLocaleString()} ${entity} synced`;
                return `${db.toLocaleString()} out of ${remote.toLocaleString()} ${entity} synced`;
            };
            const remainingStreamsCount = await integrationProgressRepository_1.default.getPendingStreamsCount(integrationId, this.options);
            const progress = {
                type: 'github',
                segmentId: integration.segmentId,
                segmentName: (_c = segments.find((s) => s.id === integration.segmentId)) === null || _c === void 0 ? void 0 : _c.name,
                platform: integration.platform,
                reportStatus: 'ok',
                data: {
                    forks: {
                        db: dbStats.forks,
                        remote: remoteStats.forks,
                        status: calculateStatus(dbStats.forks, remoteStats.forks),
                        message: calculateMessage(dbStats.forks, remoteStats.forks, 'forks'),
                        percentage: normailzeStats(dbStats.forks, remoteStats.forks),
                    },
                    pullRequests: {
                        db: dbStats.totalPRs,
                        remote: remoteStats.totalPRs,
                        status: calculateStatus(dbStats.totalPRs, remoteStats.totalPRs),
                        message: calculateMessage(dbStats.totalPRs, remoteStats.totalPRs, 'pull requests'),
                        percentage: normailzeStats(dbStats.totalPRs, remoteStats.totalPRs),
                    },
                    issues: {
                        db: dbStats.totalIssues,
                        remote: remoteStats.totalIssues,
                        status: calculateStatus(dbStats.totalIssues, remoteStats.totalIssues),
                        message: calculateMessage(dbStats.totalIssues, remoteStats.totalIssues, 'issues'),
                        percentage: normailzeStats(dbStats.totalIssues, remoteStats.totalIssues),
                    },
                    stars: {
                        db: dbStats.stars,
                        remote: remoteStats.stars,
                        status: calculateStatus(dbStats.stars, remoteStats.stars),
                        message: calculateMessage(dbStats.stars, remoteStats.stars, 'stars'),
                        percentage: normailzeStats(dbStats.stars, remoteStats.stars),
                    },
                    other: {
                        db: remainingStreamsCount,
                        status: remainingStreamsCount > 0 ? 'in-progress' : 'ok',
                        message: remainingStreamsCount > 0
                            ? `${remainingStreamsCount} data streams are being processed...`
                            : 'All data streams are processed',
                    },
                },
            };
            return progress;
        }
        if (integration.status !== 'in-progress') {
            return {
                type: 'github',
                segmentId: integration.segmentId,
                segmentName: (_d = segments.find((s) => s.id === integration.segmentId)) === null || _d === void 0 ? void 0 : _d.name,
                platform: integration.platform,
                reportStatus: 'integration-is-not-in-progress',
            };
        }
        const remainingStreamsCount = await integrationProgressRepository_1.default.getPendingStreamsCount(integrationId, this.options);
        const progress = {
            type: 'other',
            platform: integration.platform,
            reportStatus: 'ok',
            segmentId: integration.segmentId,
            segmentName: (_e = segments.find((s) => s.id === integration.segmentId)) === null || _e === void 0 ? void 0 : _e.name,
            data: {
                other: {
                    db: remainingStreamsCount,
                    status: remainingStreamsCount > 0 ? 'in-progress' : 'ok',
                    message: remainingStreamsCount > 0
                        ? `${remainingStreamsCount} data streams are being processed...`
                        : 'All data streams are processed',
                },
            },
        };
        return progress;
    }
    async getIntegrationProgressList() {
        const currentSegments = sequelizeRepository_1.default.getCurrentSegments(this.options);
        if (currentSegments.length === 1) {
            const integrationIds = await integrationProgressRepository_1.default.getAllIntegrationsInProgressForSegment(this.options);
            return Promise.all(integrationIds.map((id) => this.getIntegrationProgress(id)));
        }
        const integrationIds = await integrationProgressRepository_1.default.getAllIntegrationsInProgressForMultipleSegments(this.options);
        return Promise.all(integrationIds.map((id) => this.getIntegrationProgress(id)));
    }
    async getIntegrationMappedRepos(segmentId) {
        const qx = sequelizeRepository_1.default.getQueryExecutor(this.options);
        const githubPlatforms = [types_1.PlatformType.GITHUB, types_1.PlatformType.GITHUB_NANGO];
        const hasRepos = await (0, segments_1.hasMappedRepos)(qx, segmentId, githubPlatforms);
        if (!hasRepos) {
            return null;
        }
        const [githubMappedRepos, githubNangoMappedRepos, gitlabMappedRepos] = await Promise.all([
            (0, segments_1.getMappedRepos)(qx, segmentId, types_1.PlatformType.GITHUB),
            (0, segments_1.getMappedRepos)(qx, segmentId, types_1.PlatformType.GITHUB_NANGO),
            (0, segments_1.getMappedRepos)(qx, segmentId, types_1.PlatformType.GITLAB),
        ]);
        const projects = await (0, segments_1.getMappedAllWithSegmentName)(qx, segmentId, githubPlatforms);
        return {
            projects,
            repositories: [...githubMappedRepos, ...githubNangoMappedRepos, ...gitlabMappedRepos],
        };
    }
    async gitlabConnect(code) {
        var _a, _b, _c, _d;
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        let integration;
        try {
            // Exchange the code for access token and refresh token
            const tokenResponse = await axios_1.default.post('https://gitlab.com/oauth/token', {
                client_id: index_1.GITLAB_CONFIG.clientId,
                client_secret: index_1.GITLAB_CONFIG.clientSecret,
                code,
                grant_type: 'authorization_code',
                redirect_uri: index_1.GITLAB_CONFIG.callbackUrl,
            });
            const { access_token: accessToken, refresh_token: refreshToken } = tokenResponse.data;
            // Fetch user information to get the user ID
            const userResponse = await axios_1.default.get('https://gitlab.com/api/v4/user', {
                headers: { Authorization: `Bearer ${accessToken}` },
            });
            const userId = userResponse.data.id;
            // Fetch all groups
            const groups = await (0, getProjects_1.fetchAllGitlabGroups)(accessToken);
            // Fetch projects in each group
            const groupProjects = await (0, getProjects_1.fetchGitlabGroupProjects)(accessToken, groups);
            // Fetch projects for the current user
            const userProjects = await (0, getProjects_1.fetchGitlabUserProjects)(accessToken, userId);
            integration = await this.createOrUpdate({
                platform: types_1.PlatformType.GITLAB,
                integrationIdentifier: userId.toString(),
                token: accessToken,
                refreshToken,
                status: 'mapping',
                settings: {
                    groups,
                    groupProjects,
                    userProjects,
                    user: userResponse.data,
                    updateMemberAttributes: true,
                },
            }, transaction);
            await sequelizeRepository_1.default.commitTransaction(transaction);
        }
        catch (err) {
            this.options.log.error({
                errMessage: err === null || err === void 0 ? void 0 : err.message,
                errName: err === null || err === void 0 ? void 0 : err.name,
                errStack: err === null || err === void 0 ? void 0 : err.stack,
                gitlabStatus: (_a = err === null || err === void 0 ? void 0 : err.response) === null || _a === void 0 ? void 0 : _a.status,
                gitlabError: (_b = err === null || err === void 0 ? void 0 : err.response) === null || _b === void 0 ? void 0 : _b.data,
                gitlabUrl: (_c = err === null || err === void 0 ? void 0 : err.config) === null || _c === void 0 ? void 0 : _c.url,
                gitlabMethod: (_d = err === null || err === void 0 ? void 0 : err.config) === null || _d === void 0 ? void 0 : _d.method,
            }, 'gitlabConnect failed');
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw err;
        }
        return integration;
    }
    async mapGitlabRepos(integrationId, mapping, projectIds) {
        const integration = await this.findById(integrationId);
        const webhooks = await (0, setupWebhooks_1.setupGitlabWebhooks)(integration.token, projectIds, integrationId);
        // if any of webhooks has failed, throw an error
        if (webhooks.some((w) => w.success === false)) {
            this.options.log.error({ webhooks }, 'Failed to setup webhooks');
            throw new Error('Failed to setup webhooks');
        }
        const settings = integration.settings;
        const { groupProjects, userProjects } = settings;
        const allProjects = [...userProjects, ...Object.values(groupProjects).flat()];
        allProjects.forEach((project) => {
            const isInMapping = Object.keys(mapping).some((url) => url.includes(project.path_with_namespace));
            project.enabled = isInMapping;
        });
        // Keep the original structure of groupProjects and userProjects
        settings.groupProjects = { ...groupProjects };
        settings.userProjects = [...userProjects];
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        const txOptions = {
            ...this.options,
            transaction,
        };
        try {
            // add the repos to the git integration
            if (common_1.EDITION === types_1.Edition.LFX) {
                const repos = Object.entries(mapping).reduce((acc, [url, segmentId]) => {
                    if (!acc[segmentId]) {
                        acc[segmentId] = [];
                    }
                    acc[segmentId].push(url);
                    return acc;
                }, {});
                // Note: Repos are written to public.repositories via mapUnifiedRepositories below
                for (const [segmentId, urls] of Object.entries(repos)) {
                    let isGitintegrationConfigured;
                    const segmentOptions = {
                        ...txOptions,
                        currentSegments: [
                            {
                                ...this.options.currentSegments[0],
                                id: segmentId,
                            },
                        ],
                    };
                    try {
                        await integrationRepository_1.default.findByPlatform(types_1.PlatformType.GIT, segmentOptions);
                        isGitintegrationConfigured = true;
                    }
                    catch (err) {
                        isGitintegrationConfigured = false;
                    }
                    if (isGitintegrationConfigured) {
                        const gitInfo = await this.gitGetRemotes(segmentOptions);
                        const gitRemotes = gitInfo[segmentId].remotes;
                        const allUrls = Array.from(new Set([...gitRemotes, ...urls]));
                        await this.gitConnectOrUpdate({
                            remotes: allUrls.map((url) => {
                                const project = allProjects.find((p) => url.includes(p.path_with_namespace));
                                return { url, forkedFrom: (project === null || project === void 0 ? void 0 : project.forkedFrom) || null };
                            }),
                        }, { ...segmentOptions, transaction }, types_1.PlatformType.GITLAB);
                    }
                    else {
                        await this.gitConnectOrUpdate({
                            remotes: urls.map((url) => {
                                const project = allProjects.find((p) => url.includes(p.path_with_namespace));
                                return { url, forkedFrom: (project === null || project === void 0 ? void 0 : project.forkedFrom) || null };
                            }),
                        }, { ...segmentOptions, transaction }, types_1.PlatformType.GITLAB);
                    }
                }
                // sync to public.repositories
                const txService = new IntegrationService(txOptions);
                await txService.mapUnifiedRepositories(types_1.PlatformType.GITLAB, integrationId, mapping);
            }
            const integration = await integrationRepository_1.default.update(integrationId, { settings: { ...settings, webhooks }, status: 'in-progress' }, txOptions);
            this.options.log.info('Sending GitLab message to int-run-worker!');
            const emitter = await (0, queueService_1.getIntegrationRunWorkerEmitter)();
            await emitter.triggerIntegrationRun(integration.platform, integration.id, true);
            await sequelizeRepository_1.default.commitTransaction(transaction);
        }
        catch (err) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw err;
        }
    }
    async updateGithubIntegrationSettings(installId) {
        this.options.log.info(`Updating GitHub integration settings for installId: ${installId}`);
        // Find the integration by installId
        const integration = await integrationRepository_1.default.findByIdentifier(installId, types_1.PlatformType.GITHUB);
        if (!integration || integration.platform !== types_1.PlatformType.GITHUB) {
            this.options.log.warn(`GitHub integration not found for installId: ${installId}`);
            throw new common_1.Error404('GitHub integration not found');
        }
        this.options.log.info(`Found integration: ${integration.id}`);
        // Get the install token
        const installToken = await IntegrationService.getInstallToken(installId);
        this.options.log.info(`Obtained install token for installId: ${installId}`);
        // Fetch all installed repositories
        const repos = await (0, getInstalledRepositories_1.getInstalledRepositories)(installToken);
        this.options.log.info(`Fetched ${repos.length} installed repositories`);
        // Get current repos from repositories table
        const qx = sequelizeRepository_1.default.getQueryExecutor(this.options);
        const currentRepoRows = await (0, repositories_1.getReposForGithubIntegration)(qx, integration.id);
        const currentRepoUrls = new Set(currentRepoRows.map((r) => r.url));
        const newRepos = repos.filter((repo) => !currentRepoUrls.has(repo.url));
        this.options.log.info(`Found ${newRepos.length} new repositories`);
        this.options = {
            ...this.options,
            currentSegments: [
                {
                    id: integration.segmentId,
                },
            ],
        };
        // Build full mapping: all repos (existing + new) for correct reconciliation in mapUnifiedRepositories
        const defaultSegmentId = integration.segmentId;
        const mapping = {};
        const forkedFromMap = new Map();
        for (const repo of currentRepoRows) {
            mapping[repo.url] = defaultSegmentId;
            forkedFromMap.set(repo.url, repo.forkedFrom);
        }
        for (const repo of newRepos) {
            mapping[repo.url] = defaultSegmentId;
            forkedFromMap.set(repo.url, repo.forkedFrom || null);
        }
        if (newRepos.length > 0) {
            await this.mapGithubRepos(integration.id, mapping, false, forkedFromMap);
            this.options.log.info(`Updated GitHub repos mapping for integration id: ${integration.id}`);
        }
        else {
            this.options.log.info(`No new repos to map for integration id: ${integration.id}`);
        }
        this.options.log.info(`Completed updating GitHub integration settings for installId: ${installId}`);
        return integration;
    }
    validateRepoIntegrationMapping(existingRepos, sourceIntegrationId) {
        const integrationMismatches = existingRepos.filter((repo) => repo.deletedAt === null && repo.sourceIntegrationId !== sourceIntegrationId);
        if (integrationMismatches.length > 0) {
            const mismatchDetails = integrationMismatches
                .map((repo) => `${repo.url} belongs to integration ${repo.sourceIntegrationId}`)
                .join(', ');
            throw new common_1.Error400(this.options.language, `Cannot remap repositories from different integration: ${mismatchDetails}`);
        }
    }
    validateReposOwnership(repos, sourceIntegrationId) {
        const ownershipMismatches = repos.filter((repo) => repo.sourceIntegrationId !== sourceIntegrationId);
        if (ownershipMismatches.length > 0) {
            const mismatchUrls = ownershipMismatches.map((repo) => repo.url).join(', ');
            throw new common_1.Error400(this.options.language, `These repos are managed by another integration: ${mismatchUrls}`);
        }
    }
    /**
     * Identifies mirrored repo URLs for a Git integration.
     * Mirrored repos are those linked to this Git integration but owned by another source integration.
     */
    static getMirroredRepoUrls(repos, gitIntegrationId) {
        return new Set(repos
            .filter((repo) => repo.gitIntegrationId === gitIntegrationId &&
            repo.sourceIntegrationId !== gitIntegrationId)
            .map((repo) => repo.url));
    }
    async validateGitIntegrationDeletion(gitIntegrationId, options) {
        const qx = sequelizeRepository_1.default.getQueryExecutor(options);
        // Find repos linked to this GIT integration but owned by a different integration
        const ownedByOthers = await qx.select(`
      SELECT url
      FROM public.repositories
      WHERE "gitIntegrationId" = $(gitIntegrationId)
        AND "sourceIntegrationId" != $(gitIntegrationId)
        AND "deletedAt" IS NULL
      `, { gitIntegrationId });
        if (ownedByOthers.length > 0) {
            const mismatchUrls = ownedByOthers.map((repo) => repo.url).join(', ');
            throw new common_1.Error400(this.options.language, `Cannot delete GIT integration: these repos are managed by another integration: ${mismatchUrls}`);
        }
    }
    /**
     * Builds repository payloads for insertion into public.repositories
     */
    async buildRepositoryPayloads(qx, urls, mapping, sourcePlatform, sourceIntegrationId, txOptions, existingForkedFromMap) {
        var _a;
        if (urls.length === 0) {
            return [];
        }
        const segmentIds = [...new Set(urls.map((url) => mapping[url]))];
        const collectionService = new collectionService_1.CollectionService(txOptions);
        const insightsProjectMap = new Map();
        const gitIntegrationMap = new Map();
        for (const segmentId of segmentIds) {
            const [insightsProject] = await collectionService.findInsightsProjectsBySegmentId(segmentId);
            if (!insightsProject) {
                throw new common_1.Error400(this.options.language, `Insights project not found for segment ${segmentId}`);
            }
            insightsProjectMap.set(segmentId, insightsProject.id);
            if (sourcePlatform === types_1.PlatformType.GIT) {
                gitIntegrationMap.set(segmentId, sourceIntegrationId);
            }
            else {
                try {
                    const segmentOptions = {
                        ...txOptions,
                        currentSegments: [{ ...this.options.currentSegments[0], id: segmentId }],
                    };
                    const gitIntegration = await integrationRepository_1.default.findByPlatform(types_1.PlatformType.GIT, segmentOptions);
                    gitIntegrationMap.set(segmentId, gitIntegration.id);
                }
                catch (_b) {
                    throw new common_1.Error400(this.options.language, `Git integration not found for segment ${segmentId}`);
                }
            }
        }
        // Build forkedFrom map from existing repositories (for GITHUB platforms)
        // Use the passed map if available (on first mapping, repositories table is empty)
        const forkedFromMap = existingForkedFromMap !== null && existingForkedFromMap !== void 0 ? existingForkedFromMap : new Map();
        if (!existingForkedFromMap) {
            const isGitHubPlatform = [types_1.PlatformType.GITHUB, types_1.PlatformType.GITHUB_NANGO].includes(sourcePlatform);
            if (isGitHubPlatform) {
                const existingRepos = await (0, repositories_1.getReposForGithubIntegration)(qx, sourceIntegrationId);
                for (const repo of existingRepos) {
                    if (repo.forkedFrom) {
                        forkedFromMap.set(repo.url, repo.forkedFrom);
                    }
                }
            }
        }
        // Build payloads
        const payloads = [];
        for (const url of urls) {
            const segmentId = mapping[url];
            const id = (0, common_1.generateUUIDv4)();
            const insightsProjectId = insightsProjectMap.get(segmentId);
            const gitIntegrationId = gitIntegrationMap.get(segmentId);
            payloads.push({
                id,
                url,
                segmentId,
                gitIntegrationId,
                sourceIntegrationId,
                insightsProjectId,
                forkedFrom: (_a = forkedFromMap.get(url)) !== null && _a !== void 0 ? _a : null,
            });
        }
        return payloads;
    }
    async mapUnifiedRepositories(sourcePlatform, sourceIntegrationId, mapping, skipMirroredRepos = true, forkedFromMap) {
        // Check for existing transaction to support nested calls within outer transactions
        const existingTransaction = sequelizeRepository_1.default.getTransaction(this.options);
        const transaction = existingTransaction || (await sequelizeRepository_1.default.createTransaction(this.options));
        const txOptions = {
            ...this.options,
            transaction,
        };
        try {
            const qx = sequelizeRepository_1.default.getQueryExecutor(txOptions);
            const mappedUrls = Object.keys(mapping);
            const mappedUrlSet = new Set(mappedUrls);
            const [existingMappedRepos, activeIntegrationRepos] = await Promise.all([
                (0, repositories_1.getRepositoriesByUrl)(qx, mappedUrls, true),
                (0, repositories_1.getRepositoriesBySourceIntegrationId)(qx, sourceIntegrationId),
            ]);
            // For Git integration updates, identify mirrored repos (owned by other integrations)
            // These should be skipped from all operations unless destroying the integration
            const isGitIntegration = sourcePlatform === types_1.PlatformType.GIT;
            const mirroredRepoUrls = isGitIntegration && skipMirroredRepos
                ? IntegrationService.getMirroredRepoUrls(existingMappedRepos, sourceIntegrationId)
                : new Set();
            // Filter out mirrored repos for validation and processing
            const reposToValidate = existingMappedRepos.filter((repo) => !mirroredRepoUrls.has(repo.url));
            // Block repos that belong to a different integration (skip mirrored for Git)
            this.validateRepoIntegrationMapping(reposToValidate, sourceIntegrationId);
            // Filter out mirrored URLs from processing
            const ownedMappedUrls = mappedUrls.filter((url) => !mirroredRepoUrls.has(url));
            const existingUrlSet = new Set(reposToValidate.map((repo) => repo.url));
            const toInsertUrls = ownedMappedUrls.filter((url) => !existingUrlSet.has(url));
            // Repos to restore: soft-deleted OR segment changed (both need re-onboarding)
            const toRestoreRepos = reposToValidate.filter((repo) => repo.deletedAt !== null || repo.segmentId !== mapping[repo.url]);
            const toSoftDeleteRepos = activeIntegrationRepos.filter((repo) => !mappedUrlSet.has(repo.url));
            if (mirroredRepoUrls.size > 0) {
                this.options.log.info(`Skipping ${mirroredRepoUrls.size} mirrored repos from Git integration update`);
            }
            this.options.log.info(`Repository mapping: ${toInsertUrls.length} to insert, ${toRestoreRepos.length} to restore, ${toSoftDeleteRepos.length} to soft-delete`);
            if (toInsertUrls.length > 0) {
                this.options.log.info(`Inserting ${toInsertUrls.length} new repos into public.repositories...`);
                const payloads = await this.buildRepositoryPayloads(qx, toInsertUrls, mapping, sourcePlatform, sourceIntegrationId, txOptions, forkedFromMap);
                if (payloads.length > 0) {
                    await (0, repositories_1.insertRepositories)(qx, payloads);
                    this.options.log.info(`Inserted ${payloads.length} repos into public.repositories`);
                }
            }
            if (toRestoreRepos.length > 0) {
                this.options.log.info(`Restoring ${toRestoreRepos.length} repos in public.repositories...`);
                const toRestoreUrls = toRestoreRepos.map((repo) => repo.url);
                const restorePayloads = await this.buildRepositoryPayloads(qx, toRestoreUrls, mapping, sourcePlatform, sourceIntegrationId, txOptions, forkedFromMap);
                if (restorePayloads.length > 0) {
                    await (0, repositories_1.restoreRepositories)(qx, restorePayloads);
                    this.options.log.info(`Restored ${restorePayloads.length} repos in public.repositories`);
                }
            }
            if (toSoftDeleteRepos.length > 0) {
                this.validateReposOwnership(toSoftDeleteRepos, sourceIntegrationId);
                this.options.log.info(`Soft-deleting ${toSoftDeleteRepos.length} repos from public.repositories...`);
                await (0, repositories_1.softDeleteRepositories)(qx, toSoftDeleteRepos.map((repo) => repo.url), sourceIntegrationId);
                this.options.log.info(`Soft-deleted ${toSoftDeleteRepos.length} repos from public.repositories`);
            }
            // Only commit if we created the transaction ourselves
            if (!existingTransaction) {
                await sequelizeRepository_1.default.commitTransaction(transaction);
            }
        }
        catch (err) {
            this.options.log.error(err, 'Error while mapping unified repositories!');
            // Only rollback if we created the transaction ourselves
            if (!existingTransaction) {
                try {
                    await sequelizeRepository_1.default.rollbackTransaction(transaction);
                }
                catch (rErr) {
                    this.options.log.error(rErr, 'Error while rolling back transaction!');
                }
            }
            throw err;
        }
    }
}
exports.default = IntegrationService;
//# sourceMappingURL=integrationService.js.map