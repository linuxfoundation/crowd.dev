"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const passport_1 = __importDefault(require("passport"));
const common_1 = require("@crowd/common");
const redis_1 = require("@crowd/redis");
const conf_1 = require("../../conf");
const segmentRepository_1 = __importDefault(require("../../database/repositories/segmentRepository"));
const authMiddleware_1 = require("../../middlewares/authMiddleware");
const errorMiddleware_1 = require("../../middlewares/errorMiddleware");
const tenantService_1 = __importDefault(require("../../services/tenantService"));
const decodeBase64Url = (data) => {
    data = data.replaceAll('-', '+').replaceAll('_', '/');
    while (data.length % 4) {
        data += '=';
    }
    return atob(data);
};
exports.default = (app) => {
    app.post(`/integration/query`, (0, errorMiddleware_1.safeWrap)(require('./integrationQuery').default));
    app.post(`/integration`, (0, errorMiddleware_1.safeWrap)(require('./integrationCreate').default));
    app.put(`/integration/:id`, (0, errorMiddleware_1.safeWrap)(require('./integrationUpdate').default));
    app.delete(`/integration`, (0, errorMiddleware_1.safeWrap)(require('./integrationDestroy').default));
    app.get(`/integration/autocomplete`, (0, errorMiddleware_1.safeWrap)(require('./integrationAutocomplete').default));
    app.get(`/integration/global`, (0, errorMiddleware_1.safeWrap)(require('./integrationGlobal').default));
    app.get(`/integration/global/status`, (0, errorMiddleware_1.safeWrap)(require('./integrationGlobalStatus').default));
    app.get('/integration/github-installations', (0, errorMiddleware_1.safeWrap)(require('./helpers/githubGetInstallations').default));
    app.post('/integration/github-connect-installation', (0, errorMiddleware_1.safeWrap)(require('./helpers/githubConnectInstallation').default));
    app.get(`/integration`, (0, errorMiddleware_1.safeWrap)(require('./integrationList').default));
    app.get(`/integration/:id`, (0, errorMiddleware_1.safeWrap)(require('./integrationFind').default));
    // Unified endpoint for all code platform integrations (github, gitlab, git, gerrit)
    app.get(`/integration/:id/repositories`, (0, errorMiddleware_1.safeWrap)(require('./helpers/getIntegrationRepositories').default));
    app.put(`/authenticate/:code`, (0, errorMiddleware_1.safeWrap)(require('./helpers/githubAuthenticate').default));
    app.put(`/integration/:id/github/repos`, (0, errorMiddleware_1.safeWrap)(require('./helpers/githubMapRepos').default));
    app.get(`/integration/github/search/orgs`, (0, errorMiddleware_1.safeWrap)(require('./helpers/githubSearchOrgs').default));
    app.get(`/integration/github/search/repos`, (0, errorMiddleware_1.safeWrap)(require('./helpers/githubSearchRepos').default));
    app.get(`/integration/github/orgs/:org/repos`, (0, errorMiddleware_1.safeWrap)(require('./helpers/githubOrgRepos').default));
    app.post('/github-nango-connect', (0, errorMiddleware_1.safeWrap)(require('./helpers/githubNangoConnect').default));
    app.put(`/discord-authenticate/:guild_id`, (0, errorMiddleware_1.safeWrap)(require('./helpers/discordAuthenticate').default));
    app.put(`/reddit-onboard`, (0, errorMiddleware_1.safeWrap)(require('./helpers/redditOnboard').default));
    app.put('/linkedin-connect', (0, errorMiddleware_1.safeWrap)(require('./helpers/linkedinConnect').default));
    app.post('/linkedin-onboard', (0, errorMiddleware_1.safeWrap)(require('./helpers/linkedinOnboard').default));
    app.post(`/integration/progress/list`, (0, errorMiddleware_1.safeWrap)(require('./integrationProgressList').default));
    app.get(`/integration/progress/:id`, (0, errorMiddleware_1.safeWrap)(require('./integrationProgress').default));
    app.get(`/integration/mapped-repos/:id`, (0, errorMiddleware_1.safeWrap)(require('./integrationMappedRepos').default));
    // Git
    app.put(`/git-connect`, (0, errorMiddleware_1.safeWrap)(require('./helpers/gitAuthenticate').default));
    app.put(`/mailing-list-connect`, (0, errorMiddleware_1.safeWrap)(require('./helpers/mailingListAuthenticate').default));
    app.put(`/confluence-connect`, (0, errorMiddleware_1.safeWrap)(require('./helpers/confluenceAuthenticate').default));
    app.put(`/gerrit-connect`, (0, errorMiddleware_1.safeWrap)(require('./helpers/gerritAuthenticate').default));
    app.get('/devto-validate', (0, errorMiddleware_1.safeWrap)(require('./helpers/devtoValidators').default));
    app.get('/reddit-validate', (0, errorMiddleware_1.safeWrap)(require('./helpers/redditValidator').default));
    app.post('/devto-connect', (0, errorMiddleware_1.safeWrap)(require('./helpers/devtoCreateOrUpdate').default));
    app.post('/hackernews-connect', (0, errorMiddleware_1.safeWrap)(require('./helpers/hackerNewsCreateOrUpdate').default));
    app.post('/stackoverflow-connect', (0, errorMiddleware_1.safeWrap)(require('./helpers/stackOverflowCreateOrUpdate').default));
    app.get('/stackoverflow-validate', (0, errorMiddleware_1.safeWrap)(require('./helpers/stackOverflowValidator').default));
    app.get('/stackoverflow-volume', (0, errorMiddleware_1.safeWrap)(require('./helpers/stackOverflowVolume').default));
    app.post('/discourse-connect', (0, errorMiddleware_1.safeWrap)(require('./helpers/discourseCreateOrUpdate').default));
    app.post('/discourse-validate', (0, errorMiddleware_1.safeWrap)(require('./helpers/discourseValidator').default));
    app.post('/discourse-test-webhook', (0, errorMiddleware_1.safeWrap)(require('./helpers/discourseTestWebhook').default));
    app.post('/groupsio-connect', (0, errorMiddleware_1.safeWrap)(require('./helpers/groupsioConnectOrUpdate').default));
    app.post('/groupsio-get-token', (0, errorMiddleware_1.safeWrap)(require('./helpers/groupsioGetToken').default));
    app.post('/groupsio-verify-group', (0, errorMiddleware_1.safeWrap)(require('./helpers/groupsioVerifyGroup').default));
    app.post('/groupsio-get-user-subscriptions', (0, errorMiddleware_1.safeWrap)(require('./helpers/groupsioGetUserSubscriptions').default));
    app.post('/jira-connect', (0, errorMiddleware_1.safeWrap)(require('./helpers/jiraConnectOrUpdate').default));
    app.get('/gitlab/connect', (0, errorMiddleware_1.safeWrap)(require('./helpers/gitlabAuthenticate').default));
    app.get('/gitlab/callback', (0, errorMiddleware_1.safeWrap)(require('./helpers/gitlabAuthenticateCallback').default));
    app.put(`/integration/:id/gitlab/repos`, (0, errorMiddleware_1.safeWrap)(require('./helpers/gitlabMapRepos').default));
    if (conf_1.TWITTER_CONFIG.clientId) {
        /**
         * Using the passport.authenticate this endpoint forces a
         * redirect to happen to the twitter oauth2 page.
         * We keep a state of the important variables such as tenantId, redirectUrl, ..
         * so that after user logs in through the twitter page, these
         * variables are forwarded back to the callback as well
         * This state is sent using the authenticator options and
         * manipulated through twitterStrategy.staticPKCEStore
         */
        app.get('/twitter/connect', (0, errorMiddleware_1.safeWrap)(require('./helpers/twitterAuthenticate').default), () => {
            // The request will be redirected for authentication, so this
            // function will not be called.
        });
        /**
         * OAuth2 callback endpoint.  After user successfully
         * logs in through twitter page s/he is redirected to
         * this endpoint. Few middlewares first mimic a proper
         * api request in this order:
         *
         * Set headers-> Auth middleware (currentUser)-> Set currentTenant
         * -> finally we call the project service to update the
         * corresponding project.
         *
         * We have to call these standart middlewares explicitly because
         * the request method is get and tenant id does not exist in the
         * uri as request parameters.
         *
         */
        app.get('/twitter/callback', 
        // passport.authenticate('twitter', {
        //   session: false,
        //   failureRedirect: `${API_CONFIG.frontendUrl}/integrations?error=true`,
        // }),
        async (req, _res, next) => {
            const stateQueryParam = req.query.state;
            const decodedState = decodeBase64Url(stateQueryParam);
            req.state = JSON.parse(decodedState);
            next();
        }, (req, _res, next) => {
            const { crowdToken } = req.state;
            req.headers.authorization = `Bearer ${crowdToken}`;
            next();
        }, authMiddleware_1.authMiddleware, async (req, _res, next) => {
            const tenantId = common_1.DEFAULT_TENANT_ID;
            req.currentTenant = await new tenantService_1.default(req).findById(tenantId);
            next();
        }, async (req, _res, next) => {
            const cache = new redis_1.RedisCache('twitterPKCE', req.redis, req.log);
            const state = await cache.get(req.currentUser.id);
            const { segmentIds } = JSON.parse(state);
            const segmentRepository = new segmentRepository_1.default(req);
            req.currentSegments = await segmentRepository.findInIds(segmentIds);
            next();
        }, (0, errorMiddleware_1.safeWrap)(require('./helpers/twitterAuthenticateCallback').default));
    }
    /**
     * Slack integration endpoints
     * These should be super similar to Twitter's, since we're also using passport.js
     */
    if (conf_1.SLACK_CONFIG.clientId) {
        // path to start the OAuth flow
        app.get('/slack/connect', (0, errorMiddleware_1.safeWrap)(require('./helpers/slackAuthenticate').default));
        // OAuth callback url
        app.get('/slack/callback', passport_1.default.authorize('slack', {
            session: false,
            failureRedirect: `${conf_1.API_CONFIG.frontendUrl}/integrations?error=true`,
        }), async (req, _res, next) => {
            req.state = JSON.parse(Buffer.from(req.query.state, 'base64').toString());
            next();
        }, (req, _res, next) => {
            const { crowdToken } = req.state;
            req.headers.authorization = `Bearer ${crowdToken}`;
            next();
        }, authMiddleware_1.authMiddleware, async (req, _res, next) => {
            const tenantId = common_1.DEFAULT_TENANT_ID;
            req.currentTenant = await new tenantService_1.default(req).findById(tenantId);
            next();
        }, async (req, _res, next) => {
            const { segmentIds } = req.state;
            const segmentRepository = new segmentRepository_1.default(req);
            req.currentSegments = await segmentRepository.findInIds(segmentIds);
            next();
        }, (0, errorMiddleware_1.safeWrap)(require('./helpers/slackAuthenticateCallback').default));
    }
};
//# sourceMappingURL=index.js.map