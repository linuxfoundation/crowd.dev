"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
var _a;
Object.defineProperty(exports, "__esModule", { value: true });
exports.ENABLE_LF_COLLECTION_MANAGEMENT = exports.LINUX_FOUNDATION_CONFIG = exports.SNOWFLAKE_CONFIG = exports.REDDIT_CONFIG = exports.GITLAB_CONFIG = exports.OPEN_STATUS_API_CONFIG = exports.SEARCH_SYNC_API_CONFIG = exports.TEMPORAL_CONFIG = exports.CROWD_ANALYTICS_CONFIG = exports.INTEGRATION_PROCESSING_CONFIG = exports.STACKEXCHANGE_CONFIG = exports.OPENSEARCH_CONFIG = exports.GITHUB_TOKEN_CONFIG = exports.EAGLE_EYE_CONFIG = exports.ORGANIZATION_ENRICHMENT_CONFIG = exports.ENRICHMENT_CONFIG = exports.NANGO_CONFIG = exports.JIRA_ISSUE_REPORTER_CONFIG = exports.GITHUB_ISSUE_REPORTER_CONFIG = exports.GITHUB_CONFIG = exports.DISCORD_CONFIG = exports.GOOGLE_CONFIG = exports.SLACK_CONFIG = exports.TWITTER_CONFIG = exports.SSO_CONFIG = exports.AUTH0_CONFIG = exports.API_CONFIG = exports.CLEARBIT_CONFIG = exports.COMPREHEND_CONFIG = exports.SEGMENT_CONFIG = exports.PACKAGES_TEMPORAL_CONFIG = exports.PACKAGES_DB_CONFIG = exports.PRODUCT_DB_CONFIG = exports.DB_CONFIG = exports.S3_CONFIG = exports.REDIS_CONFIG = exports.QUEUE_CONFIG = exports.ENCRYPTION_CONFIG = exports.IS_CLOUD_ENV = exports.LOG_LEVEL = exports.IS_STAGING_ENV = exports.IS_PROD_ENV = exports.IS_DEV_ENV = exports.IS_TEST_ENV = exports.TENANT_MODE = exports.SERVICE = exports.KUBE_MODE = exports.ENCRYPTION_INIT_VECTOR = exports.ENCRYPTION_SECRET_KEY = void 0;
const config_1 = __importDefault(require("config"));
// TODO-kube
exports.ENCRYPTION_SECRET_KEY = process.env.ENCRYPTION_SECRET_KEY;
exports.ENCRYPTION_INIT_VECTOR = process.env.ENCRYPTION_INIT_VECTOR;
exports.KUBE_MODE = process.env.KUBE_MODE !== undefined;
exports.SERVICE = process.env.SERVICE;
exports.TENANT_MODE = process.env.TENANT_MODE;
exports.IS_TEST_ENV = process.env.NODE_ENV === 'test';
exports.IS_DEV_ENV = process.env.NODE_ENV === 'development' ||
    process.env.NODE_ENV === 'docker' ||
    process.env.NODE_ENV === undefined;
exports.IS_PROD_ENV = process.env.NODE_ENV === 'production';
exports.IS_STAGING_ENV = process.env.NODE_ENV === 'staging';
exports.LOG_LEVEL = process.env.LOG_LEVEL || 'info';
exports.IS_CLOUD_ENV = exports.IS_PROD_ENV || exports.IS_STAGING_ENV;
exports.ENCRYPTION_CONFIG = config_1.default.get('encryption');
exports.QUEUE_CONFIG = config_1.default.get('queue');
exports.REDIS_CONFIG = config_1.default.get('redis');
exports.S3_CONFIG = config_1.default.get('s3');
exports.DB_CONFIG = config_1.default.get('db');
exports.PRODUCT_DB_CONFIG = config_1.default.has('productDb')
    ? config_1.default.get('productDb')
    : undefined;
exports.PACKAGES_DB_CONFIG = config_1.default.has('packagesDb')
    ? config_1.default.get('packagesDb')
    : undefined;
// packages_worker (npm/maven/pypi/osv/security-contacts/...) runs in its own Temporal
// namespace, separate from the API's default namespace — see CROWD_PACKAGES_TEMPORAL_NAMESPACE.
exports.PACKAGES_TEMPORAL_CONFIG = config_1.default.has('packagesTemporal.namespace')
    ? config_1.default.get('packagesTemporal')
    : undefined;
exports.SEGMENT_CONFIG = config_1.default.get('segment');
exports.COMPREHEND_CONFIG = config_1.default.get('comprehend');
exports.CLEARBIT_CONFIG = config_1.default.get('clearbit');
exports.API_CONFIG = config_1.default.get('api');
exports.AUTH0_CONFIG = config_1.default.get('auth0');
exports.SSO_CONFIG = config_1.default.get('sso');
exports.TWITTER_CONFIG = config_1.default.get('twitter');
exports.SLACK_CONFIG = config_1.default.get('slack');
exports.GOOGLE_CONFIG = config_1.default.get('google');
exports.DISCORD_CONFIG = config_1.default.get('discord');
exports.GITHUB_CONFIG = config_1.default.get('github');
exports.GITHUB_ISSUE_REPORTER_CONFIG = config_1.default.get('githubIssueReporter');
exports.JIRA_ISSUE_REPORTER_CONFIG = config_1.default.get('jiraIssueReporter');
exports.NANGO_CONFIG = config_1.default.get('nango');
exports.ENRICHMENT_CONFIG = config_1.default.get('enrichment');
exports.ORGANIZATION_ENRICHMENT_CONFIG = config_1.default.get('organizationEnrichment');
exports.EAGLE_EYE_CONFIG = config_1.default.get('eagleEye');
exports.GITHUB_TOKEN_CONFIG = config_1.default.get('githubToken');
exports.OPENSEARCH_CONFIG = config_1.default.get('opensearch');
exports.STACKEXCHANGE_CONFIG = (_a = config_1.default.get('stackexchange')) !== null && _a !== void 0 ? _a : {
    key: process.env.STACKEXCHANGE_KEY,
};
exports.INTEGRATION_PROCESSING_CONFIG = config_1.default.get('integrationProcessing');
exports.CROWD_ANALYTICS_CONFIG = config_1.default.get('crowdAnalytics');
exports.TEMPORAL_CONFIG = config_1.default.get('temporal');
exports.SEARCH_SYNC_API_CONFIG = config_1.default.get('searchSyncApi');
exports.OPEN_STATUS_API_CONFIG = config_1.default.get('openStatusApi');
exports.GITLAB_CONFIG = config_1.default.get('gitlab');
exports.REDDIT_CONFIG = config_1.default.get('reddit');
exports.SNOWFLAKE_CONFIG = config_1.default.get('snowflake');
exports.LINUX_FOUNDATION_CONFIG = config_1.default.get('linuxFoundation');
exports.ENABLE_LF_COLLECTION_MANAGEMENT = process.env.ENABLE_LF_COLLECTION_MANAGEMENT === 'true';
//# sourceMappingURL=index.js.map