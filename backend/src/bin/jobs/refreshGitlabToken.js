"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const axios_1 = __importDefault(require("axios"));
const cron_time_generator_1 = __importDefault(require("cron-time-generator"));
const common_1 = require("@crowd/common");
const logging_1 = require("@crowd/logging");
const conf_1 = require("@/conf");
const sequelizeRepository_1 = __importDefault(require("../../database/repositories/sequelizeRepository"));
const log = (0, logging_1.getServiceChildLogger)('refreshGitlabTokenCronJob');
const job = {
    name: 'Refresh Gitlab token',
    // every hour
    cronTime: cron_time_generator_1.default.every(1).hours(),
    onTrigger: async () => {
        log.info('Checking Gitlab tokens for refresh.');
        const dbOptions = await sequelizeRepository_1.default.getDefaultIRepositoryOptions();
        const gitlabTokens = await dbOptions.database.sequelize.query(`SELECT id, token, "refreshToken" FROM integrations 
       WHERE platform = 'gitlab' AND "deletedAt" IS NULL
       AND "createdAt" < NOW() - INTERVAL '1 hour'`);
        for (const integration of gitlabTokens[0]) {
            log.info(`Refreshing token for Gitlab integration: ${integration.id}`);
            try {
                const config = {
                    method: 'post',
                    url: 'https://gitlab.com/oauth/token',
                    data: {
                        grant_type: 'refresh_token',
                        refresh_token: integration.refreshToken,
                        client_id: conf_1.GITLAB_CONFIG.clientId,
                        client_secret: conf_1.GITLAB_CONFIG.clientSecret,
                    },
                    headers: {
                        'Content-Type': 'application/json',
                    },
                };
                const response = await (0, axios_1.default)(config);
                const newToken = response.data.access_token;
                const newRefreshToken = response.data.refresh_token;
                await dbOptions.database.integration.update({
                    token: newToken,
                    refreshToken: newRefreshToken,
                }, { where: { id: integration.id } });
                log.info(`Successfully refreshed token for Gitlab integration: ${integration.id}`);
            }
            catch (err) {
                log.error(`Error refreshing token for Gitlab integration ${integration.id}: ${err.message}`);
            }
            finally {
                await (0, common_1.timeout)(1000);
            }
        }
    },
};
exports.default = job;
//# sourceMappingURL=refreshGitlabToken.js.map