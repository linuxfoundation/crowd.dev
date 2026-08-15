"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.refreshGithubRepoSettings = void 0;
/* eslint-disable no-continue */
const cron_time_generator_1 = __importDefault(require("cron-time-generator"));
const common_1 = require("@crowd/common");
const logging_1 = require("@crowd/logging");
const sequelizeRepository_1 = __importDefault(require("../../database/repositories/sequelizeRepository"));
const integrationService_1 = __importDefault(require("../../services/integrationService"));
const log = (0, logging_1.getServiceChildLogger)('refreshGithubRepoSettings');
const refreshForGitHub = async () => {
    log.info('Updating Github repo settings.');
    const dbOptions = await sequelizeRepository_1.default.getDefaultIRepositoryOptions();
    const githubIntegrations = await dbOptions.database.sequelize.query(`SELECT id, "tenantId", "integrationIdentifier" FROM integrations 
       WHERE platform = 'github' AND "deletedAt" IS NULL
       AND "createdAt" < NOW() - INTERVAL '1 minute' AND "integrationIdentifier" IS NOT NULL`);
    for (const integration of githubIntegrations[0]) {
        log.info(`Updating repo settings for Github integration: ${integration.id}`);
        try {
            const options = await sequelizeRepository_1.default.getDefaultIRepositoryOptions();
            options.currentTenant = { id: integration.tenantId };
            const integrationService = new integrationService_1.default(options);
            // newly discovered repos will be mapped to default segment of the integration
            await integrationService.updateGithubIntegrationSettings(integration.integrationIdentifier);
            log.info(`Successfully updated repo settings for Github integration: ${integration.id}`);
        }
        catch (err) {
            log.error(`Error updating repo settings for Github integration ${integration.id}: ${err.message}`);
        }
        finally {
            await (0, common_1.timeout)(1000);
        }
    }
    log.info('Finished updating Github repo settings.');
};
const refreshGithubRepoSettings = async () => {
    log.info('Updating Github repo settings.');
    await refreshForGitHub();
};
exports.refreshGithubRepoSettings = refreshGithubRepoSettings;
const job = {
    name: 'Refresh Github repo settings',
    // every day
    cronTime: common_1.IS_DEV_ENV ? cron_time_generator_1.default.every(5).minutes() : cron_time_generator_1.default.every(1).days(),
    onTrigger: async () => {
        await (0, exports.refreshGithubRepoSettings)();
    },
};
exports.default = job;
//# sourceMappingURL=refreshGithubRepoSettings.js.map