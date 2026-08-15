"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const logging_1 = require("@crowd/logging");
const refreshGithubRepoSettings_1 = require("../jobs/refreshGithubRepoSettings");
const logger = (0, logging_1.getServiceChildLogger)('refreshGithubRepoSettings');
setImmediate(async () => {
    try {
        const startTime = Date.now();
        logger.info('Starting refresh of Github repo settings');
        await (0, refreshGithubRepoSettings_1.refreshGithubRepoSettings)();
        const duration = Date.now() - startTime;
        logger.info(`Completed refresh of Github repo settings in ${duration}ms`);
        process.exit(0);
    }
    catch (error) {
        logger.error(`Error refreshing Github repo settings: ${error.message}`);
        process.exit(1);
    }
});
//# sourceMappingURL=refresh-github-repo-settings.js.map