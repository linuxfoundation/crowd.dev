"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.cleanUpOldWebhooks = exports.cleanUpOrphanedWebhooks = exports.cleanUpOldRuns = void 0;
const logging_1 = require("@crowd/logging");
const incomingWebhookRepository_1 = __importDefault(require("../../database/repositories/incomingWebhookRepository"));
const integrationRunRepository_1 = __importDefault(require("../../database/repositories/integrationRunRepository"));
const sequelizeRepository_1 = __importDefault(require("../../database/repositories/sequelizeRepository"));
const MAX_MONTHS_TO_KEEP = 3;
const log = (0, logging_1.getServiceChildLogger)('cleanUp');
const cleanUpOldRuns = async () => {
    const dbOptions = await sequelizeRepository_1.default.getDefaultIRepositoryOptions();
    const repo = new integrationRunRepository_1.default(dbOptions);
    log.info(`Cleaning up processed integration runs that are older than ${MAX_MONTHS_TO_KEEP} months!`);
    await repo.cleanupOldRuns(MAX_MONTHS_TO_KEEP);
};
exports.cleanUpOldRuns = cleanUpOldRuns;
const cleanUpOrphanedWebhooks = async () => {
    const dbOptions = await sequelizeRepository_1.default.getDefaultIRepositoryOptions();
    const repo = new incomingWebhookRepository_1.default(dbOptions);
    log.info(`Cleaning up orphaned incoming webhooks that doesn't belong to any integrations anymore!`);
    await repo.cleanUpOrphanedWebhooks();
};
exports.cleanUpOrphanedWebhooks = cleanUpOrphanedWebhooks;
const cleanUpOldWebhooks = async () => {
    const dbOptions = await sequelizeRepository_1.default.getDefaultIRepositoryOptions();
    const repo = new incomingWebhookRepository_1.default(dbOptions);
    log.info(`Cleaning up processed incoming webhooks that are older than ${MAX_MONTHS_TO_KEEP} months!`);
    await repo.cleanUpOldWebhooks(MAX_MONTHS_TO_KEEP);
};
exports.cleanUpOldWebhooks = cleanUpOldWebhooks;
const job = {
    name: 'Clean up old data',
    // run once every week on Sunday at 1AM
    cronTime: '0 1 * * 0',
    onTrigger: async () => {
        await Promise.all([(0, exports.cleanUpOldRuns)(), (0, exports.cleanUpOldWebhooks)(), (0, exports.cleanUpOrphanedWebhooks)()]);
    },
};
exports.default = job;
//# sourceMappingURL=cleanUp.js.map