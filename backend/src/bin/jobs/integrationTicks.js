"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const cron_time_generator_1 = __importDefault(require("cron-time-generator"));
const logging_1 = require("@crowd/logging");
const sequelizeRepository_1 = __importDefault(require("../../database/repositories/sequelizeRepository"));
const integrationProcessor_1 = require("../../serverless/integrations/services/integrationProcessor");
let integrationProcessorInstance;
async function getIntegrationProcessor() {
    if (integrationProcessorInstance)
        return integrationProcessorInstance;
    const options = {
        ...(await sequelizeRepository_1.default.getDefaultIRepositoryOptions()),
        log: (0, logging_1.getServiceLogger)(),
    };
    integrationProcessorInstance = new integrationProcessor_1.IntegrationProcessor(options);
    return integrationProcessorInstance;
}
const job = {
    name: 'Integration Ticker',
    // every two hours
    cronTime: cron_time_generator_1.default.every(1).minutes(),
    onTrigger: async () => {
        const processor = await getIntegrationProcessor();
        await processor.processTick();
    },
};
exports.default = job;
//# sourceMappingURL=integrationTicks.js.map