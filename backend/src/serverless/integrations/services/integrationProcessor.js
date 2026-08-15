"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.IntegrationProcessor = void 0;
const logging_1 = require("@crowd/logging");
const integrationRunRepository_1 = __importDefault(require("../../../database/repositories/integrationRunRepository"));
const integrationTickProcessor_1 = require("./integrationTickProcessor");
class IntegrationProcessor extends logging_1.LoggerBase {
    constructor(options) {
        super(options.log);
        const integrationRunRepository = new integrationRunRepository_1.default(options);
        this.tickProcessor = new integrationTickProcessor_1.IntegrationTickProcessor(options, integrationRunRepository);
    }
    async processTick() {
        await this.tickProcessor.processTick();
    }
}
exports.IntegrationProcessor = IntegrationProcessor;
//# sourceMappingURL=integrationProcessor.js.map