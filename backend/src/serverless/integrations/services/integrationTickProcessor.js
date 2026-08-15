"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.IntegrationTickProcessor = void 0;
const common_1 = require("@crowd/common");
const integrations_1 = require("@crowd/integrations");
const logging_1 = require("@crowd/logging");
const types_1 = require("@crowd/types");
const integrationRepository_1 = __importDefault(require("@/database/repositories/integrationRepository"));
const queueService_1 = require("../../utils/queueService");
class IntegrationTickProcessor extends logging_1.LoggerBase {
    constructor(options, integrationRunRepository) {
        super(options.log);
        this.integrationRunRepository = integrationRunRepository;
        this.tickTrackingMap = new Map();
        this.emittersInitialized = false;
        for (const intService of integrations_1.INTEGRATION_SERVICES) {
            this.tickTrackingMap[intService.type] = 0;
        }
    }
    async initEmitters() {
        if (!this.emittersInitialized) {
            this.intRunWorkerEmitter = await (0, queueService_1.getIntegrationRunWorkerEmitter)();
            this.intStreamWorkerEmitter = await (0, queueService_1.getIntegrationStreamWorkerEmitter)();
            this.dataSinkWorkerEmitter = await (0, queueService_1.getDataSinkWorkerEmitter)();
            this.emittersInitialized = true;
        }
    }
    async processTick() {
        await this.processCheckTick();
        await this.processDelayedTick();
    }
    async processCheckTick() {
        this.log.trace('Processing integration processor tick!');
        const tickers = integrations_1.INTEGRATION_SERVICES.map((i) => ({
            type: i.type,
            ticksBetweenChecks: i.checkEvery || -1,
        }));
        const promises = [];
        for (const intService of tickers) {
            let trigger = false;
            if (intService.ticksBetweenChecks < 0) {
                this.log.debug({ type: intService.type }, 'Integration is set to never be triggered.');
            }
            else if (intService.ticksBetweenChecks === 0) {
                this.log.warn({ type: intService.type }, 'Integration is set to be always triggered.');
                trigger = true;
            }
            else {
                this.tickTrackingMap[intService.type]++;
                if (this.tickTrackingMap[intService.type] === intService.ticksBetweenChecks) {
                    this.log.info({ type: intService.type, tickCount: intService.ticksBetweenChecks }, 'Integration is being triggered since it reached its target tick count!');
                    trigger = true;
                    this.tickTrackingMap[intService.type] = 0;
                }
            }
            if (trigger) {
                this.log.info({ type: intService.type }, 'Triggering integration check!');
                promises.push(this.processCheck(intService.type).catch((err) => {
                    this.log.error(err, 'Error while processing integration check!');
                }));
            }
        }
        if (promises.length > 0) {
            await Promise.all(promises);
        }
    }
    async fixIntegrationRuns(integrationId, logger) {
        await this.integrationRunRepository.cleanupOrphanedIntegrationRuns(integrationId);
        const stuckRuns = await this.integrationRunRepository.findStuckIntegrationRuns(integrationId);
        logger.info(`${stuckRuns.length} stuck integration runs found for integrations ${integrationId}`);
        await this.initEmitters();
        for (const run of stuckRuns) {
            logger.info(`Retrying streams for stuck run: ${run.id}`);
            await this.intStreamWorkerEmitter.continueProcessingRunStreams(run.onboarding, undefined, run.id);
        }
    }
    async processCheck(type) {
        const logger = (0, logging_1.getChildLogger)('processCheck', this.log, { IntegrationType: type });
        logger.trace('Processing integration check!');
        const newIntService = (0, common_1.singleOrDefault)(integrations_1.INTEGRATION_SERVICES, (i) => i.type === type);
        if (!newIntService) {
            throw new Error(`No integration service found for type ${type}!`);
        }
        const emitter = await (0, queueService_1.getIntegrationRunWorkerEmitter)();
        await (0, common_1.processPaginated)(async (page) => integrationRepository_1.default.findAllActive(type, page, 10), async (integrations) => {
            logger.debug({ integrationIds: integrations.map((i) => i.id) }, 'Found new integrations to check!');
            for (const integration of integrations) {
                await this.fixIntegrationRuns(integration.id, logger);
                const existingRun = await this.integrationRunRepository.findLastProcessingRunInNewFramework(integration.id);
                if (!existingRun) {
                    const CHUNKS = 3; // Define the number of chunks
                    const DELAY_BETWEEN_CHUNKS = 30 * 60 * 1000; // Define the delay between chunks in milliseconds
                    const rand = Math.random() * CHUNKS;
                    const chunkIndex = Math.min(Math.floor(rand), CHUNKS - 1);
                    const delay = chunkIndex * DELAY_BETWEEN_CHUNKS;
                    // Divide integrations into chunks for Discord
                    if (newIntService.type === types_1.IntegrationType.DISCORD) {
                        setTimeout(async () => {
                            logger.info({ integrationId: integration.id }, `Triggering new delayed integration check for Discord in ${delay / 60 / 1000} minutes!`);
                            await emitter.triggerIntegrationRun(integration.platform, integration.id, false);
                        }, delay);
                    }
                    else {
                        logger.info({ integrationId: integration.id }, 'Triggering new integration check!');
                        await emitter.triggerIntegrationRun(integration.platform, integration.id, false);
                    }
                }
                else {
                    logger.info({ integrationId: integration.id }, 'Existing run found, skipping!');
                }
            }
        });
    }
    async processDelayedTick() {
        await this.initEmitters();
        await this.intRunWorkerEmitter.checkRuns();
        await this.intStreamWorkerEmitter.checkStreams();
        await this.dataSinkWorkerEmitter.checkResults();
    }
}
exports.IntegrationTickProcessor = IntegrationTickProcessor;
//# sourceMappingURL=integrationTickProcessor.js.map