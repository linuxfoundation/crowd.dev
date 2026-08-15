"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.checkRuns = exports.isRunStuck = exports.checkStuckIntegrations = void 0;
const cron_time_generator_1 = __importDefault(require("cron-time-generator"));
const moment_1 = __importDefault(require("moment"));
const logging_1 = require("@crowd/logging");
const types_1 = require("@crowd/types");
const conf_1 = require("../../conf");
const integrationRepository_1 = __importDefault(require("../../database/repositories/integrationRepository"));
const integrationRunRepository_1 = __importDefault(require("../../database/repositories/integrationRunRepository"));
const integrationStreamRepository_1 = __importDefault(require("../../database/repositories/integrationStreamRepository"));
const sequelizeRepository_1 = __importDefault(require("../../database/repositories/sequelizeRepository"));
const integrationStreamTypes_1 = require("../../types/integrationStreamTypes");
const log = (0, logging_1.getServiceChildLogger)('checkStuckIntegrationRuns');
// we are checking "integrationRuns"."updatedAt" column
const THRESHOLD_HOURS = 1;
let running = false;
const checkStuckIntegrations = async () => {
    // find integrations that are in-progress but their last integration run is:
    // in final state (processed or error) and it has no streams
    // this happens when integration run is triggered but for some reason fails before streams are generated
    const dbOptions = await sequelizeRepository_1.default.getDefaultIRepositoryOptions();
    const runsRepo = new integrationRunRepository_1.default(dbOptions);
    const streamsRepo = new integrationStreamRepository_1.default(dbOptions);
    let inProgressIntegrations = await integrationRepository_1.default.findByStatus('in-progress', 1, 10, dbOptions);
    while (inProgressIntegrations.length > 0) {
        log.info(`Found ${inProgressIntegrations.length} integrations in progress!`);
        for (const integration of inProgressIntegrations) {
            const lastRun = await runsRepo.findLastRun(integration.id);
            if (lastRun.state === types_1.IntegrationRunState.PROCESSED ||
                lastRun.state === types_1.IntegrationRunState.ERROR) {
                const streams = await streamsRepo.findByRunId(lastRun.id, 1, 1);
                if (streams.length === 0) {
                    log.info(`Found integration ${integration.id} in progress but last run ${lastRun.id} is in final state and has no streams! Restarting the run!`);
                    await runsRepo.restart(lastRun.id);
                    const delayUntil = (0, moment_1.default)().add(1, 'second').toDate();
                    await runsRepo.delay(lastRun.id, delayUntil);
                }
            }
        }
        inProgressIntegrations = await integrationRepository_1.default.findByStatus('in-progress', 1, 10, dbOptions);
    }
};
exports.checkStuckIntegrations = checkStuckIntegrations;
const isRunStuck = async (run, streamsRepo, runsRepo, logger) => {
    const now = (0, moment_1.default)();
    // let's first check if the integration run itself is older than 3 hours
    // we are updating updatedAt at the end of the integration run when we process all streams or if it's stopped by rate limit/delays
    // so it should be a good indicator if the integration run is stuck
    // but because some integrations are really long running it must not be the only one
    const integrationLastUpdatedAt = (0, moment_1.default)(run.updatedAt);
    const diff = now.diff(integrationLastUpdatedAt, 'hours');
    if (diff < THRESHOLD_HOURS) {
        return false;
    }
    log.warn({ runId: run.id }, 'Investigating possible stuck integration run!');
    // first lets check if the we have any integration streams that are in state processing
    // and if we have let's see when were they moved to that state based on updatedAt column
    // if they are older than 3 hours we will reset them to pending state and start integration back up
    const processingStreams = await streamsRepo.findByRunId(run.id, 1, 1, [integrationStreamTypes_1.IntegrationStreamState.PROCESSING], '"updatedAt" desc', [`"updatedAt" < now() - interval '${THRESHOLD_HOURS} hours'`]);
    let stuck = false;
    if (processingStreams.length > 0) {
        const stream = processingStreams[0];
        logger.warn({ streamId: stream.id }, 'Found stuck processing stream! Reseting all processing streams!');
        stuck = true;
        let streamsToRestart = await streamsRepo.findByRunId(run.id, 1, 10, [
            integrationStreamTypes_1.IntegrationStreamState.PROCESSING,
        ]);
        while (streamsToRestart.length > 0) {
            for (const stream of streamsToRestart) {
                await streamsRepo.reset(stream.id);
            }
            streamsToRestart = await streamsRepo.findByRunId(run.id, 1, 10, [
                integrationStreamTypes_1.IntegrationStreamState.PROCESSING,
            ]);
        }
    }
    // if there were no processing streams lets check if we have pending streams that are older than 3 hours
    if (!stuck) {
        const pendingStreams = await streamsRepo.findByRunId(run.id, 1, 1, [integrationStreamTypes_1.IntegrationStreamState.PENDING], '"updatedAt" desc', [`"updatedAt" < now() - interval '${THRESHOLD_HOURS} hours'`]);
        if (pendingStreams.length > 0) {
            const stream = pendingStreams[0];
            logger.warn({ streamId: stream.id }, 'Found stuck pending stream!');
            stuck = true;
        }
    }
    // and the last check is to see whether we have any errored streams that are older than 3 hours and haven't been retried enough times
    if (!stuck) {
        const errorStreams = await streamsRepo.findByRunId(run.id, 1, 1, [integrationStreamTypes_1.IntegrationStreamState.ERROR], '"updatedAt" desc', [
            `"updatedAt" < now() - interval '${THRESHOLD_HOURS} hours'`,
            `"retries" < ${conf_1.INTEGRATION_PROCESSING_CONFIG.maxRetries}`,
        ]);
        if (errorStreams.length > 0) {
            logger.warn(`Found errored streams with not enough retries!`);
            const stream = errorStreams[0];
            logger.warn({ streamId: stream.id }, 'Found stuck errored stream!');
            stuck = true;
        }
    }
    // this check tries to see whether the integration run is actually finished but it's in a wrong state
    // by checking all streams and determining whether they are in a final state
    if (!stuck) {
        // check if there are any streams that are not in a final state
        const notFinalStreams = await streamsRepo.findByRunId(run.id, 1, 1, [
            integrationStreamTypes_1.IntegrationStreamState.PENDING,
            integrationStreamTypes_1.IntegrationStreamState.PROCESSING,
        ]);
        if (notFinalStreams.length === 0) {
            logger.warn('Found no streams in a final state! Setting integration to either error or processed!');
            const state = await runsRepo.touchState(run.id);
            if (state !== types_1.IntegrationRunState.ERROR && state !== types_1.IntegrationRunState.PROCESSED) {
                logger.error('Integration is not in a final state! Requires manual intervention!');
            }
        }
    }
    return stuck;
};
exports.isRunStuck = isRunStuck;
const checkRuns = async () => {
    const dbOptions = await sequelizeRepository_1.default.getDefaultIRepositoryOptions();
    const runsRepo = new integrationRunRepository_1.default(dbOptions);
    const streamsRepo = new integrationStreamRepository_1.default(dbOptions);
    let runs = await runsRepo.findIntegrationsByState([types_1.IntegrationRunState.PENDING, types_1.IntegrationRunState.PROCESSING], 1, 10);
    while (runs.length > 0) {
        for (const run of runs) {
            const logger = (0, logging_1.getChildLogger)('fixer', log, { runId: run.id });
            const stuck = await (0, exports.isRunStuck)(run, streamsRepo, runsRepo, logger);
            if (stuck) {
                logger.warn('Delaying integration for 1 second to restart it!');
                const delayUntil = (0, moment_1.default)().add(1, 'second').toDate();
                await runsRepo.delay(run.id, delayUntil);
            }
        }
        const lastCreatedAt = runs[runs.length - 1].createdAt;
        runs = await runsRepo.findIntegrationsByState([types_1.IntegrationRunState.PENDING, types_1.IntegrationRunState.PROCESSING], 1, 10, lastCreatedAt);
    }
};
exports.checkRuns = checkRuns;
const job = {
    name: 'Detect & Fix Stuck Integration Runs',
    cronTime: cron_time_generator_1.default.every(90).minutes(),
    onTrigger: async () => {
        if (!running) {
            running = true;
            try {
                await Promise.all([(0, exports.checkRuns)(), (0, exports.checkStuckIntegrations)()]);
            }
            finally {
                running = false;
            }
        }
    },
};
exports.default = job;
//# sourceMappingURL=checkStuckIntegrationRuns.js.map