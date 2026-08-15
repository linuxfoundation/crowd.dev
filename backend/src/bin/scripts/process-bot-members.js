"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const command_line_args_1 = __importDefault(require("command-line-args"));
const common_1 = require("@crowd/common");
const data_access_layer_1 = require("@crowd/data-access-layer");
const database_1 = require("@crowd/data-access-layer/src/database");
const utils_1 = require("@crowd/data-access-layer/src/old/apps/merge_suggestions_worker/utils");
const logging_1 = require("@crowd/logging");
const temporal_1 = require("@crowd/temporal");
const conf_1 = require("@/conf");
const log = (0, logging_1.getServiceLogger)();
const options = [
    {
        name: 'testRun',
        alias: 't',
        type: Boolean,
        description: 'Run in test mode (limit to 10 members).',
    },
    {
        name: 'help',
        alias: 'h',
        type: Boolean,
        description: 'Print this usage guide.',
    },
];
const parameters = (0, command_line_args_1.default)(options);
setImmediate(async () => {
    var _a;
    const testRun = (_a = parameters.testRun) !== null && _a !== void 0 ? _a : false;
    const BATCH_SIZE = testRun ? 10 : 100;
    const db = await (0, database_1.getDbConnection)({
        host: conf_1.DB_CONFIG.readHost,
        port: conf_1.DB_CONFIG.port,
        database: conf_1.DB_CONFIG.database,
        user: conf_1.DB_CONFIG.username,
        password: conf_1.DB_CONFIG.password,
    });
    const qx = (0, data_access_layer_1.pgpQx)(db);
    const temporal = await (0, temporal_1.getTemporalClient)(conf_1.TEMPORAL_CONFIG);
    log.info({ testRun, BATCH_SIZE }, 'Running script with the following parameters!');
    let botLikeMembers = [];
    do {
        botLikeMembers = await (0, data_access_layer_1.fetchBotCandidateMembers)(qx, BATCH_SIZE);
        const chunks = (0, utils_1.chunkArray)(botLikeMembers, 10);
        for (const chunk of chunks) {
            // parallel processing
            await Promise.all(chunk.map(async (memberId) => {
                if (testRun) {
                    log.info({ memberId }, 'Triggering workflow for member!');
                }
                try {
                    await temporal.workflow.start('processMemberBotAnalysisWithLLM', {
                        taskQueue: 'profiles',
                        workflowId: `member-bot-analysis-with-llm/${memberId}`,
                        retry: {
                            maximumAttempts: 10,
                        },
                        args: [{ memberId }],
                        searchAttributes: {
                            TenantId: [common_1.DEFAULT_TENANT_ID],
                        },
                    });
                    // wait till the workflow is finished
                    await temporal.workflow.result(`member-bot-analysis-with-llm/${memberId}`);
                }
                catch (err) {
                    log.error({ memberId, err }, 'Failed to trigger workflow for member!');
                    throw err;
                }
            }));
        }
        if (testRun) {
            log.info('Test run - stopping after first batch!');
            break;
        }
    } while (botLikeMembers.length > 0);
    process.exit(0);
});
//# sourceMappingURL=process-bot-members.js.map