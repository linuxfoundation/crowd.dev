"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const command_line_args_1 = __importDefault(require("command-line-args"));
const common_services_1 = require("@crowd/common_services");
const data_access_layer_1 = require("@crowd/data-access-layer");
const database_1 = require("@crowd/data-access-layer/src/database");
const member_segment_affiliations_1 = require("@crowd/data-access-layer/src/member_segment_affiliations");
const utils_1 = require("@crowd/data-access-layer/src/old/apps/merge_suggestions_worker/utils");
const logging_1 = require("@crowd/logging");
const redis_1 = require("@crowd/redis");
const types_1 = require("@crowd/types");
const conf_1 = require("@/conf");
const log = (0, logging_1.getServiceLogger)();
const options = [
    {
        name: 'testRun',
        alias: 't',
        type: Boolean,
        description: 'Run in test mode (limit to 1 batch and 10 members).',
    },
    {
        name: 'afterMemberId',
        alias: 'a',
        type: String,
        description: 'The member ID to start processing after.',
    },
    {
        name: 'batchSize',
        alias: 'b',
        type: Number,
        description: 'The number of members to fetch in each batch.',
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
    var _a, _b, _c;
    const testRun = (_a = parameters.testRun) !== null && _a !== void 0 ? _a : false;
    const BATCH_SIZE = (_b = parameters.batchSize) !== null && _b !== void 0 ? _b : (testRun ? 10 : 500);
    let afterMemberId = (_c = parameters.afterMemberId) !== null && _c !== void 0 ? _c : undefined;
    const db = await (0, database_1.getDbConnection)({
        host: conf_1.DB_CONFIG.writeHost,
        port: conf_1.DB_CONFIG.port,
        database: conf_1.DB_CONFIG.database,
        user: conf_1.DB_CONFIG.username,
        password: conf_1.DB_CONFIG.password,
    });
    const qx = (0, data_access_layer_1.pgpQx)(db);
    const redis = await (0, redis_1.getRedisClient)(conf_1.REDIS_CONFIG, true);
    log.info({ testRun, BATCH_SIZE, afterMemberId }, 'Running script with the following parameters!');
    let hasMore = true;
    while (hasMore) {
        const memberIds = await (0, data_access_layer_1.fetchEmailDomainMemberOrganizationsWithoutDates)(qx, BATCH_SIZE, afterMemberId);
        if (memberIds.length > 0) {
            for (const chunk of (0, utils_1.chunkArray)(memberIds, 50)) {
                await Promise.all(chunk.map(async (memberId) => {
                    if (testRun) {
                        log.info({ memberId }, 'Processing member!');
                    }
                    try {
                        const [existingMemberOrganizations, activityDates] = await Promise.all([
                            (0, data_access_layer_1.fetchMemberOrganizationsBySource)(qx, memberId, types_1.OrganizationSource.EMAIL_DOMAIN),
                            (0, data_access_layer_1.fetchEmailDomainMemberOrganizationActivityDates)(qx, memberId),
                        ]);
                        const changes = (0, common_services_1.inferMemberOrganizationStintChanges)(memberId, existingMemberOrganizations, activityDates);
                        if (testRun) {
                            log.info({ existingMemberOrganizations, activityDates, changes }, 'Previewing changes for member.');
                        }
                        if (changes.length > 0) {
                            await qx.tx(async (tx) => {
                                for (const change of changes) {
                                    if (change.type === 'insert') {
                                        const memberOrganizationId = await (0, data_access_layer_1.createMemberOrganization)(tx, memberId, {
                                            organizationId: change.organizationId,
                                            dateStart: change.dateStart,
                                            dateEnd: change.dateEnd,
                                            source: types_1.OrganizationSource.EMAIL_DOMAIN,
                                        });
                                        const orgAffiliationPolicyById = await (0, data_access_layer_1.fetchManyOrganizationAffiliationPolicies)(tx, [change.organizationId]);
                                        if (memberOrganizationId &&
                                            orgAffiliationPolicyById.get(change.organizationId)) {
                                            await (0, data_access_layer_1.changeMemberOrganizationAffiliationOverrides)(tx, [
                                                {
                                                    memberId,
                                                    memberOrganizationId,
                                                    allowAffiliation: false,
                                                },
                                            ]);
                                            await (0, member_segment_affiliations_1.deleteMemberSegmentAffiliations)(tx, {
                                                memberId,
                                                organizationId: change.organizationId,
                                            });
                                        }
                                    }
                                    else if (change.type === 'update') {
                                        await (0, data_access_layer_1.updateMemberOrganization)(tx, memberId, change.id, {
                                            dateStart: change.dateStart,
                                            dateEnd: change.dateEnd,
                                        });
                                    }
                                    if (testRun) {
                                        log.info({ memberId, orgId: change.organizationId, type: change.type }, 'Member organization updated.');
                                    }
                                }
                            });
                            await redis.sAdd('recalculate-member-affiliations', [memberId]);
                        }
                        else if (testRun) {
                            log.info({ memberId }, 'No changes found for member!');
                        }
                    }
                    catch (err) {
                        log.error({ memberId, err }, 'Failed to process for member!');
                        throw err;
                    }
                }));
            }
            const lastMemberId = memberIds[memberIds.length - 1];
            afterMemberId = lastMemberId;
            log.info({ lastMemberId, count: memberIds.length }, 'Batch processed!');
            if (testRun || memberIds.length < BATCH_SIZE) {
                hasMore = false;
            }
        }
        else {
            hasMore = false;
        }
    }
    process.exit(0);
});
//# sourceMappingURL=backfill-email-domain-member-organization-dates.js.map