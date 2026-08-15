"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const command_line_args_1 = __importDefault(require("command-line-args"));
const common_services_1 = require("@crowd/common_services");
const database_1 = require("@crowd/data-access-layer/src/database");
const logging_1 = require("@crowd/logging");
const temporal_1 = require("@crowd/temporal");
const conf_1 = require("@/conf");
const log = (0, logging_1.getServiceLogger)();
const options = [
    {
        name: 'organizationId',
        alias: 'o',
        typeLabel: '{underline organizationId}',
        type: String,
        description: 'The organization ID to process members for.',
    },
    {
        name: 'dryRun',
        alias: 'd',
        type: Boolean,
        description: 'Run in dry-run mode (show what would be processed).',
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
    const organizationId = parameters.organizationId;
    const dryRun = (_a = parameters.dryRun) !== null && _a !== void 0 ? _a : false;
    log.info({ organizationId, dryRun }, 'Running script with the following parameters!');
    const db = await (0, database_1.getDbConnection)({
        host: conf_1.DB_CONFIG.readHost,
        port: conf_1.DB_CONFIG.port,
        database: conf_1.DB_CONFIG.database,
        user: conf_1.DB_CONFIG.username,
        password: conf_1.DB_CONFIG.password,
    });
    const temporal = await (0, temporal_1.getTemporalClient)(conf_1.TEMPORAL_CONFIG);
    try {
        const memberIds = await db.any(`
      SELECT DISTINCT ar."memberId" AS id
      FROM "activityRelations" ar
      JOIN "memberOrganizations" mo
        ON ar."memberId" = mo."memberId"
        AND ar."organizationId" = mo."organizationId"
      LEFT JOIN "memberOrganizationAffiliationOverrides" moao
        ON mo."id" = moao."memberOrganizationId"
      WHERE ar."organizationId" = $1
        AND (
          (
            mo."deletedAt" IS NOT NULL
            AND NOT EXISTS (
              SELECT 1
              FROM "memberOrganizations" mo2
              WHERE mo2."memberId" = mo."memberId"
                AND mo2."organizationId" = mo."organizationId"
                AND mo2."deletedAt" IS NULL
            )
          )
          OR (
            mo."deletedAt" IS NULL
            AND moao."allowAffiliation" = false
          )
        );
      `, [organizationId]);
        log.info(`Found ${memberIds.length} members to process`);
        if (memberIds.length === 0) {
            log.info('No members found. Implement the query to get actual memberIds.');
            return;
        }
        if (dryRun) {
            log.info('DRY RUN - Would update affiliations for the following members:');
            memberIds.forEach((member) => {
                log.info(`  - Member ID: ${member.id}`);
            });
            return;
        }
        let processedCount = 0;
        for (const member of memberIds) {
            try {
                log.info(`Processing member: ${member.id}`);
                await (0, common_services_1.signalMemberUpdate)(temporal, member.id, {
                    memberOrganizationIds: [organizationId],
                });
                processedCount++;
                log.info(`Successfully triggered workflow for member: ${member.id}`);
            }
            catch (error) {
                log.error(`Failed to process member ${member.id}:`, error);
            }
        }
        log.info(`Script completed. Processed ${processedCount}/${memberIds.length} members.`);
    }
    catch (error) {
        log.error('Script failed:', error);
        throw error;
    }
    process.exit(0);
});
//# sourceMappingURL=fix-members-activities-after-unaffilation.js.map