"use strict";
/* eslint-disable @typescript-eslint/dot-notation */
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
/* eslint-disable no-console */
/* eslint-disable import/no-extraneous-dependencies */
const command_line_args_1 = __importDefault(require("command-line-args"));
const command_line_usage_1 = __importDefault(require("command-line-usage"));
const databaseConnection_1 = require("@/database/databaseConnection");
const organizationRepository_1 = __importDefault(require("@/database/repositories/organizationRepository"));
const sequelizeRepository_1 = __importDefault(require("@/database/repositories/sequelizeRepository"));
const options = [
    {
        name: 'help',
        alias: 'h',
        type: Boolean,
        description: 'Print this usage guide.',
    },
    {
        name: 'tenantId',
        alias: 't',
        type: String,
        description: 'Tenant Id',
    },
];
const sections = [
    {
        header: `Fix empty displayName in organizations`,
        content: 'Script will fix organizations with empty displayName',
    },
    {
        header: 'Options',
        optionList: options,
    },
];
const usage = (0, command_line_usage_1.default)(sections);
const parameters = (0, command_line_args_1.default)(options);
function getOrgsWithoutDisplayName(qx, tenantId, { limit = 50, countOnly = false }) {
    return qx.select(`
        SELECT
        ${countOnly ? 'COUNT(*)' : 'o.id'}
        FROM organizations o
        WHERE o."tenantId" = $(tenantId)
        AND o."displayName" IS NULL
        ${countOnly ? '' : 'LIMIT $(limit)'}
    `, { tenantId, limit });
}
async function getOrgIdentities(qx, orgId, tenantId) {
    return qx.select(`
      SELECT value
      FROM "organizationIdentities"
      WHERE "organizationId" = $(orgId)
      AND "tenantId" = $(tenantId)
      LIMIT 1
    `, { orgId, tenantId });
}
async function getOrgAttributes(qx, orgId) {
    return qx.select(`
      SELECT value
      FROM "orgAttributes"
      WHERE "organizationId" = $(orgId)
      AND name = 'name'
      LIMIT 1
    `, { orgId });
}
async function updateOrgDisplayName(qx, orgId, displayName) {
    await qx.result(`
      UPDATE organizations
      SET "displayName" = $(displayName)
      WHERE id = $(id)
    `, { id: orgId, displayName });
}
if (parameters.help || !parameters.tenantId) {
    console.log(usage);
}
else {
    setImmediate(async () => {
        var _a, _b;
        const prodDb = await (0, databaseConnection_1.databaseInit)();
        const tenantId = parameters.tenantId;
        const qx = sequelizeRepository_1.default.getQueryExecutor({
            database: prodDb,
        });
        const options = await sequelizeRepository_1.default.getDefaultIRepositoryOptions();
        const BATCH_SIZE = 50;
        let processed = 0;
        const totalOrgs = await getOrgsWithoutDisplayName(qx, tenantId, { countOnly: true });
        console.log(`Total organizations without displayName: ${totalOrgs[0].count}`);
        let orgs = await getOrgsWithoutDisplayName(qx, tenantId, { limit: BATCH_SIZE });
        while (totalOrgs[0].count > processed) {
            for (const org of orgs) {
                let displayName;
                let updateAttributes = false;
                const attributes = await getOrgAttributes(qx, org.id);
                if (attributes.length > 0) {
                    displayName = (_a = attributes[0]) === null || _a === void 0 ? void 0 : _a.value;
                }
                else {
                    const identities = await getOrgIdentities(qx, org.id, tenantId);
                    displayName = identities && ((_b = identities[0]) === null || _b === void 0 ? void 0 : _b.value);
                    updateAttributes = true;
                }
                if (displayName) {
                    await updateOrgDisplayName(qx, org.id, displayName);
                    if (updateAttributes) {
                        await organizationRepository_1.default.updateOrgAttributes(org.id, {
                            attributes: {
                                name: {
                                    custom: [displayName],
                                    default: displayName,
                                },
                            },
                        }, options);
                    }
                }
                else {
                    console.log(`Organization ${org.id} does not have displayName`);
                }
                processed++;
            }
            console.log(`Processed ${processed}/${totalOrgs[0].count} organizations`);
            orgs = await getOrgsWithoutDisplayName(qx, tenantId, { limit: BATCH_SIZE });
        }
        process.exit(0);
    });
}
//# sourceMappingURL=fix-missing-org-displayName.js.map