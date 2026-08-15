"use strict";
/* eslint-disable @typescript-eslint/dot-notation */
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
/* eslint-disable no-console */
/* eslint-disable import/no-extraneous-dependencies */
const command_line_args_1 = __importDefault(require("command-line-args"));
const command_line_usage_1 = __importDefault(require("command-line-usage"));
const sync_1 = require("csv-parse/sync");
const fs = __importStar(require("fs"));
const uniq_1 = __importDefault(require("lodash/uniq"));
const moment_1 = __importDefault(require("moment"));
const path_1 = __importDefault(require("path"));
const lfx_memberships_1 = require("@crowd/data-access-layer/src/lfx_memberships");
const organizations_1 = require("@crowd/data-access-layer/src/organizations");
const segments_1 = require("@crowd/data-access-layer/src/segments");
const databaseConnection_1 = require("@/database/databaseConnection");
const sequelizeRepository_1 = __importDefault(require("@/database/repositories/sequelizeRepository"));
const options = [
    {
        name: 'help',
        alias: 'h',
        type: Boolean,
        description: 'Print this usage guide.',
    },
    {
        name: 'file',
        alias: 'f',
        type: String,
        description: 'Path to CSV file to import',
    },
    {
        name: 'tenantId',
        alias: 't',
        type: String,
        description: 'Tenant Id. Hint: what you probably need is 875c38bd-2b1b-4e91-ad07-0cfbabb4c49f',
    },
];
const sections = [
    {
        header: `Import LFX Membership `,
        content: 'Merges two members, then unmerges these and cross checks unmerge result with original data.',
    },
    {
        header: 'Options',
        optionList: options,
    },
];
const usage = (0, command_line_usage_1.default)(sections);
const parameters = (0, command_line_args_1.default)(options);
function parseDomains(domains) {
    return (0, uniq_1.default)(domains
        .split(',')
        .map((domain) => domain.trim())
        .filter((domain) => domain.length > 0)
        // the rest if for values that look like this: "andesdigital.cl\n\n--- Merged Data:\n\ andesdigital.cl"
        .flatMap((domain) => domain.split('\n'))
        .filter((domain) => domain.match(/^[a-z0-9.-]+$/)));
}
async function findOrgId(qx, record) {
    let org = await (0, organizations_1.findOrgIdByDomain)(qx, [record['Account Domain']]);
    if (org) {
        return org;
    }
    org = await (0, organizations_1.findOrgIdByDomain)(qx, record['Domain Alias']);
    if (org) {
        return org;
    }
    org = await (0, organizations_1.findOrgIdByDisplayName)(qx, { orgName: record['Account Name'], exact: true });
    if (org) {
        return org;
    }
    org = await (0, organizations_1.findOrgIdByDisplayName)(qx, {
        orgName: record['Account Name'],
        exact: false,
    });
    return org;
}
if (parameters.help || !parameters.file || !parameters.tenantId) {
    console.log(usage);
}
else {
    setImmediate(async () => {
        const prodDb = await (0, databaseConnection_1.databaseInit)();
        const qx = sequelizeRepository_1.default.getQueryExecutor({
            database: prodDb,
        });
        await qx.result(`DELETE FROM "lfxMemberships"`);
        console.log('All records deleted');
        const fileData = fs.readFileSync(path_1.default.resolve(parameters.file), 'latin1');
        const records = (0, sync_1.parse)(fileData, {
            columns: true,
            skip_empty_lines: true,
        });
        console.log('New records:', records.length);
        for (let i = 0; i < records.length; i++) {
            const record = records[i];
            const orgName = record['Account Name'];
            // Exclude individual no account organizations from LF Members
            if (![
                'Individual - No Account',
                'Individual ? No  Account',
                'individual with no account',
            ].includes(orgName)) {
                record['Domain Alias'] = parseDomains(record['Domain Alias']);
                const segment = await (0, segments_1.findProjectGroupByName)(qx, {
                    name: record['Project'],
                });
                const orgId = await findOrgId(qx, record);
                const row = {
                    organizationId: orgId,
                    segmentId: segment === null || segment === void 0 ? void 0 : segment.id,
                    accountName: orgName,
                    parentAccount: record['Parent Account'],
                    project: record['Project'],
                    productName: record['Product Name'],
                    purchaseHistoryName: record['Purchase History Name'],
                    installDate: (0, moment_1.default)(record['Install Date'], 'MM/DD/YYYY').toDate(),
                    usageEndDate: (0, moment_1.default)(record['Usage End Date'], 'MM/DD/YYYY').toDate(),
                    status: record['Status'],
                    priceCurrency: record['Price Currency'],
                    price: parseInt(record['Price'], 10),
                    productFamily: record['Product Family'],
                    tier: record['Tier'],
                    accountDomain: record['Account Domain'],
                    domainAlias: record['Domain Alias'],
                };
                await (0, lfx_memberships_1.insertLfxMembership)(qx, row);
                console.log('Inserted record:', i, orgName);
            }
            else {
                console.log('Ignored Individual - No account:', i, orgName);
            }
        }
        process.exit(0);
    });
}
//# sourceMappingURL=import-lfx-memberships.js.map