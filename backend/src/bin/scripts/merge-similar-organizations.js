"use strict";
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
const command_line_args_1 = __importDefault(require("command-line-args"));
const command_line_usage_1 = __importDefault(require("command-line-usage"));
const fs = __importStar(require("fs"));
const path_1 = __importDefault(require("path"));
const sequelize_1 = require("sequelize");
const repo_1 = require("@crowd/data-access-layer/src/mergeActions/repo");
const types_1 = require("@crowd/types");
const sequelizeQueryExecutor_1 = require("@/database/sequelizeQueryExecutor");
const getUserContext_1 = __importDefault(require("@/database/utils/getUserContext"));
const organizationService_1 = __importDefault(require("@/services/organizationService"));
const tenantService_1 = __importDefault(require("@/services/tenantService"));
const sequelizeRepository_1 = __importDefault(require("../../database/repositories/sequelizeRepository"));
/* eslint-disable no-console */
const banner = fs.readFileSync(path_1.default.join(__dirname, 'banner.txt'), 'utf8');
const options = [
    {
        name: 'tenant',
        alias: 't',
        type: String,
        description: 'The unique ID of tenant',
    },
    {
        name: 'allTenants',
        alias: 'a',
        type: Boolean,
        defaultValue: false,
        description: 'Set this flag to merge similar organizations for all tenants.',
    },
    {
        name: 'similarityThreshold',
        alias: 's',
        type: String,
        defaultValue: false,
        description: 'Similarity threshold of organization merge suggestions. Suggestions lower than this value will not be merged. Defaults to 0.95',
    },
    {
        name: 'hardLimit',
        alias: 'l',
        type: String,
        defaultValue: false,
        description: `Hard limit for # of organizations that'll be merged. Mostly a flag for testing purposes.`,
    },
    {
        name: 'help',
        alias: 'h',
        type: Boolean,
        description: 'Print this usage guide.',
    },
];
const sections = [
    {
        content: banner,
        raw: true,
    },
    {
        header: 'Merge organizations with similarity higher than given threshold.',
        content: 'Merge organizations with similarity higher than given threshold.',
    },
    {
        header: 'Options',
        optionList: options,
    },
];
const usage = (0, command_line_usage_1.default)(sections);
const parameters = (0, command_line_args_1.default)(options);
if (parameters.help || (!parameters.tenant && !parameters.allTenants)) {
    console.log(usage);
}
else {
    setImmediate(async () => {
        const options = await sequelizeRepository_1.default.getDefaultIRepositoryOptions();
        let tenantIds;
        if (parameters.allTenants) {
            tenantIds = (await tenantService_1.default._findAndCountAllForEveryUser({})).rows.map((t) => t.id);
        }
        else if (parameters.tenant) {
            tenantIds = parameters.tenant.split(',');
        }
        else {
            tenantIds = [];
        }
        for (const tenantId of tenantIds) {
            const userContext = await (0, getUserContext_1.default)(tenantId);
            const orgService = new organizationService_1.default(userContext);
            let hasMoreData = true;
            let counter = 0;
            while (hasMoreData) {
                // find organization merge suggestions of tenant
                const result = await options.database.sequelize.query(`
                SELECT 
                "ot"."organizationId", 
                "ot"."toMergeId", 
                "ot".similarity, 
                "ot".status,
                "org1"."displayName" AS "orgDisplayName",
                "org2"."displayName" AS "mergeDisplayName"
                FROM 
                    "organizationToMerge" "ot"
                LEFT JOIN 
                    "organizations" "org1" 
                ON 
                    "ot"."organizationId" = "org1"."id"
                LEFT JOIN 
                    "organizations" "org2" 
                ON 
                    "ot"."toMergeId" = "org2"."id"
                WHERE 
                    ("ot".similarity > :similarityThreshold) AND
                    ("org1"."displayName" ilike "org2"."displayName") AND
                    ("org1"."tenantId" = :tenantId) AND
                    ("org2"."tenantId" = :tenantId)
                ORDER BY 
                    "ot".similarity DESC
                LIMIT 100 
                OFFSET :offset;`, {
                    replacements: {
                        similarityThreshold: parameters.similarityThreshold || 0.95,
                        offset: 0,
                        tenantId,
                    },
                    type: sequelize_1.QueryTypes.SELECT,
                });
                if (result.length === 0) {
                    hasMoreData = false;
                }
                else {
                    for (const row of result) {
                        try {
                            console.log(`Merging [${row.organizationId}] "${row.orgDisplayName}" into ${row.toMergeId} "${row.mergeDisplayName}"...`);
                            await (0, repo_1.addMergeAction)((0, sequelizeQueryExecutor_1.optionsQx)(userContext), types_1.MergeActionType.ORG, row.organizationId, row.toMergeId, undefined);
                            await orgService.mergeSync(row.organizationId, row.toMergeId, null);
                        }
                        catch (err) {
                            console.log('Error merging organizations - continuing with the rest', err);
                            await (0, repo_1.setMergeAction)((0, sequelizeQueryExecutor_1.optionsQx)(userContext), types_1.MergeActionType.ORG, row.organizationId, row.toMergeId, {
                                state: types_1.MergeActionState.ERROR,
                            });
                        }
                        if (parameters.hardLimit && counter >= parameters.hardLimit) {
                            console.log(`Hard limit of ${parameters.hardLimit} reached. Exiting...`);
                            process.exit(0);
                        }
                        counter += 1;
                    }
                }
            }
        }
        process.exit(0);
    });
}
//# sourceMappingURL=merge-similar-organizations.js.map