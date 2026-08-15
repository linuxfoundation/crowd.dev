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
const logging_1 = require("@crowd/logging");
const organizationRepository_1 = __importDefault(require("@/database/repositories/organizationRepository"));
const organizationService_1 = __importDefault(require("@/services/organizationService"));
const sequelizeRepository_1 = __importDefault(require("../../database/repositories/sequelizeRepository"));
/* eslint-disable no-console */
const banner = fs.readFileSync(path_1.default.join(__dirname, 'banner.txt'), 'utf8');
const log = (0, logging_1.getServiceLogger)();
const options = [
    {
        name: 'tenantId',
        alias: 't',
        typeLabel: '{underline tenantId}',
        type: String,
        description: 'The tenantId both organizations belongs to',
    },
    {
        name: 'originalId',
        alias: 'o',
        typeLabel: '{underline originalId}',
        type: String,
        description: 'The unique ID of an organization that will be kept. The other will be merged into this one.',
    },
    {
        name: 'toMergeId',
        alias: 'm',
        typeLabel: '{underline toMergeId}',
        type: String,
        description: 'The unique ID of an organization that will be merged into the first one. This one will be destroyed. You can provide multiple ids here separated by comma.',
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
        header: 'Merge two organizations',
        content: 'Merge two organizations so that only one remains. The other one will be destroyed.',
    },
    {
        header: 'Options',
        optionList: options,
    },
];
const usage = (0, command_line_usage_1.default)(sections);
const parameters = (0, command_line_args_1.default)(options);
if (parameters.help || !parameters.originalId || !parameters.toMergeId || !parameters.tenantId) {
    console.log(usage);
}
else {
    setImmediate(async () => {
        const tenantId = parameters.tenantId;
        const originalId = parameters.originalId;
        const toMergeIds = parameters.toMergeId.split(',');
        const options = await sequelizeRepository_1.default.getDefaultIRepositoryOptions();
        options.currentTenant = { id: tenantId };
        const originalOrganizaton = await organizationRepository_1.default.findById(originalId, options);
        for (const toMergeId of toMergeIds) {
            const toMergeOrganization = await organizationRepository_1.default.findById(toMergeId, options);
            if (originalOrganizaton.tenantId !== toMergeOrganization.tenantId) {
                log.error(`Organizations ${originalId} and ${toMergeId} are not from the same tenant. Will not merge!`);
            }
            else {
                log.info(`Merging ${toMergeId} into ${originalId}...`);
                const service = new organizationService_1.default(options);
                try {
                    await service.mergeSync(originalId, toMergeId, null);
                }
                catch (err) {
                    log.error(`Error merging organizations: ${err.message}`);
                    process.exit(1);
                }
            }
        }
        process.exit(0);
    });
}
//# sourceMappingURL=merge-organizations.js.map