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
const common_1 = require("@crowd/common");
const common_services_1 = require("@crowd/common_services");
const members_1 = require("@crowd/data-access-layer/src/members");
const logging_1 = require("@crowd/logging");
const sequelizeQueryExecutor_1 = require("@/database/sequelizeQueryExecutor");
const sequelizeRepository_1 = __importDefault(require("../../database/repositories/sequelizeRepository"));
/* eslint-disable no-console */
const banner = fs.readFileSync(path_1.default.join(__dirname, 'banner.txt'), 'utf8');
const log = (0, logging_1.getServiceLogger)();
const options = [
    {
        name: 'originalId',
        alias: 'o',
        typeLabel: '{underline originalId}',
        type: String,
        description: 'The unique ID of a member that will be kept. The other will be merged into this one.',
    },
    {
        name: 'targetId',
        alias: 't',
        typeLabel: '{underline targetId}',
        type: String,
        description: 'The unique ID of a member that will be merged into the first one. This one will be destroyed. You can provide multiple ids here separated by comma.',
    },
    {
        name: 'userId',
        alias: 'u',
        typeLabel: '{underline userId}',
        type: String,
        description: 'User ID of the user performing the merge.',
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
        header: 'Merge two members',
        content: 'Merge two members so that only one remains. The other one will be destroyed.',
    },
    {
        header: 'Options',
        optionList: options,
    },
];
const usage = (0, command_line_usage_1.default)(sections);
const parameters = (0, command_line_args_1.default)(options);
if (parameters.help || !parameters.originalId || !parameters.targetId) {
    console.log(usage);
}
else {
    setImmediate(async () => {
        const originalId = parameters.originalId;
        const targetIds = parameters.targetId.split(',');
        const userId = parameters.userId;
        const options = await sequelizeRepository_1.default.getDefaultIRepositoryOptions();
        const qx = sequelizeRepository_1.default.getQueryExecutor(options);
        const originalMember = await (0, members_1.findMemberById)(qx, originalId, [
            members_1.MemberField.ID,
            members_1.MemberField.TENANT_ID,
        ]);
        options.currentTenant = { id: originalMember.tenantId };
        options.currentUser = { id: userId };
        const ctx = {
            ...options,
            requestId: (0, common_1.generateUUIDv1)(),
            userData: {
                ip: '127.0.0.1',
                userAgent: 'merge-members-script',
            },
        };
        for (const targetId of targetIds) {
            const targetMember = await (0, members_1.findMemberById)(qx, targetId, [
                members_1.MemberField.ID,
                members_1.MemberField.TENANT_ID,
            ]);
            if (originalMember.tenantId !== targetMember.tenantId) {
                log.error(`Members ${originalId} and ${targetId} are not from the same tenant. Will not merge!`);
            }
            else {
                log.info(`Merging ${targetId} into ${originalId}...`);
                const service = new common_services_1.CommonMemberService((0, sequelizeQueryExecutor_1.optionsQx)(options), options.temporal, log);
                try {
                    await service.merge(originalId, targetId, ctx);
                }
                catch (err) {
                    log.error(`Error merging members: ${err.message}`);
                    process.exit(1);
                }
            }
        }
        process.exit(0);
    });
}
//# sourceMappingURL=merge-members.js.map