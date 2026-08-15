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
/* eslint-disable no-continue */
const lodash = __importStar(require("lodash"));
const audit_logs_1 = require("@crowd/audit-logs");
const common_1 = require("@crowd/common");
const members_1 = require("@crowd/data-access-layer/src/members");
const logging_1 = require("@crowd/logging");
const sequelizeRepository_1 = __importDefault(require("@/database/repositories/sequelizeRepository"));
class MemberAttributesService extends logging_1.LoggerBase {
    constructor(options) {
        super(options.log);
        this.options = options;
    }
    async list(memberId) {
        const qx = sequelizeRepository_1.default.getQueryExecutor(this.options);
        const result = await (0, members_1.fetchMemberAttributes)(qx, memberId);
        if (!result) {
            throw new common_1.Error404('Attributes not found for the given member!');
        }
        return result;
    }
    async update(memberId, data, manuallyChanged) {
        return (0, audit_logs_1.captureApiChange)(this.options, (0, audit_logs_1.memberEditProfileAction)(memberId, async (captureOldState, captureNewState) => {
            var _a, _b, _c, _d, _e, _f;
            const repoOptions = await sequelizeRepository_1.default.createTransactionalRepositoryOptions(this.options);
            const tx = repoOptions.transaction;
            const qx = sequelizeRepository_1.default.getQueryExecutor(repoOptions);
            try {
                const currentMemberAttributes = await (0, members_1.fetchMemberAttributes)(qx, memberId);
                if (!currentMemberAttributes) {
                    throw new common_1.Error404('Attributes not found for the given member during update!');
                }
                captureOldState({ attributes: currentMemberAttributes });
                const existingManuallyChangedFields = await (0, members_1.getMemberManuallyChangedFields)(qx, memberId);
                const updatedManuallyChangedFields = [...existingManuallyChangedFields];
                for (const key of Object.keys(data)) {
                    if (!currentMemberAttributes[key] ||
                        !lodash.isEqual(currentMemberAttributes[key].default, data[key].default)) {
                        const fieldName = `attributes.${key}`;
                        if (!updatedManuallyChangedFields.includes(fieldName)) {
                            updatedManuallyChangedFields.push(fieldName);
                        }
                    }
                }
                if (!(0, common_1.hasAttributeValue)(data.country)) {
                    const location = (0, common_1.getAttributeValue)(data.location);
                    const country = (0, common_1.getCountry)(location);
                    if (country) {
                        data.country = {
                            ...data.country,
                            system: country,
                            default: country,
                        };
                    }
                }
                await (0, members_1.updateMemberAttributes)(qx, memberId, data);
                // Handle isBot status and maintain consistency with bot tracking tables
                if (Object.keys(data).includes('isBot')) {
                    const newIsBot = (_b = (_a = data.isBot) === null || _a === void 0 ? void 0 : _a.default) !== null && _b !== void 0 ? _b : false;
                    const currentDefaultIsBot = (_d = (_c = currentMemberAttributes.isBot) === null || _c === void 0 ? void 0 : _c.default) !== null && _d !== void 0 ? _d : false;
                    // Only exists if system flagged member as a bot
                    const currentSystemIsBot = (_f = (_e = currentMemberAttributes.isBot) === null || _e === void 0 ? void 0 : _e.system) !== null && _f !== void 0 ? _f : false;
                    // When user sets isBot to false, always clean up
                    if (!newIsBot) {
                        // Clean up any bot suggestions if exists
                        await (0, members_1.deleteMemberBotSuggestion)(qx, memberId);
                        // If system previously flagged them as bot, prevent future detection
                        if (currentSystemIsBot) {
                            await (0, members_1.insertMemberNoBot)(qx, memberId);
                        }
                    }
                    // When user sets isBot to true, clean up any existing entries
                    else if (newIsBot && !currentDefaultIsBot) {
                        // Clean up existing bot suggestions and no-bot entries
                        await Promise.all([
                            (0, members_1.deleteMemberBotSuggestion)(qx, memberId),
                            (0, members_1.deleteMemberNoBot)(qx, memberId),
                        ]);
                    }
                }
                if (manuallyChanged) {
                    await (0, members_1.setMemberManuallyChangedFields)(qx, memberId, updatedManuallyChangedFields);
                }
                const updatedAttributes = await (0, members_1.fetchMemberAttributes)(qx, memberId);
                captureNewState({ attributes: updatedAttributes });
                await sequelizeRepository_1.default.commitTransaction(tx);
                return updatedAttributes;
            }
            catch (error) {
                await sequelizeRepository_1.default.rollbackTransaction(tx);
                throw error;
            }
        }));
    }
}
exports.default = MemberAttributesService;
//# sourceMappingURL=memberAttributesService.js.map