"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
/* eslint-disable no-continue */
const lodash_1 = __importDefault(require("lodash"));
const audit_logs_1 = require("@crowd/audit-logs");
const common_1 = require("@crowd/common");
const data_access_layer_1 = require("@crowd/data-access-layer");
const members_1 = require("@crowd/data-access-layer/src/members");
const logging_1 = require("@crowd/logging");
const sequelizeRepository_1 = __importDefault(require("@/database/repositories/sequelizeRepository"));
const sequelizeQueryExecutor_1 = require("@/database/sequelizeQueryExecutor");
class MemberIdentityService extends logging_1.LoggerBase {
    constructor(options) {
        super(options.log);
        this.options = options;
    }
    // Member identity list
    async list(memberId) {
        const qx = sequelizeRepository_1.default.getQueryExecutor(this.options);
        return (0, members_1.fetchMemberIdentities)(qx, memberId);
    }
    // Member identity creation
    async create(memberId, data) {
        let tx;
        try {
            const list = await (0, audit_logs_1.captureApiChange)(this.options, (0, audit_logs_1.memberEditIdentitiesAction)(memberId, async (captureOldState, captureNewState) => {
                const repoOptions = await sequelizeRepository_1.default.createTransactionalRepositoryOptions(this.options);
                const memberIdentities = (await (0, data_access_layer_1.findIdentitiesForMembers)((0, sequelizeQueryExecutor_1.optionsQx)(repoOptions), [memberId]))
                    .get(memberId)
                    .map((identity) => lodash_1.default.omit(identity, ['createdAt', 'integrationId']));
                captureOldState(lodash_1.default.sortBy(memberIdentities, [(i) => i.platform, (i) => i.type]));
                tx = repoOptions.transaction;
                const qx = sequelizeRepository_1.default.getQueryExecutor(repoOptions);
                // Check if identity already exists
                const conflict = await (0, members_1.findMemberIdentityConflict)(qx, {
                    value: data.value,
                    platform: data.platform,
                    type: data.type,
                });
                if (conflict) {
                    throw new common_1.Error409(this.options.language, 'errors.alreadyExists', 
                    // @ts-ignore
                    JSON.stringify({
                        memberId: conflict.memberId,
                    }));
                }
                // Create member identity
                await (0, data_access_layer_1.insertMemberIdentities)(qx, [{ ...data, memberId }]);
                await (0, members_1.touchMemberUpdatedAt)(qx, memberId);
                // List all member identities
                const list = await (0, members_1.fetchMemberIdentities)(qx, memberId);
                captureNewState(lodash_1.default.sortBy(list, [(i) => i.platform, (i) => i.type]));
                await sequelizeRepository_1.default.commitTransaction(tx);
                return list;
            }));
            return list;
        }
        catch (error) {
            if (tx) {
                await sequelizeRepository_1.default.rollbackTransaction(tx);
            }
            throw error;
        }
    }
    async findById(memberId, id) {
        const qx = sequelizeRepository_1.default.getQueryExecutor(this.options);
        return (0, members_1.findMemberIdentityById)(qx, memberId, id);
    }
    // Member multiple identity creation
    async createMultiple(memberId, data) {
        let tx;
        try {
            const list = await (0, audit_logs_1.captureApiChange)(this.options, (0, audit_logs_1.memberEditIdentitiesAction)(memberId, async (captureOldState, captureNewState) => {
                const repoOptions = await sequelizeRepository_1.default.createTransactionalRepositoryOptions(this.options);
                const memberIdentities = (await (0, data_access_layer_1.findIdentitiesForMembers)((0, sequelizeQueryExecutor_1.optionsQx)(repoOptions), [memberId]))
                    .get(memberId)
                    .map((identity) => lodash_1.default.omit(identity, ['createdAt', 'integrationId']));
                captureOldState(lodash_1.default.sortBy(memberIdentities, [(i) => i.platform, (i) => i.type]));
                tx = repoOptions.transaction;
                const qx = sequelizeRepository_1.default.getQueryExecutor(repoOptions);
                // Check if any of the identities already exist
                for (const identity of data) {
                    const conflict = await (0, members_1.findMemberIdentityConflict)(qx, {
                        value: identity.value,
                        platform: identity.platform,
                        type: identity.type,
                    });
                    if (conflict) {
                        throw new common_1.Error409(this.options.language, 'errors.alreadyExists', 
                        // @ts-ignore
                        JSON.stringify({
                            memberId: conflict.memberId,
                        }));
                    }
                }
                // Create member identities
                await (0, data_access_layer_1.insertMemberIdentities)(qx, data.map((identity) => ({ ...identity, memberId })));
                await (0, members_1.touchMemberUpdatedAt)(qx, memberId);
                // List all member identities
                const list = await (0, members_1.fetchMemberIdentities)(qx, memberId);
                captureNewState(lodash_1.default.sortBy(list, [(i) => i.platform, (i) => i.type]));
                await sequelizeRepository_1.default.commitTransaction(tx);
                return list;
            }));
            return list;
        }
        catch (error) {
            if (tx) {
                await sequelizeRepository_1.default.rollbackTransaction(tx);
            }
            throw error;
        }
    }
    // Update member identity
    async update(id, memberId, data) {
        let tx;
        try {
            const list = await (0, audit_logs_1.captureApiChange)(this.options, (0, audit_logs_1.memberEditIdentitiesAction)(memberId, async (captureOldState, captureNewState) => {
                var _a, _b, _c;
                const repoOptions = await sequelizeRepository_1.default.createTransactionalRepositoryOptions(this.options);
                const memberIdentities = (await (0, data_access_layer_1.findIdentitiesForMembers)((0, sequelizeQueryExecutor_1.optionsQx)(repoOptions), [memberId]))
                    .get(memberId)
                    .map((identity) => lodash_1.default.omit(identity, ['createdAt', 'integrationId']));
                captureOldState(lodash_1.default.sortBy(memberIdentities, [(i) => i.platform, (i) => i.type]));
                tx = repoOptions.transaction;
                const qx = sequelizeRepository_1.default.getQueryExecutor(repoOptions);
                const currentIdentity = memberIdentities.find((identity) => identity.id === id);
                if (!currentIdentity) {
                    throw new common_1.Error404(this.options.language, 'errors.notFound.message');
                }
                const value = (_a = data.value) !== null && _a !== void 0 ? _a : currentIdentity.value;
                const platform = (_b = data.platform) !== null && _b !== void 0 ? _b : currentIdentity.platform;
                const type = (_c = data.type) !== null && _c !== void 0 ? _c : currentIdentity.type;
                const conflict = await (0, members_1.findMemberIdentityConflict)(qx, {
                    value,
                    platform,
                    type,
                    excludeMemberId: memberId,
                });
                if (conflict) {
                    throw new common_1.Error409(this.options.language, 'errors.alreadyExists', 
                    // @ts-ignore
                    JSON.stringify({
                        memberId: conflict.memberId,
                    }));
                }
                // Update member identity with new data
                await (0, members_1.updateMemberIdentity)(qx, memberId, id, {
                    ...data,
                    ...(data.value !== undefined ? { value } : {}),
                });
                await (0, members_1.touchMemberUpdatedAt)(qx, memberId);
                // List all member identities
                const list = await (0, members_1.fetchMemberIdentities)(qx, memberId);
                captureNewState(lodash_1.default.sortBy(list, [(i) => i.platform, (i) => i.type]));
                await sequelizeRepository_1.default.commitTransaction(tx);
                return list;
            }));
            return list;
        }
        catch (error) {
            if (tx) {
                await sequelizeRepository_1.default.rollbackTransaction(tx);
            }
            throw error;
        }
    }
    // Delete member identity
    async delete(id, memberId) {
        let tx;
        try {
            const repoOptions = await sequelizeRepository_1.default.createTransactionalRepositoryOptions(this.options);
            tx = repoOptions.transaction;
            const qx = sequelizeRepository_1.default.getQueryExecutor(repoOptions);
            // Delete member identity
            await (0, members_1.deleteMemberIdentity)(qx, memberId, id);
            await (0, members_1.touchMemberUpdatedAt)(qx, memberId);
            // List all member identities
            const list = await (0, members_1.fetchMemberIdentities)(qx, memberId);
            await sequelizeRepository_1.default.commitTransaction(tx);
            return list;
        }
        catch (error) {
            if (tx) {
                await sequelizeRepository_1.default.rollbackTransaction(tx);
            }
            throw error;
        }
    }
}
exports.default = MemberIdentityService;
//# sourceMappingURL=memberIdentityService.js.map