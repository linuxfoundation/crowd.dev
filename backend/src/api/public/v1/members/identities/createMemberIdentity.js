"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.createMemberIdentity = createMemberIdentity;
const zod_1 = require("zod");
const audit_logs_1 = require("@crowd/audit-logs");
const common_1 = require("@crowd/common");
const data_access_layer_1 = require("@crowd/data-access-layer");
const types_1 = require("@crowd/types");
const sequelizeQueryExecutor_1 = require("@/database/sequelizeQueryExecutor");
const api_1 = require("@/utils/api");
const err_1 = require("@/utils/err");
const validation_1 = require("@/utils/validation");
const paramsSchema = zod_1.z.object({
    memberId: zod_1.z.uuid(),
});
const bodySchema = zod_1.z
    .object({
    value: zod_1.z.string().min(1),
    platform: zod_1.z.string().min(1),
    type: zod_1.z.enum(types_1.MemberIdentityType),
    source: zod_1.z.string().min(1),
    verified: zod_1.z.boolean(),
    verifiedBy: zod_1.z.string().optional(),
})
    .refine((data) => !data.verified || data.verifiedBy, {
    message: 'verifiedBy is required when verified is true',
    path: ['verifiedBy'],
});
async function createMemberIdentity(req, res) {
    var _a, _b;
    const { memberId } = (0, validation_1.validateOrThrow)(paramsSchema, req.params);
    const raw = (0, validation_1.validateOrThrow)(bodySchema, req.body);
    const data = {
        ...raw,
        value: (0, common_1.normalizeMemberIdentityValue)(raw.value),
    };
    const qx = (0, sequelizeQueryExecutor_1.optionsQx)(req);
    const member = await (0, data_access_layer_1.findMemberById)(qx, memberId, [data_access_layer_1.MemberField.ID]);
    if (!member) {
        throw new common_1.NotFoundError('Member not found');
    }
    const conflictContext = {
        memberId,
        platform: data.platform,
        value: data.value,
        type: data.type,
    };
    let identity;
    let alreadyExisted = false;
    await (0, audit_logs_1.captureApiChange)(req, (0, audit_logs_1.memberEditIdentitiesAction)(memberId, async (captureOldState, captureNewState) => {
        captureOldState({});
        const outcome = await qx.tx(async (tx) => {
            const existing = await (0, data_access_layer_1.findMemberIdentitiesByValue)(tx, memberId, data.value, {
                type: data.type,
            });
            const exactMatch = existing.find((row) => row.platform === data.platform);
            let result = exactMatch;
            const existed = Boolean(exactMatch);
            // Unverified identities aren't unique in the db, so the same handle or
            // email can sit on several members. Reject it here if someone else has it.
            if (!result && !data.verified) {
                const conflict = await (0, data_access_layer_1.findMemberIdentityConflict)(tx, {
                    value: data.value,
                    platform: data.platform,
                    type: data.type,
                    excludeMemberId: memberId,
                });
                if (conflict) {
                    throw new common_1.ConflictError('Identity already exists on another member', {
                        ...conflictContext,
                        conflictMemberId: conflict.memberId,
                    });
                }
            }
            try {
                if (!result) {
                    const [inserted] = await (0, data_access_layer_1.insertMemberIdentities)(tx, [
                        {
                            memberId,
                            platform: data.platform,
                            value: data.value,
                            type: data.type,
                            source: data.source,
                            verified: data.verified,
                            verifiedBy: data.verifiedBy,
                        },
                    ], true, true);
                    result = inserted;
                }
                // A verified identity confirms the same value for this member, so keep same-value
                // identities in sync instead of leaving stale unverified duplicates behind.
                if (data.verified && existing.length > 0) {
                    const updatedRows = await Promise.all(existing.map((row) => (0, data_access_layer_1.updateMemberIdentity)(tx, memberId, row.id, {
                        verified: true,
                        verifiedBy: data.verifiedBy,
                    })));
                    const updatedExact = updatedRows.find((row) => (row === null || row === void 0 ? void 0 : row.id) === (exactMatch === null || exactMatch === void 0 ? void 0 : exactMatch.id));
                    if (updatedExact) {
                        result = updatedExact;
                    }
                }
            }
            catch (error) {
                if ((0, err_1.isMemberIdentityDbConflict)(error)) {
                    const conflictMemberId = await (0, data_access_layer_1.findMemberIdByVerifiedIdentity)(qx, data.platform, data.value, data.type);
                    (0, err_1.rethrowDbConflict)(error, {
                        ...conflictContext,
                        ...(conflictMemberId ? { conflictMemberId } : {}),
                    });
                }
                throw error;
            }
            await (0, data_access_layer_1.touchMemberUpdatedAt)(tx, memberId);
            return { identity: result, alreadyExisted: existed };
        });
        identity = outcome.identity;
        alreadyExisted = outcome.alreadyExisted;
        captureNewState(identity);
    }));
    const response = {
        id: identity.id,
        value: identity.value,
        platform: identity.platform,
        type: identity.type,
        verified: identity.verified,
        verifiedBy: (_a = identity.verifiedBy) !== null && _a !== void 0 ? _a : null,
        source: (_b = identity.source) !== null && _b !== void 0 ? _b : null,
        createdAt: identity.createdAt,
        updatedAt: identity.updatedAt,
    };
    if (alreadyExisted) {
        (0, api_1.ok)(res, response);
    }
    else {
        (0, api_1.created)(res, response);
    }
}
//# sourceMappingURL=createMemberIdentity.js.map