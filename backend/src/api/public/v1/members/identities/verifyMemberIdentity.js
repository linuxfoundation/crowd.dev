"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.verifyMemberIdentity = verifyMemberIdentity;
const zod_1 = require("zod");
const audit_logs_1 = require("@crowd/audit-logs");
const common_1 = require("@crowd/common");
const common_services_1 = require("@crowd/common_services");
const data_access_layer_1 = require("@crowd/data-access-layer");
const sequelizeQueryExecutor_1 = require("@/database/sequelizeQueryExecutor");
const api_1 = require("@/utils/api");
const err_1 = require("@/utils/err");
const validation_1 = require("@/utils/validation");
const paramsSchema = zod_1.z.object({
    memberId: zod_1.z.uuid(),
    identityId: zod_1.z.uuid(),
});
const bodySchema = zod_1.z.object({
    verified: zod_1.z.boolean(),
    verifiedBy: zod_1.z.string(),
});
function toReturn(identity) {
    var _a;
    return {
        id: identity.id,
        value: identity.value,
        platform: identity.platform,
        type: identity.type,
        verified: identity.verified,
        verifiedBy: (_a = identity.verifiedBy) !== null && _a !== void 0 ? _a : null,
        source: identity.source,
        createdAt: identity.createdAt,
        updatedAt: identity.updatedAt,
    };
}
async function verifyMemberIdentity(req, res) {
    const { memberId, identityId } = (0, validation_1.validateOrThrow)(paramsSchema, req.params);
    const { verified, verifiedBy } = (0, validation_1.validateOrThrow)(bodySchema, req.body);
    const qx = (0, sequelizeQueryExecutor_1.optionsQx)(req);
    const member = await (0, data_access_layer_1.findMemberById)(qx, memberId, [data_access_layer_1.MemberField.ID]);
    if (!member) {
        throw new common_1.NotFoundError('Member not found');
    }
    const identity = await (0, data_access_layer_1.findMemberIdentityById)(qx, memberId, identityId);
    if (!identity)
        throw new common_1.NotFoundError('Member identity not found');
    let unmerge;
    let updatedIdentity;
    await (0, audit_logs_1.captureApiChange)(req, (0, audit_logs_1.memberVerifyIdentityAction)(memberId, async (captureOldState, captureNewState) => {
        captureOldState(identity);
        await qx.tx(async (tx) => {
            try {
                updatedIdentity = await (0, data_access_layer_1.updateMemberIdentity)(tx, memberId, identityId, {
                    verified,
                    verifiedBy,
                });
            }
            catch (error) {
                if (verified && (0, err_1.isMemberIdentityDbConflict)(error)) {
                    const conflictMemberId = await (0, data_access_layer_1.findMemberIdByVerifiedIdentity)(qx, identity.platform, identity.value, identity.type);
                    (0, err_1.rethrowDbConflict)(error, {
                        memberId,
                        ...(conflictMemberId ? { conflictMemberId } : {}),
                        platform: identity.platform,
                        value: identity.value,
                        type: identity.type,
                    });
                }
                throw error;
            }
            if (!updatedIdentity) {
                throw new common_1.InternalError('Failed to update member identity');
            }
            if (!verified) {
                const { count } = await (0, data_access_layer_1.queryActivityRelations)(tx, {
                    filter: {
                        and: [
                            {
                                memberId: { eq: memberId },
                                username: { eq: identity.value },
                                platform: { eq: identity.platform },
                            },
                        ],
                    },
                    limit: 1,
                    countOnly: true,
                });
                if (count === 0) {
                    await (0, data_access_layer_1.deleteMemberIdentity)(tx, memberId, identityId);
                }
                else {
                    const preview = await (0, common_services_1.prepareMemberUnmerge)(tx, memberId, identityId, false);
                    const result = await (0, common_services_1.unmergeMember)(tx, memberId, preview, req.actor.id);
                    unmerge = { preview, result };
                }
            }
        });
        captureNewState(updatedIdentity);
    }));
    if (unmerge) {
        const { preview, result } = unmerge;
        await (0, audit_logs_1.captureApiChange)(req, (0, audit_logs_1.memberUnmergeAction)(memberId, async (captureOldState, captureNewState) => {
            captureOldState({ primary: preview.primary });
            captureNewState({
                primary: result.primary,
                secondary: result.secondary,
            });
        }));
        try {
            await (0, common_services_1.invalidateMemberQueryCache)(req.redis, [result.primary.id, result.secondary.id], true);
        }
        catch (error) {
            req.log.warn({ error }, 'Cache invalidation failed after identity unmerge');
        }
        try {
            await (0, common_services_1.startMemberUnmergeWorkflow)(req.temporal, {
                primaryId: result.primary.id,
                secondaryId: result.secondary.id,
                movedIdentities: result.movedIdentities,
                primaryDisplayName: result.primary.displayName,
                secondaryDisplayName: result.secondary.displayName,
                actorId: req.actor.id,
            });
        }
        catch (error) {
            req.log.error({ error }, 'Failed to start unmerge workflow');
            throw error;
        }
    }
    // If verified = false and no activities (deleted): 204 No Content
    if (!verified && !unmerge) {
        (0, api_1.noContent)(res);
        return;
    }
    // If verified = false and has activities (unmerge): 200 OK + unmergedToMemberId
    (0, api_1.ok)(res, {
        ...toReturn(updatedIdentity),
        ...(unmerge && { unmergedToMemberId: unmerge.result.secondary.id }),
    });
}
//# sourceMappingURL=verifyMemberIdentity.js.map