"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.createMember = createMember;
const zod_1 = require("zod");
const audit_logs_1 = require("@crowd/audit-logs");
const common_1 = require("@crowd/common");
const data_access_layer_1 = require("@crowd/data-access-layer");
const types_1 = require("@crowd/types");
const sequelizeQueryExecutor_1 = require("@/database/sequelizeQueryExecutor");
const api_1 = require("@/utils/api");
const err_1 = require("@/utils/err");
const validation_1 = require("@/utils/validation");
const bodySchema = zod_1.z.object({
    displayName: zod_1.z.string().trim().min(1),
    identities: zod_1.z
        .array(zod_1.z
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
    }))
        .min(1),
});
async function createMember(req, res) {
    const { displayName, identities } = (0, validation_1.validateOrThrow)(bodySchema, req.body);
    const qx = (0, sequelizeQueryExecutor_1.optionsQx)(req);
    const normalizedDisplayName = (0, common_1.getProperDisplayName)(displayName);
    const { dbMember, dbIdentities } = await qx.tx(async (tx) => {
        // Unverified identities aren't unique in the db, so the same handle or
        // email can sit on several members. Reject it here if someone else has it.
        const unverified = identities.filter((identity) => !identity.verified);
        if (unverified.length > 0) {
            const owners = await (0, data_access_layer_1.findMembersByIdentities)(tx, unverified);
            const hit = unverified.find((identity) => owners.has(`${identity.platform}:${identity.type}:${identity.value.trim()}`));
            if (hit) {
                throw new common_1.ConflictError('Identity already exists on another member', {
                    conflictMemberId: owners.get(`${hit.platform}:${hit.type}:${hit.value.trim()}`),
                    platform: hit.platform,
                    value: hit.value,
                    type: hit.type,
                });
            }
        }
        try {
            const dbMember = await (0, data_access_layer_1.createMember)(tx, {
                displayName: normalizedDisplayName,
                joinedAt: new Date().toISOString(),
                attributes: {},
                reach: {},
                // OpenSearch sync only keeps members that either have activities or have manuallyCreated set.
                manuallyCreated: true,
            });
            const dbIdentities = await (0, data_access_layer_1.insertMemberIdentities)(tx, identities.map((identity) => ({
                ...identity,
                memberId: dbMember.id,
            })), true, true);
            return { dbMember, dbIdentities };
        }
        catch (error) {
            // Only notify for a single identity because we can't tell which one conflicted in a batch.
            if (identities.length === 1 && (0, err_1.isMemberIdentityDbConflict)(error)) {
                const identity = identities[0];
                const conflictMemberId = await (0, data_access_layer_1.findMemberIdByVerifiedIdentity)(qx, identity.platform, identity.value, identity.type);
                return (0, err_1.rethrowDbConflict)(error, {
                    ...(conflictMemberId ? { conflictMemberId } : {}),
                    platform: identity.platform,
                    value: identity.value,
                    type: identity.type,
                });
            }
            return (0, err_1.rethrowDbConflict)(error);
        }
    });
    await (0, audit_logs_1.captureApiChange)(req, (0, audit_logs_1.memberCreateAction)(dbMember.id, async (captureNewState) => {
        captureNewState({
            memberId: dbMember.id,
            displayName: dbMember.displayName,
            manuallyCreated: true,
        });
    }));
    await (0, audit_logs_1.captureApiChange)(req, (0, audit_logs_1.memberEditIdentitiesAction)(dbMember.id, async (captureOldState, captureNewState) => {
        captureOldState({});
        captureNewState(dbIdentities);
    }));
    (0, api_1.created)(res, { memberId: dbMember.id });
}
//# sourceMappingURL=createMember.js.map