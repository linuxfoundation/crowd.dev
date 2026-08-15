"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.resolveMemberByIdentities = resolveMemberByIdentities;
const zod_1 = require("zod");
const common_1 = require("@crowd/common");
const data_access_layer_1 = require("@crowd/data-access-layer");
const types_1 = require("@crowd/types");
const sequelizeQueryExecutor_1 = require("@/database/sequelizeQueryExecutor");
const api_1 = require("@/utils/api");
const validation_1 = require("@/utils/validation");
const bodySchema = zod_1.z.object({
    lfids: zod_1.z.array(zod_1.z.string().trim()).min(1, 'At least one lfid is required'),
    emails: zod_1.z.array(zod_1.z.email()).optional(),
});
async function resolveMemberByIdentities(req, res) {
    var _a;
    const { lfids, emails } = (0, validation_1.validateOrThrow)(bodySchema, req.body);
    const qx = (0, sequelizeQueryExecutor_1.optionsQx)(req);
    const identities = [
        ...lfids.map((lfid) => ({
            platform: types_1.PlatformType.LFID,
            type: types_1.MemberIdentityType.USERNAME,
            value: lfid,
            verified: true,
        })),
        ...((_a = emails === null || emails === void 0 ? void 0 : emails.map((email) => ({
            type: types_1.MemberIdentityType.EMAIL,
            value: email,
            verified: true,
        }))) !== null && _a !== void 0 ? _a : []),
    ];
    const memberIds = await (0, data_access_layer_1.findMemberIdsByIdentities)(qx, identities);
    if (memberIds.length === 0) {
        throw new common_1.NotFoundError('Member not found');
    }
    else if (memberIds.length > 1) {
        throw new common_1.ConflictError('Multiple member profiles matched', { memberIds });
    }
    const memberId = memberIds[0];
    (0, api_1.ok)(res, { memberId });
}
//# sourceMappingURL=resolveMember.js.map