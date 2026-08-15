"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.verifyMemberWorkExperience = verifyMemberWorkExperience;
const zod_1 = require("zod");
const audit_logs_1 = require("@crowd/audit-logs");
const common_1 = require("@crowd/common");
const common_services_1 = require("@crowd/common_services");
const data_access_layer_1 = require("@crowd/data-access-layer");
const sequelizeQueryExecutor_1 = require("@/database/sequelizeQueryExecutor");
const api_1 = require("@/utils/api");
const mapper_1 = require("@/utils/mapper");
const validation_1 = require("@/utils/validation");
const paramsSchema = zod_1.z.object({
    memberId: zod_1.z.uuid(),
    workExperienceId: zod_1.z.uuid(),
});
const bodySchema = zod_1.z.object({
    verified: zod_1.z.boolean(),
    verifiedBy: zod_1.z.string(),
});
async function verifyMemberWorkExperience(req, res) {
    var _a, _b, _c;
    const { memberId, workExperienceId } = (0, validation_1.validateOrThrow)(paramsSchema, req.params);
    const { verified, verifiedBy } = (0, validation_1.validateOrThrow)(bodySchema, req.body);
    const qx = (0, sequelizeQueryExecutor_1.optionsQx)(req);
    const member = await (0, data_access_layer_1.findMemberById)(qx, memberId, [data_access_layer_1.MemberField.ID]);
    if (!member) {
        throw new common_1.NotFoundError('Member not found');
    }
    const memberOrgs = await (0, data_access_layer_1.fetchMemberOrganizations)(qx, memberId);
    const memberOrg = memberOrgs.find((mo) => mo.id === workExperienceId);
    if (!memberOrg) {
        throw new common_1.NotFoundError('Work experience not found');
    }
    // Stash org fields for response fallback when reject soft-deletes the row.
    const memberOrgsWithOrgDataBeforeChange = verified
        ? []
        : ((_a = (await (0, data_access_layer_1.fetchManyMemberOrgsWithOrgData)(qx, [memberId], {
            withDomains: true,
        })).get(memberId)) !== null && _a !== void 0 ? _a : []);
    const overlappingGroupedRows = (0, mapper_1.getOverlappingGroupedMemberOrganizations)(memberOrgs, memberOrg);
    const overlappingRowsWithIds = overlappingGroupedRows.filter((row) => !!row.id);
    const memberOrgIdsToDelete = [workExperienceId, ...overlappingRowsWithIds.map((row) => row.id)];
    const verifiedUpdate = { verified, verifiedBy };
    let updatedMemberOrg;
    await (0, audit_logs_1.captureApiChange)(req, (0, audit_logs_1.memberVerifyWorkExperienceAction)(memberId, async (captureOldState, captureNewState) => {
        captureOldState(memberOrg);
        await qx.tx(async (tx) => {
            if (verified) {
                // Verification status belongs to the grouped work experience, not just the visible row
                updatedMemberOrg = await (0, data_access_layer_1.updateMemberOrganization)(tx, memberId, workExperienceId, verifiedUpdate);
                for (const overlappingRow of overlappingRowsWithIds) {
                    await (0, data_access_layer_1.updateMemberOrganization)(tx, memberId, overlappingRow.id, verifiedUpdate);
                }
            }
            else {
                // Unverifying removes the grouped work experience from both visible and hidden rows.
                // This is a human decision, so deletedBy is set — enrichment must never recreate it.
                await (0, data_access_layer_1.deleteMemberOrganizations)(tx, memberId, memberOrgIdsToDelete, true, verifiedBy);
            }
        });
        // Signal after commit so the workflow sees persisted changes
        if (!verified) {
            await (0, common_services_1.signalMemberUpdate)(req.temporal, memberId, {
                memberOrganizationIds: [memberOrg.organizationId],
            });
        }
        captureNewState(updatedMemberOrg !== null && updatedMemberOrg !== void 0 ? updatedMemberOrg : { ...memberOrg, ...verifiedUpdate });
    }));
    const orgsMap = await (0, data_access_layer_1.fetchManyMemberOrgsWithOrgData)(qx, [memberId], {
        withDomains: true,
    });
    const groupedMemberOrgs = (0, mapper_1.groupMemberOrganizations)((_b = orgsMap.get(memberId)) !== null && _b !== void 0 ? _b : []);
    const groupedMemberOrgsBeforeChange = (0, mapper_1.groupMemberOrganizations)(memberOrgsWithOrgDataBeforeChange);
    const fallbackMo = groupedMemberOrgsBeforeChange.find((mo) => mo.id === workExperienceId);
    const responseMo = (_c = groupedMemberOrgs.find((mo) => mo.id === workExperienceId)) !== null && _c !== void 0 ? _c : (fallbackMo ? { ...fallbackMo, ...verifiedUpdate } : undefined);
    if (!responseMo) {
        throw new common_1.NotFoundError('Work experience not found');
    }
    (0, api_1.ok)(res, (0, mapper_1.toMemberWorkExperience)(responseMo));
}
//# sourceMappingURL=verifyMemberWorkExperience.js.map