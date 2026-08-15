"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.deleteMemberWorkExperience = deleteMemberWorkExperience;
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
async function deleteMemberWorkExperience(req, res) {
    const { memberId, workExperienceId } = (0, validation_1.validateOrThrow)(paramsSchema, req.params);
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
    const overlappingGroupedRows = (0, mapper_1.getOverlappingGroupedMemberOrganizations)(memberOrgs, memberOrg);
    const memberOrgIdsToDelete = [
        workExperienceId,
        ...overlappingGroupedRows.flatMap((row) => (row.id ? [row.id] : [])),
    ];
    // Delete hidden grouped rows with the visible row so read responses stay consistent
    await (0, audit_logs_1.captureApiChange)(req, (0, audit_logs_1.memberEditOrganizationsAction)(memberId, async (captureOldState, captureNewState) => {
        captureOldState(memberOrg);
        await qx.tx(async (tx) => {
            await (0, data_access_layer_1.deleteMemberOrganizations)(tx, memberId, memberOrgIdsToDelete);
        });
        // Signal after commit so the workflow sees persisted changes
        await (0, common_services_1.signalMemberUpdate)(req.temporal, memberId, {
            memberOrganizationIds: [memberOrg.organizationId],
        });
        captureNewState(null);
    }));
    (0, api_1.noContent)(res);
}
//# sourceMappingURL=deleteMemberWorkExperience.js.map