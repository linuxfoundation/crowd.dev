"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.createMemberWorkExperience = createMemberWorkExperience;
const zod_1 = require("zod");
const audit_logs_1 = require("@crowd/audit-logs");
const common_1 = require("@crowd/common");
const common_services_1 = require("@crowd/common_services");
const data_access_layer_1 = require("@crowd/data-access-layer");
const member_segment_affiliations_1 = require("@crowd/data-access-layer/src/member_segment_affiliations");
const sequelizeQueryExecutor_1 = require("@/database/sequelizeQueryExecutor");
const api_1 = require("@/utils/api");
const mapper_1 = require("@/utils/mapper");
const validation_1 = require("@/utils/validation");
const paramsSchema = zod_1.z.object({
    memberId: zod_1.z.uuid(),
});
const bodySchema = zod_1.z.object({
    organizationId: zod_1.z.uuid(),
    jobTitle: zod_1.z.string(),
    verified: zod_1.z.boolean(),
    verifiedBy: zod_1.z.string(),
    source: zod_1.z.string(),
    startDate: zod_1.z.coerce.date(),
    endDate: zod_1.z.coerce.date().nullable().optional(),
});
async function createMemberWorkExperience(req, res) {
    const { memberId } = (0, validation_1.validateOrThrow)(paramsSchema, req.params);
    const data = (0, validation_1.validateOrThrow)(bodySchema, req.body);
    const qx = (0, sequelizeQueryExecutor_1.optionsQx)(req);
    const member = await (0, data_access_layer_1.findMemberById)(qx, memberId, [data_access_layer_1.MemberField.ID]);
    if (!member) {
        throw new common_1.NotFoundError('Member not found');
    }
    let createdMo;
    await (0, audit_logs_1.captureApiChange)(req, (0, audit_logs_1.memberEditOrganizationsAction)(memberId, async (captureOldState, captureNewState) => {
        var _a;
        captureOldState({});
        let dates;
        try {
            dates = (0, common_1.sanitizeMemberOrganizationDateRange)(data.startDate, data.endDate, true);
        }
        catch (error) {
            throw new common_1.BadRequestError('Invalid work experience date range');
        }
        const memberOrgData = {
            memberId,
            organizationId: data.organizationId,
            title: data.jobTitle,
            dateStart: dates.dateStart,
            dateEnd: dates.dateEnd,
            source: data.source,
            verified: data.verified,
            verifiedBy: data.verifiedBy,
        };
        let newMemberOrgId;
        await qx.tx(async (tx) => {
            await (0, data_access_layer_1.cleanSoftDeletedMemberOrganization)(tx, memberId, data.organizationId, memberOrgData);
            newMemberOrgId = await (0, data_access_layer_1.createMemberOrganization)(tx, memberId, memberOrgData);
            if (!newMemberOrgId) {
                throw new common_1.ConflictError('A work experience with the same dates already exists');
            }
            const orgAffiliationPolicyById = await (0, data_access_layer_1.fetchManyOrganizationAffiliationPolicies)(tx, [
                data.organizationId,
            ]);
            if (newMemberOrgId && orgAffiliationPolicyById.get(data.organizationId)) {
                await (0, data_access_layer_1.changeMemberOrganizationAffiliationOverrides)(tx, [
                    {
                        memberId,
                        memberOrganizationId: newMemberOrgId,
                        allowAffiliation: false,
                    },
                ]);
                await (0, member_segment_affiliations_1.deleteMemberSegmentAffiliations)(tx, {
                    memberId,
                    organizationId: data.organizationId,
                });
            }
        });
        // Signal after commit so the workflow sees persisted changes
        await (0, common_services_1.signalMemberUpdate)(req.temporal, memberId, {
            memberOrganizationIds: [data.organizationId],
        });
        const orgsMap = await (0, data_access_layer_1.fetchManyMemberOrgsWithOrgData)(qx, [memberId], { withDomains: true });
        createdMo = ((_a = orgsMap.get(memberId)) !== null && _a !== void 0 ? _a : []).find((mo) => mo.id === newMemberOrgId);
        captureNewState(createdMo !== null && createdMo !== void 0 ? createdMo : null);
    }));
    if (!createdMo) {
        throw new common_1.NotFoundError('Work experience not found after creation');
    }
    (0, api_1.created)(res, (0, mapper_1.toMemberWorkExperience)(createdMo));
}
//# sourceMappingURL=createMemberWorkExperience.js.map