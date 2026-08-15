"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.patchProjectAffiliation = patchProjectAffiliation;
const zod_1 = require("zod");
const audit_logs_1 = require("@crowd/audit-logs");
const common_1 = require("@crowd/common");
const common_services_1 = require("@crowd/common_services");
const data_access_layer_1 = require("@crowd/data-access-layer");
const member_segment_affiliations_1 = require("@crowd/data-access-layer/src/member_segment_affiliations");
const sequelizeQueryExecutor_1 = require("@/database/sequelizeQueryExecutor");
const api_1 = require("@/utils/api");
const validation_1 = require("@/utils/validation");
const mappers_1 = require("./mappers");
const paramsSchema = zod_1.z.object({
    memberId: zod_1.z.uuid(),
    projectId: zod_1.z.uuid(),
});
const bodySchema = zod_1.z
    .object({
    affiliations: zod_1.z.array(zod_1.z
        .object({
        organizationId: zod_1.z.uuid(),
        dateStart: zod_1.z.coerce.date(),
        dateEnd: zod_1.z.coerce.date().nullable().optional(),
    })
        .refine((a) => a.dateEnd == null || a.dateEnd >= a.dateStart, {
        message: 'dateEnd must be greater than or equal to dateStart',
    })),
    verifiedBy: zod_1.z.string().max(255).optional(),
})
    .refine((b) => b.affiliations.length === 0 || b.verifiedBy != null, {
    message: 'verifiedBy is required when affiliations is non-empty',
    path: ['verifiedBy'],
});
async function patchProjectAffiliation(req, res) {
    var _a;
    const { memberId, projectId } = (0, validation_1.validateOrThrow)(paramsSchema, req.params);
    const { affiliations, verifiedBy } = (0, validation_1.validateOrThrow)(bodySchema, req.body);
    const qx = (0, sequelizeQueryExecutor_1.optionsQx)(req);
    const member = await (0, data_access_layer_1.findMemberById)(qx, memberId, [data_access_layer_1.MemberField.ID]);
    if (!member) {
        throw new common_1.NotFoundError('Member not found');
    }
    const [segment] = await (0, data_access_layer_1.fetchMemberProjectSegments)(qx, memberId, projectId);
    if (!segment) {
        throw new common_1.NotFoundError('Project not found');
    }
    if (affiliations.length > 0) {
        const policies = await (0, data_access_layer_1.fetchManyOrganizationAffiliationPolicies)(qx, affiliations.map((a) => a.organizationId));
        if ([...policies.values()].some((isBlocked) => isBlocked)) {
            throw new common_1.BadRequestError('This organization does not allow affiliations');
        }
    }
    const existingAffiliations = await (0, data_access_layer_1.fetchMemberSegmentAffiliationsForProject)(qx, memberId, projectId);
    let updatedAffiliations = [];
    await (0, audit_logs_1.captureApiChange)(req, (0, audit_logs_1.memberEditAffiliationsAction)(memberId, async (captureOldState, captureNewState) => {
        captureOldState(existingAffiliations);
        const oldOrgIds = existingAffiliations.map((a) => a.organizationId);
        const newOrgIds = affiliations.map((a) => a.organizationId);
        const orgIdsToRecalculate = [...new Set([...oldOrgIds, ...newOrgIds])];
        await qx.tx(async (tx) => {
            await (0, member_segment_affiliations_1.deleteMemberSegmentAffiliations)(tx, { memberId, segmentId: projectId });
            if (affiliations.length > 0) {
                await (0, data_access_layer_1.insertMemberSegmentAffiliations)(tx, affiliations.map((a) => {
                    var _a, _b;
                    return ({
                        memberId,
                        segmentId: projectId,
                        organizationId: a.organizationId,
                        dateStart: a.dateStart.toISOString(),
                        dateEnd: (_b = (_a = a.dateEnd) === null || _a === void 0 ? void 0 : _a.toISOString()) !== null && _b !== void 0 ? _b : null,
                        verified: true,
                        verifiedBy: verifiedBy,
                    });
                }), true);
            }
        });
        // Signal after commit so the workflow sees persisted changes
        await (0, common_services_1.signalMemberUpdate)(req.temporal, memberId, {
            memberOrganizationIds: orgIdsToRecalculate,
        });
        updatedAffiliations = await (0, data_access_layer_1.fetchMemberSegmentAffiliationsForProject)(qx, memberId, projectId);
        captureNewState(updatedAffiliations);
    }));
    const maintainerRoles = await (0, data_access_layer_1.findMaintainerRoles)(qx, [memberId]);
    const roles = maintainerRoles
        .filter((r) => r.segmentId === projectId)
        .map((r) => {
        var _a, _b, _c, _d;
        return ({
            id: r.id,
            role: r.role,
            startDate: (_a = r.dateStart) !== null && _a !== void 0 ? _a : null,
            endDate: (_b = r.dateEnd) !== null && _b !== void 0 ? _b : null,
            repoUrl: (_c = r.url) !== null && _c !== void 0 ? _c : null,
            repoFileUrl: (_d = r.maintainerFile) !== null && _d !== void 0 ? _d : null,
        });
    });
    (0, api_1.ok)(res, {
        id: segment.id,
        projectSlug: segment.slug,
        projectName: segment.name,
        projectLogo: (_a = segment.projectLogo) !== null && _a !== void 0 ? _a : null,
        contributionCount: Number(segment.activityCount),
        roles,
        affiliations: updatedAffiliations.map(mappers_1.mapSegmentAffiliation),
    });
}
//# sourceMappingURL=patchProjectAffiliation.js.map