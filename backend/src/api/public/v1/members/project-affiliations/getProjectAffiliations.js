"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getProjectAffiliations = getProjectAffiliations;
const zod_1 = require("zod");
const common_1 = require("@crowd/common");
const data_access_layer_1 = require("@crowd/data-access-layer");
const sequelizeQueryExecutor_1 = require("@/database/sequelizeQueryExecutor");
const api_1 = require("@/utils/api");
const validation_1 = require("@/utils/validation");
const mappers_1 = require("./mappers");
const paramsSchema = zod_1.z.object({
    memberId: zod_1.z.uuid(),
});
async function getProjectAffiliations(req, res) {
    var _a, _b;
    const { memberId } = (0, validation_1.validateOrThrow)(paramsSchema, req.params);
    const qx = (0, sequelizeQueryExecutor_1.optionsQx)(req);
    const member = await (0, data_access_layer_1.findMemberById)(qx, memberId, [data_access_layer_1.MemberField.ID]);
    if (!member) {
        throw new common_1.NotFoundError('Member not found');
    }
    const [projectSegments, maintainerRoles, segmentAffiliations, workExperiences] = await Promise.all([
        (0, data_access_layer_1.fetchMemberProjectSegments)(qx, memberId),
        (0, data_access_layer_1.findMaintainerRoles)(qx, [memberId]),
        (0, data_access_layer_1.fetchMemberSegmentAffiliationsWithOrg)(qx, memberId),
        (0, data_access_layer_1.fetchMemberWorkExperienceAffiliations)(qx, memberId),
    ]);
    // Group maintainer roles by segmentId
    const rolesBySegment = new Map();
    for (const role of maintainerRoles) {
        const existing = (_a = rolesBySegment.get(role.segmentId)) !== null && _a !== void 0 ? _a : [];
        existing.push(role);
        rolesBySegment.set(role.segmentId, existing);
    }
    // Group segment affiliations by segmentId
    const affiliationsBySegment = new Map();
    for (const aff of segmentAffiliations) {
        const existing = (_b = affiliationsBySegment.get(aff.segmentId)) !== null && _b !== void 0 ? _b : [];
        existing.push(aff);
        affiliationsBySegment.set(aff.segmentId, existing);
    }
    const projectAffiliations = projectSegments.map((segment) => {
        var _a, _b;
        const roles = ((_a = rolesBySegment.get(segment.id)) !== null && _a !== void 0 ? _a : []).map((r) => {
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
        // Use segment affiliations if they exist for this project, otherwise fall back to work experiences
        const segmentAffs = affiliationsBySegment.get(segment.id);
        const affiliations = segmentAffs
            ? segmentAffs.map(mappers_1.mapSegmentAffiliation)
            : workExperiences.map(mappers_1.mapWorkExperienceAffiliation);
        return {
            id: segment.id,
            projectSlug: segment.slug,
            projectName: segment.name,
            projectLogo: (_b = segment.projectLogo) !== null && _b !== void 0 ? _b : null,
            contributionCount: Number(segment.activityCount),
            roles,
            affiliations,
        };
    });
    (0, api_1.ok)(res, { projectAffiliations });
}
//# sourceMappingURL=getProjectAffiliations.js.map