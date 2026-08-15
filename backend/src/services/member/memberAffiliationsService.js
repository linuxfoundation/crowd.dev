"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
/* eslint-disable no-continue */
const lodash_1 = require("lodash");
const common_1 = require("@crowd/common");
const common_services_1 = require("@crowd/common_services");
const data_access_layer_1 = require("@crowd/data-access-layer");
const maintainers_1 = require("@crowd/data-access-layer/src/maintainers");
const member_segment_affiliations_1 = require("@crowd/data-access-layer/src/member_segment_affiliations");
const segments_1 = require("@crowd/data-access-layer/src/segments");
const logging_1 = require("@crowd/logging");
const memberAffiliationsRepository_1 = __importDefault(require("@/database/repositories/member/memberAffiliationsRepository"));
const sequelizeRepository_1 = __importDefault(require("@/database/repositories/sequelizeRepository"));
const mapper_1 = require("@/utils/mapper");
const memberOrganizationsService_1 = __importDefault(require("./memberOrganizationsService"));
class MemberAffiliationsService extends logging_1.LoggerBase {
    constructor(options) {
        super(options.log);
        this.options = options;
    }
    // Member affiliations list
    async list(memberId) {
        const affiliations = await memberAffiliationsRepository_1.default.list(memberId, this.options);
        await (async function addMaintainerRoles(options, rows) {
            const qx = sequelizeRepository_1.default.getQueryExecutor(options);
            const maintainerRoles = await (0, maintainers_1.findMaintainerRoles)(qx, [memberId]);
            const segmentIds = (0, lodash_1.uniq)(maintainerRoles.map((m) => m.segmentId));
            const segmentsInfo = await (0, segments_1.fetchManySegments)(qx, segmentIds);
            const groupedMaintainers = (0, common_1.groupBy)(maintainerRoles, (m) => m.segmentId);
            rows.forEach((row) => {
                row.maintainerRoles = (groupedMaintainers.get(row.segmentId) || []).map((role) => {
                    const segmentInfo = segmentsInfo.find((s) => s.id === role.segmentId);
                    return {
                        ...role,
                        segmentName: segmentInfo === null || segmentInfo === void 0 ? void 0 : segmentInfo.name,
                    };
                });
            });
        })(this.options, affiliations);
        return affiliations;
    }
    // Member multiple identity creation
    async upsertMultiple(memberId, data) {
        if ((data === null || data === void 0 ? void 0 : data.length) > 0) {
            const qx = sequelizeRepository_1.default.getQueryExecutor(this.options);
            const organizationIds = data
                .map((a) => a.organizationId)
                .filter((id) => Boolean(id));
            const policies = await (0, data_access_layer_1.fetchManyOrganizationAffiliationPolicies)(qx, organizationIds);
            if ([...policies.values()].some((isBlocked) => isBlocked)) {
                throw new common_1.Error400(this.options.language, 'This organization does not allow affiliations');
            }
        }
        return memberAffiliationsRepository_1.default.upsertMultiple(memberId, data, this.options);
    }
    async changeAffiliationOverride(data) {
        if (data.isPrimaryWorkExperience) {
            const memberOrgService = new memberOrganizationsService_1.default(this.options);
            // check if any other work experience in intersecting date range was marked as primary
            // we don't allow this because "isPrimaryWorkExperience" decides which work exp to pick on date conflicts
            const allWorkExperiencesOfMember = (await memberOrgService.list(data.memberId)).map((mo) => mo.memberOrganizations);
            const currentlyEditedWorkExperience = allWorkExperiencesOfMember.find((w) => w.id === data.memberOrganizationId);
            const primaryWorkExperiencesOfMember = allWorkExperiencesOfMember.filter((w) => w.affiliationOverride.isPrimaryWorkExperience);
            if (currentlyEditedWorkExperience.affiliationOverride.isPrimaryWorkExperience === false) {
                for (const existingPrimaryWorkExp of primaryWorkExperiencesOfMember) {
                    if ((0, common_1.dateIntersects)(existingPrimaryWorkExp.dateStart, existingPrimaryWorkExp.dateEnd, currentlyEditedWorkExperience.dateStart, currentlyEditedWorkExperience.dateEnd)) {
                        throw new common_1.Error400(this.options.language, `Date range conflicts with another primary work experience id = ${existingPrimaryWorkExp.id}`);
                    }
                }
            }
        }
        const qx = sequelizeRepository_1.default.getQueryExecutor(this.options);
        const memberOrgs = await (0, data_access_layer_1.fetchMemberOrganizations)(qx, data.memberId);
        const memberOrg = memberOrgs.find((mo) => mo.id === data.memberOrganizationId);
        const overlappingGroupedRows = memberOrg
            ? (0, mapper_1.getOverlappingGroupedMemberOrganizations)(memberOrgs, memberOrg)
            : [];
        const memberOrgIds = [
            data.memberOrganizationId,
            ...overlappingGroupedRows.flatMap((row) => (row.id ? [row.id] : [])),
        ];
        // Apply the override to hidden grouped rows so the merged work experience has one decision
        await (0, data_access_layer_1.changeMemberOrganizationAffiliationOverrides)(qx, memberOrgIds.map((memberOrganizationId) => ({
            ...data,
            memberOrganizationId,
        })));
        if (data.allowAffiliation === false && (memberOrg === null || memberOrg === void 0 ? void 0 : memberOrg.organizationId)) {
            await (0, member_segment_affiliations_1.deleteMemberSegmentAffiliations)(qx, {
                memberId: data.memberId,
                organizationId: memberOrg.organizationId,
            });
        }
        const overrides = await (0, data_access_layer_1.findMemberAffiliationOverrides)(qx, data.memberId, [
            data.memberOrganizationId,
        ]);
        const override = overrides[0];
        await (0, common_services_1.signalMemberUpdate)(this.options.temporal, data.memberId);
        return override;
    }
}
exports.default = MemberAffiliationsService;
//# sourceMappingURL=memberAffiliationsService.js.map