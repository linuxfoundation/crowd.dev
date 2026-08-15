"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const data_access_layer_1 = require("@crowd/data-access-layer");
const member_segment_affiliations_1 = require("@crowd/data-access-layer/src/member_segment_affiliations");
const sequelizeRepository_1 = __importDefault(require("../sequelizeRepository"));
class MemberOrganizationAffiliationOverridesRepository {
    static async changeOverride(data, options) {
        const qx = sequelizeRepository_1.default.getQueryExecutor(options);
        await (0, data_access_layer_1.changeMemberOrganizationAffiliationOverrides)(qx, [data]);
        const { allowAffiliation, memberId, memberOrganizationId } = data;
        if (allowAffiliation === false && memberId && memberOrganizationId) {
            const memberOrganization = await (0, data_access_layer_1.fetchMemberOrganizationById)(qx, memberOrganizationId);
            if (memberOrganization === null || memberOrganization === void 0 ? void 0 : memberOrganization.organizationId) {
                await (0, member_segment_affiliations_1.deleteMemberSegmentAffiliations)(qx, {
                    memberId,
                    organizationId: memberOrganization.organizationId,
                });
            }
        }
        const overrides = await (0, data_access_layer_1.findMemberAffiliationOverrides)(qx, data.memberId, [
            data.memberOrganizationId,
        ]);
        return overrides[0];
    }
    static async findPrimaryWorkExperiences(memberId, options) {
        const qx = sequelizeRepository_1.default.getQueryExecutor(options);
        return (0, data_access_layer_1.findPrimaryWorkExperiencesOfMember)(qx, memberId);
    }
}
exports.default = MemberOrganizationAffiliationOverridesRepository;
//# sourceMappingURL=memberOrganizationAffiliationOverridesRepository.js.map