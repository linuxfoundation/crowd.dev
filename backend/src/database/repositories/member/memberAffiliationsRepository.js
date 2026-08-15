"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const data_access_layer_1 = require("@crowd/data-access-layer");
const member_segment_affiliations_1 = require("@crowd/data-access-layer/src/member_segment_affiliations");
const segments_1 = require("@crowd/data-access-layer/src/segments");
const sequelizeRepository_1 = __importDefault(require("../sequelizeRepository"));
class MemberAffiliationsRepository {
    static async list(memberId, options) {
        const transaction = await sequelizeRepository_1.default.createTransaction(options);
        try {
            const txOptions = { ...options, transaction };
            const qx = sequelizeRepository_1.default.getQueryExecutor(txOptions);
            // Fetch member affiliations
            const affiliations = await (0, member_segment_affiliations_1.fetchMemberAffiliations)(qx, memberId);
            const orgIds = affiliations.map((a) => a.organizationId);
            const segmentIds = affiliations.map((a) => a.segmentId);
            // Fetch organizations
            let orgObject = {};
            if (orgIds.length > 0) {
                const organizations = await (0, data_access_layer_1.queryOrgs)(qx, {
                    filter: {
                        [data_access_layer_1.OrganizationField.ID]: {
                            in: orgIds,
                        },
                    },
                    fields: [data_access_layer_1.OrganizationField.ID, data_access_layer_1.OrganizationField.DISPLAY_NAME, data_access_layer_1.OrganizationField.LOGO],
                });
                orgObject = organizations.reduce((acc, org) => {
                    acc[org.id] = org;
                    return acc;
                }, {});
            }
            // Fetch organizations
            let segmentsObject = {};
            if (segmentIds.length > 0) {
                const segments = await (0, segments_1.fetchManySegments)(qx, segmentIds, 'id, "slug", "name", "parentName"');
                segmentsObject = segments.reduce((acc, seg) => {
                    acc[seg.id] = seg;
                    return acc;
                }, {});
            }
            // Map affiliations
            const list = affiliations.map((affiliation) => {
                const org = orgObject[affiliation.organizationId];
                const segment = segmentsObject[affiliation.segmentId];
                return {
                    ...affiliation,
                    segmentSlug: segment === null || segment === void 0 ? void 0 : segment.slug,
                    segmentName: segment === null || segment === void 0 ? void 0 : segment.name,
                    segmentParentName: segment === null || segment === void 0 ? void 0 : segment.parentName,
                    organizationName: org === null || org === void 0 ? void 0 : org.displayName,
                    organizationLogo: org === null || org === void 0 ? void 0 : org.logo,
                };
            });
            await sequelizeRepository_1.default.commitTransaction(transaction);
            return list;
        }
        catch (err) {
            if (transaction) {
                await sequelizeRepository_1.default.rollbackTransaction(transaction);
            }
            throw err;
        }
    }
    static async upsertMultiple(memberId, data, options) {
        const transaction = await sequelizeRepository_1.default.createTransaction(options);
        try {
            const txOptions = { ...options, transaction };
            const qx = sequelizeRepository_1.default.getQueryExecutor(txOptions);
            // Delete all member affiliations
            await (0, member_segment_affiliations_1.deleteMemberSegmentAffiliations)(qx, { memberId });
            if ((data === null || data === void 0 ? void 0 : data.length) > 0) {
                await (0, member_segment_affiliations_1.insertMemberSegmentAffiliations)(qx, data.map((item) => ({
                    memberId,
                    segmentId: item.segmentId,
                    organizationId: item.organizationId,
                    dateStart: item.dateStart || null,
                    dateEnd: item.dateEnd || null,
                })), true);
            }
            await sequelizeRepository_1.default.commitTransaction(transaction);
            return await this.list(memberId, options);
        }
        catch (err) {
            if (transaction) {
                await sequelizeRepository_1.default.rollbackTransaction(transaction);
            }
            throw err;
        }
    }
}
exports.default = MemberAffiliationsRepository;
//# sourceMappingURL=memberAffiliationsRepository.js.map