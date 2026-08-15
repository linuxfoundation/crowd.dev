"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
/* eslint-disable no-continue */
const lodash_1 = __importDefault(require("lodash"));
const common_1 = require("@crowd/common");
const common_services_1 = require("@crowd/common_services");
const data_access_layer_1 = require("@crowd/data-access-layer");
const member_segment_affiliations_1 = require("@crowd/data-access-layer/src/member_segment_affiliations");
const logging_1 = require("@crowd/logging");
const types_1 = require("@crowd/types");
const sequelizeRepository_1 = __importDefault(require("@/database/repositories/sequelizeRepository"));
const mapper_1 = require("@/utils/mapper");
class MemberOrganizationsService extends logging_1.LoggerBase {
    constructor(options) {
        super(options.log);
        this.options = options;
    }
    // Member organization list
    async list(memberId, transaction) {
        const qx = sequelizeRepository_1.default.getQueryExecutor({ ...this.options, transaction });
        // Fetch member organizations
        const memberOrganizations = await (0, data_access_layer_1.fetchMemberOrganizations)(qx, memberId);
        if (memberOrganizations.length === 0) {
            return [];
        }
        // Parse unique organization ids
        const orgIds = [...new Set(memberOrganizations.map((mo) => mo.organizationId))];
        // Fetch organizations
        let organizations = [];
        if (orgIds.length) {
            organizations = await (0, data_access_layer_1.queryOrgs)(qx, {
                filter: {
                    [data_access_layer_1.OrganizationField.ID]: {
                        in: orgIds,
                    },
                },
                fields: [
                    data_access_layer_1.OrganizationField.ID,
                    data_access_layer_1.OrganizationField.DISPLAY_NAME,
                    data_access_layer_1.OrganizationField.LOGO,
                    data_access_layer_1.OrganizationField.CREATED_AT,
                ],
            });
        }
        // Fetch affiliation overrides
        const affiliationOverrides = await (0, data_access_layer_1.findMemberAffiliationOverrides)(qx, memberId, memberOrganizations.map((mo) => mo.id));
        const overridesByMemberOrganizationId = new Map(affiliationOverrides.map((override) => [override.memberOrganizationId, override]));
        // Create mapping by id to speed up the processing
        const orgById = organizations.reduce((obj, org) => ({
            ...obj,
            [org.id]: org,
        }), {});
        // Format the results and order by dateStart and dateEnd
        const groupedMemberOrganizations = (0, mapper_1.groupMemberOrganizations)(memberOrganizations);
        const allOrganizations = groupedMemberOrganizations
            .filter((mo) => !!mo.id && !!orgById[mo.organizationId])
            .map((mo) => {
            const overlappingGroupedRows = (0, mapper_1.getOverlappingGroupedMemberOrganizations)(memberOrganizations, mo);
            const relatedIds = [mo.id, ...overlappingGroupedRows.map((row) => row.id)];
            const relatedOverrides = relatedIds.map((memberOrganizationId) => overridesByMemberOrganizationId.get(memberOrganizationId));
            const resolvedOverrides = relatedOverrides.filter((override) => !!override);
            // Merge override flags from rows that are displayed as one work experience
            const allowAffiliation = resolvedOverrides.length === 0 ||
                resolvedOverrides.every((override) => override.allowAffiliation !== false);
            const isPrimaryWorkExperience = resolvedOverrides.some((override) => override.isPrimaryWorkExperience);
            return {
                ...orgById[mo.organizationId],
                id: mo.organizationId,
                memberOrganizations: {
                    ...mo,
                    affiliationOverride: {
                        memberId,
                        memberOrganizationId: mo.id,
                        allowAffiliation,
                        isPrimaryWorkExperience,
                    },
                },
            };
        })
            .sort((a, b) => {
            if (!a || !b) {
                return 0;
            }
            // Sort by dateStart (newest first), then by dateEnd (active first - null dateEnd comes first)
            const aDateStart = a.memberOrganizations.dateStart
                ? new Date(a.memberOrganizations.dateStart).getTime()
                : 0;
            const bDateStart = b.memberOrganizations.dateStart
                ? new Date(b.memberOrganizations.dateStart).getTime()
                : 0;
            if (aDateStart !== bDateStart) {
                return bDateStart - aDateStart; // Newest dateStart first
            }
            // If dateStart is the same, prioritize active memberships (null dateEnd)
            const aDateEnd = a.memberOrganizations.dateEnd;
            const bDateEnd = b.memberOrganizations.dateEnd;
            if (!aDateEnd && bDateEnd)
                return -1; // a is active, b is not
            if (aDateEnd && !bDateEnd)
                return 1; // b is active, a is not
            // Both have null dateEnd and dateStart - sort by createdAt, then alphabetically
            if (!aDateEnd && !bDateEnd && aDateStart === 0 && bDateStart === 0) {
                // First try to sort by createdAt
                const aCreatedAt = a.createdAt ? new Date(a.createdAt).getTime() : 0;
                const bCreatedAt = b.createdAt ? new Date(b.createdAt).getTime() : 0;
                if (aCreatedAt !== bCreatedAt) {
                    return bCreatedAt - aCreatedAt; // Newest createdAt first
                }
                // If createdAt is also the same, sort alphabetically by displayName
                const aName = (a.displayName || '').toLowerCase();
                const bName = (b.displayName || '').toLowerCase();
                return aName.localeCompare(bName);
            }
            if (!aDateEnd && !bDateEnd)
                return 0; // both are active with same dateStart
            // Both have dateEnd, sort by dateEnd (newest first)
            return new Date(bDateEnd).getTime() - new Date(aDateEnd).getTime();
        });
        return allOrganizations;
    }
    // Member organization creation
    async create(memberId, data) {
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        const repositoryOptions = { ...this.options, transaction };
        try {
            const qx = sequelizeRepository_1.default.getQueryExecutor(repositoryOptions);
            const dates = (0, common_1.sanitizeMemberOrganizationDateRange)(data.dateStart, data.dateEnd, true);
            const memberOrgData = {
                ...data,
                dateStart: dates.dateStart,
                dateEnd: dates.dateEnd,
            };
            // Clean up any soft-deleted entries
            await (0, data_access_layer_1.cleanSoftDeletedMemberOrganization)(qx, memberId, data.organizationId, memberOrgData);
            // Create new member organization
            const newMemberOrgId = await (0, data_access_layer_1.createMemberOrganization)(qx, memberId, memberOrgData);
            const orgAffiliationPolicyById = await (0, data_access_layer_1.fetchManyOrganizationAffiliationPolicies)(qx, [
                data.organizationId,
            ]);
            if (newMemberOrgId && orgAffiliationPolicyById.get(data.organizationId)) {
                await (0, data_access_layer_1.changeMemberOrganizationAffiliationOverrides)(qx, [
                    {
                        memberId,
                        memberOrganizationId: newMemberOrgId,
                        allowAffiliation: false,
                    },
                ]);
                await (0, member_segment_affiliations_1.deleteMemberSegmentAffiliations)(qx, { memberId, organizationId: data.organizationId });
            }
            // Fetch updated list
            const result = await this.list(memberId, transaction);
            await sequelizeRepository_1.default.commitTransaction(transaction);
            // Signal after commit so the workflow sees persisted changes
            await (0, common_services_1.signalMemberUpdate)(this.options.temporal, memberId, {
                memberOrganizationIds: [data.organizationId],
            });
            return result;
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw error;
        }
    }
    // Update member organization
    async update(id, memberId, data) {
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        const repositoryOptions = { ...this.options, transaction };
        try {
            const qx = sequelizeRepository_1.default.getQueryExecutor(repositoryOptions);
            const existing = await (0, data_access_layer_1.fetchMemberOrganizationById)(qx, id);
            if (!existing || existing.memberId !== memberId) {
                throw new common_1.Error404(`Member organization with id ${id} not found!`);
            }
            const hasDateStart = data.dateStart !== undefined;
            const hasDateEnd = data.dateEnd !== undefined;
            const targetDateRange = (0, common_1.sanitizeMemberOrganizationDateRange)(hasDateStart ? data.dateStart : existing.dateStart, hasDateEnd ? data.dateEnd : existing.dateEnd, true);
            const update = lodash_1.default.pickBy({
                organizationId: data.organizationId,
                title: data.title,
                dateStart: hasDateStart ? targetDateRange.dateStart : undefined,
                dateEnd: hasDateEnd ? targetDateRange.dateEnd : undefined,
                verified: data.verified,
                verifiedBy: data.verifiedBy,
            }, (v) => v !== undefined);
            await (0, data_access_layer_1.cleanSoftDeletedMemberOrganization)(qx, memberId, data.organizationId, update);
            await (0, data_access_layer_1.updateMemberOrganization)(qx, memberId, id, {
                ...update,
                source: types_1.OrganizationSource.UI,
            });
            const memberOrganizations = await (0, data_access_layer_1.fetchMemberOrganizations)(qx, memberId);
            const overlapBasis = { ...existing, ...update };
            const overlappingGroupedRows = (0, mapper_1.getOverlappingGroupedMemberOrganizations)(memberOrganizations, overlapBasis);
            const groupedUpdate = lodash_1.default.pickBy({
                // Keep grouped rows aligned for shared display fields; dates stay on the edited row
                title: data.title,
                verified: data.verified,
                verifiedBy: data.verifiedBy,
            }, (value) => value !== undefined);
            if (overlappingGroupedRows.length > 0 && Object.keys(groupedUpdate).length > 0) {
                for (const overlappingRow of overlappingGroupedRows) {
                    if (!overlappingRow.id) {
                        continue;
                    }
                    await (0, data_access_layer_1.updateMemberOrganization)(qx, memberId, overlappingRow.id, groupedUpdate);
                }
            }
            // Trigger recalculation for old and new orgs if changed
            const orgsToRecalculate = Array.from(new Set([existing.organizationId, data.organizationId])).filter((orgId) => Boolean(orgId));
            const result = await this.list(memberId, transaction);
            await sequelizeRepository_1.default.commitTransaction(transaction);
            // Signal after commit so the workflow sees persisted changes
            await (0, common_services_1.signalMemberUpdate)(this.options.temporal, memberId, {
                memberOrganizationIds: orgsToRecalculate,
            });
            return result;
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw error;
        }
    }
    // Delete member organization
    async delete(id, memberId) {
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        const repositoryOptions = { ...this.options, transaction };
        try {
            const qx = sequelizeRepository_1.default.getQueryExecutor(repositoryOptions);
            const existingMemberOrganizations = await (0, data_access_layer_1.fetchMemberOrganizations)(qx, memberId);
            const memberOrganizationToBeDeleted = existingMemberOrganizations.find((mo) => mo.id === id);
            if (!memberOrganizationToBeDeleted) {
                throw new common_1.Error404(`Member organization with id ${id} not found!`);
            }
            const overlappingGroupedRows = (0, mapper_1.getOverlappingGroupedMemberOrganizations)(existingMemberOrganizations, memberOrganizationToBeDeleted);
            const memberOrganizationIdsToDelete = [
                id,
                ...overlappingGroupedRows.flatMap((row) => (row.id ? [row.id] : [])),
            ];
            // Delete hidden grouped rows with the visible row so list responses stay consistent
            await (0, data_access_layer_1.deleteMemberOrganizations)(qx, memberId, memberOrganizationIdsToDelete, true, this.options.currentUser.id);
            const result = await this.list(memberId, transaction);
            await sequelizeRepository_1.default.commitTransaction(transaction);
            // Signal after commit so the workflow sees persisted changes
            await (0, common_services_1.signalMemberUpdate)(this.options.temporal, memberId, {
                memberOrganizationIds: [memberOrganizationToBeDeleted.organizationId],
                syncToOpensearch: true,
            });
            return result;
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw error;
        }
    }
}
exports.default = MemberOrganizationsService;
//# sourceMappingURL=memberOrganizationsService.js.map