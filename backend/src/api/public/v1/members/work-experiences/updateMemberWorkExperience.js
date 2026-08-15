"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.updateMemberWorkExperience = updateMemberWorkExperience;
const zod_1 = require("zod");
const audit_logs_1 = require("@crowd/audit-logs");
const common_1 = require("@crowd/common");
const common_services_1 = require("@crowd/common_services");
const data_access_layer_1 = require("@crowd/data-access-layer");
const sequelizeQueryExecutor_1 = require("@/database/sequelizeQueryExecutor");
const api_1 = require("@/utils/api");
const mapper_1 = require("@/utils/mapper");
const validation_1 = require("@/utils/validation");
/** Matches the active unique index on memberOrganizations (org + date range). */
function sameUniqueKey(a, b) {
    return (a.organizationId === b.organizationId &&
        (0, common_services_1.normalizeMemberOrganizationDate)(a.dateStart) === (0, common_services_1.normalizeMemberOrganizationDate)(b.dateStart) &&
        (0, common_services_1.normalizeMemberOrganizationDate)(a.dateEnd) === (0, common_services_1.normalizeMemberOrganizationDate)(b.dateEnd));
}
const paramsSchema = zod_1.z.object({
    memberId: zod_1.z.uuid(),
    workExperienceId: zod_1.z.uuid(),
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
async function updateMemberWorkExperience(req, res) {
    const { memberId, workExperienceId } = (0, validation_1.validateOrThrow)(paramsSchema, req.params);
    const data = (0, validation_1.validateOrThrow)(bodySchema, req.body);
    const qx = (0, sequelizeQueryExecutor_1.optionsQx)(req);
    const member = await (0, data_access_layer_1.findMemberById)(qx, memberId, [data_access_layer_1.MemberField.ID]);
    if (!member) {
        throw new common_1.NotFoundError('Member not found');
    }
    const memberOrgs = await (0, data_access_layer_1.fetchMemberOrganizations)(qx, memberId);
    const existing = memberOrgs.find((mo) => mo.id === workExperienceId);
    if (!existing) {
        throw new common_1.NotFoundError('Work experience not found');
    }
    let dates;
    try {
        dates = (0, common_1.sanitizeMemberOrganizationDateRange)(data.startDate, data.endDate, true);
    }
    catch (error) {
        throw new common_1.BadRequestError('Invalid work experience date range');
    }
    const update = {
        organizationId: data.organizationId,
        title: data.jobTitle,
        verified: data.verified,
        verifiedBy: data.verifiedBy,
        source: data.source,
        dateStart: dates.dateStart,
        dateEnd: dates.dateEnd,
    };
    let updated;
    await (0, audit_logs_1.captureApiChange)(req, (0, audit_logs_1.memberEditOrganizationsAction)(memberId, async (captureOldState, captureNewState) => {
        var _a;
        captureOldState(existing);
        await qx.tx(async (tx) => {
            // Avoid unique-index collisions before we UPDATE the visible row.
            const conflictingRows = memberOrgs.filter((row) => !!row.id &&
                row.id !== workExperienceId &&
                sameUniqueKey(row, {
                    organizationId: data.organizationId,
                    dateStart: dates.dateStart,
                    dateEnd: dates.dateEnd,
                }));
            // Conflict if a visible work experience with the same dates already exists. Throw a conflict error.
            const conflictingVisibleIds = conflictingRows
                .filter((row) => !(0, mapper_1.isCollapsibleMemberOrganization)(row))
                .map((row) => row.id)
                .filter((id) => !!id);
            if (conflictingVisibleIds.length > 0) {
                throw new common_1.ConflictError('A work experience with the same dates already exists');
            }
            // Conflict if a collapsible work experience with the same dates already exists.
            // Soft-delete it so the visible update can take that unique key.
            const conflictingHiddenIds = conflictingRows
                .filter((row) => (0, mapper_1.isCollapsibleMemberOrganization)(row))
                .map((row) => row.id)
                .filter((id) => !!id);
            if (conflictingHiddenIds.length > 0) {
                await (0, data_access_layer_1.deleteMemberOrganizations)(tx, memberId, conflictingHiddenIds);
            }
            // Fan-out below should not touch rows we just soft-deleted.
            const memberOrgsAfterConflict = memberOrgs.filter((row) => !row.id || !conflictingHiddenIds.includes(row.id));
            await (0, data_access_layer_1.cleanSoftDeletedMemberOrganization)(tx, memberId, data.organizationId, update);
            await (0, data_access_layer_1.updateMemberOrganization)(tx, memberId, workExperienceId, update);
            const overlapBasis = { ...existing, ...update };
            const overlappingGroupedRows = (0, mapper_1.getOverlappingGroupedMemberOrganizations)(memberOrgsAfterConflict, overlapBasis);
            const groupedUpdate = {};
            // Keep grouped rows aligned for shared display fields; dates stay on the edited row
            if (data.jobTitle !== undefined) {
                groupedUpdate.title = data.jobTitle;
            }
            if (data.verified !== undefined) {
                groupedUpdate.verified = data.verified;
            }
            if (data.verifiedBy !== undefined) {
                groupedUpdate.verifiedBy = data.verifiedBy;
            }
            if (overlappingGroupedRows.length > 0 && Object.keys(groupedUpdate).length > 0) {
                for (const overlappingRow of overlappingGroupedRows.filter((row) => !!row.id)) {
                    await (0, data_access_layer_1.updateMemberOrganization)(tx, memberId, overlappingRow.id, groupedUpdate);
                }
            }
        });
        // Signal after commit so the workflow sees persisted changes
        await (0, common_services_1.signalMemberUpdate)(req.temporal, memberId, {
            memberOrganizationIds: [data.organizationId],
        });
        const orgsMap = await (0, data_access_layer_1.fetchManyMemberOrgsWithOrgData)(qx, [memberId], { withDomains: true });
        const updatedMo = (0, mapper_1.groupMemberOrganizations)((_a = orgsMap.get(memberId)) !== null && _a !== void 0 ? _a : []).find((mo) => mo.id === workExperienceId);
        if (!updatedMo) {
            throw new common_1.NotFoundError('Work experience not found');
        }
        captureNewState(updatedMo);
        updated = (0, mapper_1.toMemberWorkExperience)(updatedMo);
    }));
    (0, api_1.ok)(res, updated);
}
//# sourceMappingURL=updateMemberWorkExperience.js.map