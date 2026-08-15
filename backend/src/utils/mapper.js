"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.isCollapsibleMemberOrganization = isCollapsibleMemberOrganization;
exports.getOverlappingGroupedMemberOrganizations = getOverlappingGroupedMemberOrganizations;
exports.groupMemberOrganizations = groupMemberOrganizations;
exports.toMemberWorkExperience = toMemberWorkExperience;
const common_1 = require("@crowd/common");
const common_services_1 = require("@crowd/common_services");
const types_1 = require("@crowd/types");
function memberOrganizationsOverlap(a, b) {
    return (a.organizationId === b.organizationId &&
        (0, common_1.dateIntersects)((0, common_services_1.normalizeMemberOrganizationDate)(a.dateStart), (0, common_services_1.normalizeMemberOrganizationDate)(a.dateEnd), (0, common_services_1.normalizeMemberOrganizationDate)(b.dateStart), (0, common_services_1.normalizeMemberOrganizationDate)(b.dateEnd)));
}
function isCollapsibleMemberOrganization(row) {
    if (!row.source) {
        return false;
    }
    return row.source
        .split(',')
        .map((value) => value.trim())
        .some((value) => [types_1.OrganizationSource.EMAIL_DOMAIN, types_1.OrganizationSource.PROJECT_REGISTRY].includes(value));
}
function compareMemberOrganizationsBySourceRank(a, b) {
    var _a, _b;
    const rankDiff = (0, common_1.getMemberOrganizationSourceRank)(a.source) - (0, common_1.getMemberOrganizationSourceRank)(b.source);
    if (rankDiff !== 0) {
        return rankDiff;
    }
    return ((_a = a.id) !== null && _a !== void 0 ? _a : '').localeCompare((_b = b.id) !== null && _b !== void 0 ? _b : '');
}
/** Hidden inferential rows that overlap a visible work experience (for delete/update/override). */
function getOverlappingGroupedMemberOrganizations(rows, memberOrganization) {
    return rows.filter((row) => row.id !== memberOrganization.id &&
        isCollapsibleMemberOrganization(row) &&
        memberOrganizationsOverlap(row, memberOrganization));
}
function canDisplayCollapsibleRow(displayRow, collapsibleRow) {
    if (!isCollapsibleMemberOrganization(displayRow)) {
        return true;
    }
    return ((0, common_1.getMemberOrganizationSourceRank)(displayRow.source) <
        (0, common_1.getMemberOrganizationSourceRank)(collapsibleRow.source));
}
/** Collapse overlapping email-domain and project-registry rows into one work experience for display. */
function groupMemberOrganizations(rows) {
    const collapsibleRows = rows.filter((row) => !!row.id && isCollapsibleMemberOrganization(row));
    const hiddenCollapsibleIds = new Set();
    const collapsibleParentDisplayId = new Map();
    const displayGroups = new Map();
    for (const collapsibleRow of collapsibleRows) {
        const overlappingDisplayRows = rows.filter((row) => !!row.id &&
            row.id !== collapsibleRow.id &&
            memberOrganizationsOverlap(collapsibleRow, row) &&
            canDisplayCollapsibleRow(row, collapsibleRow));
        if (overlappingDisplayRows.length > 0) {
            const displayRow = [...overlappingDisplayRows].sort(compareMemberOrganizationsBySourceRank)[0];
            hiddenCollapsibleIds.add(collapsibleRow.id);
            collapsibleParentDisplayId.set(collapsibleRow.id, displayRow.id);
        }
    }
    const resolveDisplayRowId = (collapsibleRowId) => {
        let displayRowId = collapsibleRowId;
        while (collapsibleParentDisplayId.has(displayRowId)) {
            displayRowId = collapsibleParentDisplayId.get(displayRowId);
        }
        return displayRowId;
    };
    for (const collapsibleRow of collapsibleRows) {
        if (hiddenCollapsibleIds.has(collapsibleRow.id)) {
            const displayRowId = resolveDisplayRowId(collapsibleRow.id);
            const displayRow = rows.find((row) => row.id === displayRowId);
            if (displayRow) {
                const existingGroup = displayGroups.get(displayRowId);
                if (existingGroup) {
                    existingGroup.groupedRows.push(collapsibleRow);
                }
                else {
                    displayGroups.set(displayRowId, {
                        displayRow,
                        groupedRows: [collapsibleRow],
                    });
                }
            }
        }
    }
    return rows
        .filter((row) => !!row.id && !hiddenCollapsibleIds.has(row.id))
        .map((row) => {
        const group = displayGroups.get(row.id);
        if (!group) {
            return row;
        }
        const groupedRows = [group.displayRow, ...group.groupedRows];
        const normalizedStarts = groupedRows
            .map((groupedRow) => (0, common_services_1.normalizeMemberOrganizationDate)(groupedRow.dateStart))
            .filter((date) => date !== null);
        const normalizedEnds = groupedRows.map((groupedRow) => (0, common_services_1.normalizeMemberOrganizationDate)(groupedRow.dateEnd));
        const sources = new Set();
        for (const groupedRow of groupedRows) {
            if (groupedRow.source) {
                for (const source of groupedRow.source.split(',')) {
                    const trimmed = source.trim();
                    if (trimmed) {
                        sources.add(trimmed);
                    }
                }
            }
        }
        let dateEnd = null;
        if (normalizedEnds.some((date) => date === null)) {
            dateEnd = null;
        }
        else if (normalizedEnds.length > 0) {
            const datedEnds = normalizedEnds.filter((date) => date !== null);
            dateEnd = datedEnds.reduce((max, date) => (date > max ? date : max));
        }
        return {
            ...row,
            source: [...sources]
                .sort((a, b) => (0, common_1.getMemberOrganizationSourceRank)(a) - (0, common_1.getMemberOrganizationSourceRank)(b))
                .join(','),
            dateStart: normalizedStarts.length > 0
                ? normalizedStarts.reduce((min, date) => (date < min ? date : min))
                : null,
            dateEnd,
        };
    });
}
function toMemberWorkExperience(mo) {
    var _a, _b, _c, _d, _e, _f, _g, _h, _j;
    return {
        id: mo.id,
        organizationId: mo.organizationId,
        organizationName: mo.organizationName,
        organizationLogo: mo.organizationLogo,
        organizationDomains: (_a = mo.organizationDomains) !== null && _a !== void 0 ? _a : [],
        jobTitle: (_b = mo.title) !== null && _b !== void 0 ? _b : null,
        verified: (_c = mo.verified) !== null && _c !== void 0 ? _c : false,
        verifiedBy: (_d = mo.verifiedBy) !== null && _d !== void 0 ? _d : null,
        source: (_e = mo.source) !== null && _e !== void 0 ? _e : null,
        startDate: (_f = mo.dateStart) !== null && _f !== void 0 ? _f : null,
        endDate: (_g = mo.dateEnd) !== null && _g !== void 0 ? _g : null,
        createdAt: (_h = mo.createdAt) !== null && _h !== void 0 ? _h : null,
        updatedAt: (_j = mo.updatedAt) !== null && _j !== void 0 ? _j : null,
    };
}
//# sourceMappingURL=mapper.js.map