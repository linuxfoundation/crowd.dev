"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.AFFILIATION_TYPE = void 0;
exports.mapSegmentAffiliation = mapSegmentAffiliation;
exports.mapWorkExperienceAffiliation = mapWorkExperienceAffiliation;
exports.AFFILIATION_TYPE = {
    PROJECT: 'project',
    WORK_HISTORY: 'work-history',
};
function mapSegmentAffiliation(a) {
    var _a, _b, _c, _d;
    return {
        id: a.id,
        organizationId: a.organizationId,
        organizationName: a.organizationName,
        organizationLogo: (_a = a.organizationLogo) !== null && _a !== void 0 ? _a : null,
        verified: a.verified,
        verifiedBy: (_b = a.verifiedBy) !== null && _b !== void 0 ? _b : null,
        startDate: (_c = a.dateStart) !== null && _c !== void 0 ? _c : null,
        endDate: (_d = a.dateEnd) !== null && _d !== void 0 ? _d : null,
        type: exports.AFFILIATION_TYPE.PROJECT,
    };
}
function mapWorkExperienceAffiliation(a) {
    var _a, _b, _c, _d, _e, _f;
    return {
        id: a.id,
        organizationId: a.organizationId,
        organizationName: a.organizationName,
        organizationLogo: (_a = a.organizationLogo) !== null && _a !== void 0 ? _a : null,
        verified: (_b = a.verified) !== null && _b !== void 0 ? _b : false,
        verifiedBy: (_c = a.verifiedBy) !== null && _c !== void 0 ? _c : null,
        source: (_d = a.source) !== null && _d !== void 0 ? _d : null,
        startDate: (_e = a.dateStart) !== null && _e !== void 0 ? _e : null,
        endDate: (_f = a.dateEnd) !== null && _f !== void 0 ? _f : null,
        type: exports.AFFILIATION_TYPE.WORK_HISTORY,
    };
}
//# sourceMappingURL=mappers.js.map