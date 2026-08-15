"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.toAkritesExternalAdvisoryDetail = toAkritesExternalAdvisoryDetail;
// Maps the DB's (lowercased) severity vocabulary to the contract's enum. The DB
// normalizes the middle band to MEDIUM (initial_schema.sql; osv/extractSeverity.ts),
// whereas the Akrites contract calls that same level `moderate` — hence the explicit
// medium → moderate crosswalk. Anything unrecognized (or null) maps to null so the
// response never violates the published enum.
const SEVERITY_CROSSWALK = {
    critical: 'critical',
    high: 'high',
    medium: 'moderate',
    moderate: 'moderate',
    low: 'low',
};
function toAkritesSeverity(severity) {
    var _a;
    if (severity === null)
        return null;
    return (_a = SEVERITY_CROSSWALK[severity]) !== null && _a !== void 0 ? _a : null;
}
// Builds the AdvisoryDetail for a single purl from its DAL rows. Rows carry a null
// osvId sentinel for a found-but-advisory-less package (see AkritesExternalAdvisoryRow) —
// those are dropped here so `advisories` is an empty array, not a list with a null entry.
function toAkritesExternalAdvisoryDetail(purl, rows) {
    const advisories = rows
        .filter((r) => r.osvId !== null)
        .map((r) => ({
        osvId: r.osvId,
        severity: toAkritesSeverity(r.severity),
        resolution: r.resolution,
        isCritical: r.isCritical,
    }));
    return { purl, advisories };
}
//# sourceMappingURL=akritesExternalAdvisoryDetail.js.map