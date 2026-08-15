"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.toAkritesExternalPackageDetail = toAkritesExternalPackageDetail;
const data_access_layer_1 = require("@crowd/data-access-layer");
const mappers_1 = require("./mappers");
const types_1 = require("./types");
const LIFECYCLE_SET = new Set(types_1.LIFECYCLE_VALUES);
function toAkritesHealthBand(healthLabel, scorecardScore) {
    // Same validity guard as getPackage.ts: an unrecognized (not just null) stored
    // label falls back to computeHealthBand() instead of silently miscategorizing.
    return healthLabel != null && types_1.HEALTH_BAND_SET.has(healthLabel)
        ? healthLabel
        : (0, data_access_layer_1.computeHealthBand)(scorecardScore);
}
function toAkritesLifecycle(lifecycleLabel) {
    if (lifecycleLabel === null || !LIFECYCLE_SET.has(lifecycleLabel))
        return null;
    return lifecycleLabel;
}
// Timestamptz columns arrive as the raw Postgres string (OID 1184 parser returns
// it verbatim — see AkritesExternalPackageDetailRow), so normalize to canonical
// ISO 8601 for the contract's date-time fields. Returns null for null/unparseable.
function toIsoOrNull(value) {
    if (value == null)
        return null;
    const ms = Date.parse(value);
    return Number.isNaN(ms) ? null : new Date(ms).toISOString();
}
function toAkritesExternalPackageDetail(row) {
    var _a, _b, _c, _d, _e, _f, _g, _h, _j, _k, _l, _m, _o, _p, _q;
    const scorecardScore = row.scorecardScore != null ? Number(row.scorecardScore) : null;
    const mappingConfidence = (0, mappers_1.toNullableNumber)(row.repoMappingConfidence);
    return {
        purl: row.purl,
        name: row.name,
        ecosystem: row.ecosystem,
        latestVersion: (_a = row.latestVersion) !== null && _a !== void 0 ? _a : null,
        versionCount: (_b = row.versionsCount) !== null && _b !== void 0 ? _b : null,
        health: {
            score: row.healthScore,
            band: toAkritesHealthBand(row.healthLabel, scorecardScore),
            subScores: {
                maintainerHealth: row.maintainerHealthScore,
                securitySupplyChain: row.securitySupplyChainScore,
                developmentActivity: row.developmentActivityScore,
            },
            signalCoverageHealth: (0, mappers_1.snakeToCamelKeys)(row.signalCoverageHealth),
        },
        impact: {
            score: row.criticalityScore != null ? Math.round(row.criticalityScore * 100) : null,
            downloadsLast30Days: (_c = row.downloadsLast30d) !== null && _c !== void 0 ? _c : null,
            dependentPackagesCount: (_d = row.dependentPackagesCount) !== null && _d !== void 0 ? _d : null,
            dependentReposCount: (_e = row.dependentReposCount) !== null && _e !== void 0 ? _e : null,
            transitiveReach: null,
        },
        riskSignals: {
            lifecycle: toAkritesLifecycle(row.lifecycleLabel),
            maintainerBusFactor: row.maintainerCount,
            lastReleaseAt: toIsoOrNull(row.latestReleaseAt),
            hasSecurityFile: (_f = row.hasSecurityFile) !== null && _f !== void 0 ? _f : null,
            hasSecurityPolicy: (_g = row.hasSecurityPolicy) !== null && _g !== void 0 ? _g : null,
            branchProtectionEnabled: (_h = row.branchProtectionEnabled) !== null && _h !== void 0 ? _h : null,
            openssfScorecardScore: scorecardScore,
        },
        security: {
            securityPolicyUrl: (_j = row.securityPolicyUrl) !== null && _j !== void 0 ? _j : null,
            vulnerabilityReportingUrl: (_k = row.vulnerabilityReportingUrl) !== null && _k !== void 0 ? _k : null,
            bugBountyUrl: (_l = row.bugBountyUrl) !== null && _l !== void 0 ? _l : null,
            pvrEnabled: (_m = row.pvrEnabled) !== null && _m !== void 0 ? _m : null,
            criticalVulnerabilityFlag: row.hasCriticalVulnerability,
        },
        provenance: {
            resolvedRepositoryUrl: (_p = (_o = row.resolvedRepositoryUrl) !== null && _o !== void 0 ? _o : row.repositoryUrl) !== null && _p !== void 0 ? _p : null,
            declaredRepositoryUrl: (_q = row.declaredRepositoryUrl) !== null && _q !== void 0 ? _q : null,
            mappingConfidenceScore: mappingConfidence,
            mappingConfidenceLabel: (0, mappers_1.repoMappingLabel)(mappingConfidence),
            lastCommitAt: toIsoOrNull(row.repoLastCommitAt),
        },
        supplyChainIntegrity: {
            buildProvenance: null,
            signedReleases: null,
        },
    };
}
//# sourceMappingURL=akritesExternalPackageDetail.js.map