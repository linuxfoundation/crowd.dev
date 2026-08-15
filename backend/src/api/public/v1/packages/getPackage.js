"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getPackage = getPackage;
const common_1 = require("@crowd/common");
const data_access_layer_1 = require("@crowd/data-access-layer");
const packagesDb_1 = require("@/db/packagesDb");
const api_1 = require("@/utils/api");
const validation_1 = require("@/utils/validation");
const mappers_1 = require("./mappers");
const purl_1 = require("./purl");
const types_1 = require("./types");
const LIFECYCLE_SET = new Set(types_1.LIFECYCLE_VALUES);
async function getPackage(req, res) {
    var _a, _b, _c, _d, _e, _f, _g, _h, _j, _k, _l, _m, _o, _p, _q, _r, _s, _t, _u, _v;
    const { purl } = (0, validation_1.validateOrThrow)(purl_1.purlQuerySchema, req.query);
    const qx = await (0, packagesDb_1.getPackagesQx)();
    const pkg = await (0, data_access_layer_1.getPackageDetailByPurl)(qx, purl);
    if (!pkg) {
        throw new common_1.NotFoundError();
    }
    const [{ rows: advisories }, stewardshipSummary] = await Promise.all([
        (0, data_access_layer_1.getAdvisoriesByPackageId)(qx, pkg.id),
        pkg.stewardshipId ? (0, data_access_layer_1.getStewardshipSummary)(qx, Number(pkg.stewardshipId)) : null,
    ]);
    const scorecardScore = pkg.scorecardScore != null ? Number(pkg.scorecardScore) : null;
    const mappingConfidence = (0, mappers_1.toNullableNumber)(pkg.repoMappingConfidence);
    const securityContacts = pkg.contactsLastRefreshed == null
        ? null
        : ((_a = pkg.securityContacts) !== null && _a !== void 0 ? _a : []).map((c) => ({
            channel: c.channel,
            value: c.value,
            role: c.role,
            confidence: c.confidence,
            score: Number(c.score),
        }));
    const packageConfidence = securityContacts && securityContacts.length > 0
        ? (0, data_access_layer_1.securityContactConfidenceBand)(Math.max(...securityContacts.map((c) => c.score)))
        : null;
    (0, api_1.ok)(res, {
        purl: pkg.purl,
        name: pkg.name,
        ecosystem: pkg.ecosystem,
        latestVersion: (_b = pkg.latestVersion) !== null && _b !== void 0 ? _b : null,
        general: {
            healthScore: pkg.healthScore,
            healthScoreDetails: {
                total: pkg.healthScore,
                label: pkg.healthLabel != null && types_1.HEALTH_BAND_SET.has(pkg.healthLabel) ? pkg.healthLabel : null,
                maintainerHealth: pkg.maintainerHealthScore,
                securitySupplyChain: pkg.securitySupplyChainScore,
                developmentActivity: pkg.developmentActivityScore,
            },
            healthBand: pkg.healthLabel != null && types_1.HEALTH_BAND_SET.has(pkg.healthLabel)
                ? pkg.healthLabel
                : (0, data_access_layer_1.computeHealthBand)(scorecardScore),
            impact: {
                impactScore: pkg.criticalityScore != null ? Math.round(pkg.criticalityScore * 100) : null,
                downloadsLastMonth: (_c = pkg.downloadsLast30d) !== null && _c !== void 0 ? _c : null,
                dependentPackages: (_d = pkg.dependentPackagesCount) !== null && _d !== void 0 ? _d : null,
                dependentRepos: (_e = pkg.dependentReposCount) !== null && _e !== void 0 ? _e : null,
                transitiveReach: pkg.transitiveReach,
            },
            riskSignals: {
                lifecycle: pkg.lifecycleLabel != null && LIFECYCLE_SET.has(pkg.lifecycleLabel)
                    ? pkg.lifecycleLabel
                    : null,
                maintainerBusFactor: pkg.maintainerCount,
                lastRelease: pkg.latestReleaseAt ? pkg.latestReleaseAt.toISOString() : null,
                hasSecurityFile: pkg.hasSecurityFile,
                hasSecurityPolicy: pkg.hasSecurityPolicy,
                branchProtectionEnabled: pkg.branchProtectionEnabled,
                openSSFScorecard: scorecardScore,
            },
        },
        signalCoverageHealth: (0, mappers_1.snakeToCamelKeys)(pkg.signalCoverageHealth),
        assessment: null,
        security: {
            securityContacts,
            packageConfidence,
            securityPolicies: {
                securityPolicyUrl: (_f = pkg.securityPolicyUrl) !== null && _f !== void 0 ? _f : null,
                vulnerabilityReportingUrl: (_g = pkg.vulnerabilityReportingUrl) !== null && _g !== void 0 ? _g : null,
                bugBountyUrl: (_h = pkg.bugBountyUrl) !== null && _h !== void 0 ? _h : null,
                pvrEnabled: (_j = pkg.pvrEnabled) !== null && _j !== void 0 ? _j : null,
            },
            advisories: advisories.map((a) => ({
                osvId: a.osvId,
                severity: a.severity,
                resolution: a.resolution,
                isCritical: a.isCritical,
            })),
            cvd: {
                isPvrEnabled: (_k = pkg.pvrEnabled) !== null && _k !== void 0 ? _k : null,
                tier0Steward: null,
                criticalVulnerabilityFlag: pkg.hasCriticalVulnerability,
            },
        },
        provenance: {
            repositoryMapping: {
                declaredRepo: (_m = (_l = pkg.repoUrl) !== null && _l !== void 0 ? _l : pkg.repositoryUrl) !== null && _m !== void 0 ? _m : null,
                mappingConfidence,
                mappingLabel: (0, mappers_1.repoMappingLabel)(mappingConfidence),
                lastCommitAt: pkg.repoLastCommitAt ? pkg.repoLastCommitAt.toISOString() : null,
            },
            supplyChainIntegrity: {
                buildProvenance: null,
                signedReleases: null,
            },
        },
        stewardship: {
            id: (_o = pkg.stewardshipId) !== null && _o !== void 0 ? _o : null,
            status: ((_p = pkg.stewardshipStatus) !== null && _p !== void 0 ? _p : 'unassigned'),
            origin: (_q = pkg.stewardshipOrigin) !== null && _q !== void 0 ? _q : null,
            version: (_r = pkg.stewardshipVersion) !== null && _r !== void 0 ? _r : null,
            openedAt: pkg.stewardshipOpenedAt ? pkg.stewardshipOpenedAt.toISOString() : null,
            lastStatusAt: pkg.stewardshipLastStatusAt ? pkg.stewardshipLastStatusAt.toISOString() : null,
            resolutionPath: (_s = pkg.stewardshipResolutionPath) !== null && _s !== void 0 ? _s : null,
            statusNote: (_t = pkg.stewardshipStatusNote) !== null && _t !== void 0 ? _t : null,
            stewards: (_u = stewardshipSummary === null || stewardshipSummary === void 0 ? void 0 : stewardshipSummary.stewards) !== null && _u !== void 0 ? _u : null,
            lastActivityAt: (_v = stewardshipSummary === null || stewardshipSummary === void 0 ? void 0 : stewardshipSummary.lastActivityAt) !== null && _v !== void 0 ? _v : null,
        },
        history: null,
    });
}
//# sourceMappingURL=getPackage.js.map