"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const vitest_1 = require("vitest");
const akritesExternalPackageDetail_1 = require("./akritesExternalPackageDetail");
function baseRow(overrides = {}) {
    return {
        purl: 'pkg:npm/lodash',
        name: 'lodash',
        ecosystem: 'npm',
        latestVersion: '4.17.21',
        versionsCount: 120,
        criticalityScore: 0.5,
        dependentPackagesCount: 100,
        dependentReposCount: 50,
        healthScore: 80,
        healthLabel: 'healthy',
        maintainerHealthScore: 70,
        securitySupplyChainScore: 90,
        developmentActivityScore: 60,
        signalCoverageHealth: null,
        lifecycleLabel: 'active',
        latestReleaseAt: null,
        hasCriticalVulnerability: false,
        declaredRepositoryUrl: null,
        repositoryUrl: null,
        maintainerCount: 1,
        resolvedRepositoryUrl: null,
        repoMappingConfidence: null,
        repoLastCommitAt: null,
        scorecardScore: 7.5,
        hasSecurityFile: true,
        hasSecurityPolicy: true,
        branchProtectionEnabled: true,
        pvrEnabled: true,
        securityPolicyUrl: null,
        vulnerabilityReportingUrl: null,
        bugBountyUrl: null,
        downloadsLast30d: '1000',
        ...overrides,
    };
}
(0, vitest_1.describe)('toAkritesExternalPackageDetail', () => {
    (0, vitest_1.it)('maps a well-formed row to the akrites-external shape', () => {
        const result = (0, akritesExternalPackageDetail_1.toAkritesExternalPackageDetail)(baseRow());
        (0, vitest_1.expect)(result.purl).toBe('pkg:npm/lodash');
        (0, vitest_1.expect)(result.health.band).toBe('healthy');
        (0, vitest_1.expect)(result.riskSignals.lifecycle).toBe('active');
        (0, vitest_1.expect)(result.impact.score).toBe(50);
    });
    (0, vitest_1.it)('falls back to computeHealthBand(scorecardScore) for an unrecognized (non-null) healthLabel', () => {
        // scorecardScore 7.5 -> computeHealthBand() returns 'healthy'. A naive
        // `?? computeHealthBand(...)` (bug fixed in code review) would have trusted
        // 'not-a-real-label' as-is and returned it verbatim instead.
        const result = (0, akritesExternalPackageDetail_1.toAkritesExternalPackageDetail)(baseRow({ healthLabel: 'not-a-real-label', scorecardScore: 7.5 }));
        (0, vitest_1.expect)(result.health.band).toBe('healthy');
    });
    (0, vitest_1.it)('falls back to computeHealthBand(scorecardScore) when healthLabel is null', () => {
        const result = (0, akritesExternalPackageDetail_1.toAkritesExternalPackageDetail)(baseRow({ healthLabel: null, scorecardScore: 1 }));
        (0, vitest_1.expect)(result.health.band).toBe('critical');
    });
    (0, vitest_1.it)('returns every known internal health label verbatim', () => {
        (0, vitest_1.expect)((0, akritesExternalPackageDetail_1.toAkritesExternalPackageDetail)(baseRow({ healthLabel: 'excellent' })).health.band).toBe('excellent');
        (0, vitest_1.expect)((0, akritesExternalPackageDetail_1.toAkritesExternalPackageDetail)(baseRow({ healthLabel: 'concerning' })).health.band).toBe('concerning');
        (0, vitest_1.expect)((0, akritesExternalPackageDetail_1.toAkritesExternalPackageDetail)(baseRow({ healthLabel: 'critical' })).health.band).toBe('critical');
    });
    (0, vitest_1.it)('returns null lifecycle for an unrecognized (non-null) lifecycleLabel instead of throwing', () => {
        const result = (0, akritesExternalPackageDetail_1.toAkritesExternalPackageDetail)(baseRow({ lifecycleLabel: 'not-a-real-stage' }));
        (0, vitest_1.expect)(result.riskSignals.lifecycle).toBeNull();
    });
    (0, vitest_1.it)('returns internal lifecycle labels verbatim (stable, archived)', () => {
        (0, vitest_1.expect)((0, akritesExternalPackageDetail_1.toAkritesExternalPackageDetail)(baseRow({ lifecycleLabel: 'stable' })).riskSignals.lifecycle).toBe('stable');
        (0, vitest_1.expect)((0, akritesExternalPackageDetail_1.toAkritesExternalPackageDetail)(baseRow({ lifecycleLabel: 'archived' })).riskSignals.lifecycle).toBe('archived');
    });
    (0, vitest_1.it)('normalizes raw timestamptz strings (not Date objects) to ISO 8601', () => {
        // Timestamptz comes back from pg as a raw string, so a naive `.toISOString()`
        // on the row value would throw and 500 the request (bug fixed in code review).
        const result = (0, akritesExternalPackageDetail_1.toAkritesExternalPackageDetail)(baseRow({
            latestReleaseAt: '2024-01-15 12:30:00+00',
            repoLastCommitAt: '2024-02-20 08:00:00+00',
        }));
        (0, vitest_1.expect)(result.riskSignals.lastReleaseAt).toBe('2024-01-15T12:30:00.000Z');
        (0, vitest_1.expect)(result.provenance.lastCommitAt).toBe('2024-02-20T08:00:00.000Z');
    });
    (0, vitest_1.it)('falls back to the raw repositoryUrl column when resolvedRepositoryUrl has no package_repos link', () => {
        const result = (0, akritesExternalPackageDetail_1.toAkritesExternalPackageDetail)(baseRow({ resolvedRepositoryUrl: null, repositoryUrl: 'https://github.com/example/repo' }));
        (0, vitest_1.expect)(result.provenance.resolvedRepositoryUrl).toBe('https://github.com/example/repo');
    });
    (0, vitest_1.it)('prefers the confidence-joined resolvedRepositoryUrl over the raw repositoryUrl when both are present', () => {
        const result = (0, akritesExternalPackageDetail_1.toAkritesExternalPackageDetail)(baseRow({
            resolvedRepositoryUrl: 'https://github.com/example/resolved',
            repositoryUrl: 'https://github.com/example/raw',
        }));
        (0, vitest_1.expect)(result.provenance.resolvedRepositoryUrl).toBe('https://github.com/example/resolved');
    });
});
//# sourceMappingURL=akritesExternalPackageDetail.test.js.map