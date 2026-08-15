"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const vitest_1 = require("vitest");
const akritesExternalAdvisoryDetail_1 = require("./akritesExternalAdvisoryDetail");
const purl = 'pkg:npm/lodash';
function row(overrides = {}) {
    return {
        purl,
        osvId: 'GHSA-1111-1111-1111',
        severity: 'critical',
        resolution: 'open',
        isCritical: true,
        ...overrides,
    };
}
(0, vitest_1.describe)('toAkritesExternalAdvisoryDetail', () => {
    (0, vitest_1.it)('maps well-formed advisory rows to the akrites-external shape', () => {
        const result = (0, akritesExternalAdvisoryDetail_1.toAkritesExternalAdvisoryDetail)(purl, [
            row({ osvId: 'GHSA-aaaa', severity: 'critical', resolution: 'open', isCritical: true }),
            row({ osvId: 'GHSA-bbbb', severity: 'high', resolution: 'patched', isCritical: false }),
        ]);
        (0, vitest_1.expect)(result.purl).toBe(purl);
        (0, vitest_1.expect)(result.advisories).toEqual([
            { osvId: 'GHSA-aaaa', severity: 'critical', resolution: 'open', isCritical: true },
            { osvId: 'GHSA-bbbb', severity: 'high', resolution: 'patched', isCritical: false },
        ]);
    });
    (0, vitest_1.it)('returns an empty advisories array for the found-but-advisory-less sentinel row', () => {
        // A found package with no advisories comes back as a single null-osvId row.
        const result = (0, akritesExternalAdvisoryDetail_1.toAkritesExternalAdvisoryDetail)(purl, [
            row({ osvId: null, severity: null, resolution: null, isCritical: null }),
        ]);
        (0, vitest_1.expect)(result.advisories).toEqual([]);
    });
    (0, vitest_1.it)('crosswalks the DB medium severity to the contract moderate value', () => {
        // The DB normalizes the middle band to MEDIUM; the contract calls it moderate.
        const result = (0, akritesExternalAdvisoryDetail_1.toAkritesExternalAdvisoryDetail)(purl, [row({ severity: 'medium' })]);
        (0, vitest_1.expect)(result.advisories[0].severity).toBe('moderate');
    });
    (0, vitest_1.it)('coerces a severity outside the known vocabulary to null', () => {
        const result = (0, akritesExternalAdvisoryDetail_1.toAkritesExternalAdvisoryDetail)(purl, [row({ severity: 'info' })]);
        (0, vitest_1.expect)(result.advisories[0].severity).toBeNull();
    });
    (0, vitest_1.it)('passes through null severity and null isCritical (unscored advisory)', () => {
        const result = (0, akritesExternalAdvisoryDetail_1.toAkritesExternalAdvisoryDetail)(purl, [
            row({ osvId: 'GHSA-cccc', severity: null, resolution: 'open', isCritical: null }),
        ]);
        (0, vitest_1.expect)(result.advisories[0]).toEqual({
            osvId: 'GHSA-cccc',
            severity: null,
            resolution: 'open',
            isCritical: null,
        });
    });
    (0, vitest_1.it)('keeps the accepted moderate severity value', () => {
        const result = (0, akritesExternalAdvisoryDetail_1.toAkritesExternalAdvisoryDetail)(purl, [row({ severity: 'moderate' })]);
        (0, vitest_1.expect)(result.advisories[0].severity).toBe('moderate');
    });
});
//# sourceMappingURL=akritesExternalAdvisoryDetail.test.js.map