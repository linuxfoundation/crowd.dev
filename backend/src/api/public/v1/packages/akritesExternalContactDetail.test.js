"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const vitest_1 = require("vitest");
const akritesExternalContactDetail_1 = require("./akritesExternalContactDetail");
function baseRow(overrides = {}) {
    return {
        purl: 'pkg:npm/lodash',
        name: 'lodash',
        ecosystem: 'npm',
        securityPolicyUrl: 'https://example.org/SECURITY.md',
        vulnerabilityReportingUrl: null,
        bugBountyUrl: null,
        pvrEnabled: true,
        declaredRepositoryUrl: 'https://github.com/lodash/lodash.git',
        resolvedRepositoryUrl: 'https://github.com/lodash/lodash',
        repoMappingConfidence: 0.9,
        contactsLastRefreshed: '2024-01-01T00:00:00.000Z',
        securityContacts: [
            {
                channel: 'email',
                value: 'security@example.org',
                role: 'security-team',
                confidence: 'SECONDARY',
                score: 0.735,
            },
            {
                channel: 'github-pvr',
                value: 'https://example.org/advisories/new',
                role: 'security-team',
                confidence: 'PRIMARY',
                score: 0.94,
            },
        ],
        ...overrides,
    };
}
(0, vitest_1.describe)('toAkritesExternalContactDetail', () => {
    (0, vitest_1.it)('maps contacts with the confidence/score field renames', () => {
        const result = (0, akritesExternalContactDetail_1.toAkritesExternalContactDetail)(baseRow());
        (0, vitest_1.expect)(result.purl).toBe('pkg:npm/lodash');
        (0, vitest_1.expect)(result.contacts).toEqual([
            {
                channel: 'email',
                value: 'security@example.org',
                role: 'security-team',
                confidenceBand: 'SECONDARY',
                confidenceScore: 0.735,
            },
            {
                channel: 'github-pvr',
                value: 'https://example.org/advisories/new',
                role: 'security-team',
                confidenceBand: 'PRIMARY',
                confidenceScore: 0.94,
            },
        ]);
    });
    (0, vitest_1.it)('derives overallConfidenceBand from the highest-scoring contact, returned verbatim', () => {
        // max score 0.94 -> PRIMARY (not SECONDARY from the other, lower-scoring contact).
        (0, vitest_1.expect)((0, akritesExternalContactDetail_1.toAkritesExternalContactDetail)(baseRow()).overallConfidenceBand).toBe('PRIMARY');
    });
    (0, vitest_1.it)('returns the aggregate band on the internal scale (no crosswalk)', () => {
        const band = (confidence, score) => (0, akritesExternalContactDetail_1.toAkritesExternalContactDetail)(baseRow({
            securityContacts: [
                {
                    channel: 'email',
                    value: 'x@y.z',
                    role: 'maintainer',
                    confidence: confidence,
                    score,
                },
            ],
        })).overallConfidenceBand;
        (0, vitest_1.expect)(band('SECONDARY', 0.6)).toBe('SECONDARY');
        (0, vitest_1.expect)(band('FALLBACK', 0.4)).toBe('FALLBACK');
    });
    (0, vitest_1.it)('returns NONE band and an empty array when there are no contacts', () => {
        const nullContacts = (0, akritesExternalContactDetail_1.toAkritesExternalContactDetail)(baseRow({ securityContacts: null }));
        (0, vitest_1.expect)(nullContacts.contacts).toEqual([]);
        (0, vitest_1.expect)(nullContacts.overallConfidenceBand).toBe('NONE');
        const emptyContacts = (0, akritesExternalContactDetail_1.toAkritesExternalContactDetail)(baseRow({ securityContacts: [] }));
        (0, vitest_1.expect)(emptyContacts.overallConfidenceBand).toBe('NONE');
    });
    (0, vitest_1.it)('passes through the repo-sourced policy fields', () => {
        const result = (0, akritesExternalContactDetail_1.toAkritesExternalContactDetail)(baseRow({ vulnerabilityReportingUrl: 'https://example.org/report', pvrEnabled: false }));
        (0, vitest_1.expect)(result.securityPolicyUrl).toBe('https://example.org/SECURITY.md');
        (0, vitest_1.expect)(result.vulnerabilityReportingUrl).toBe('https://example.org/report');
        (0, vitest_1.expect)(result.pvrEnabled).toBe(false);
    });
    (0, vitest_1.it)('passes through the repo provenance fields when present', () => {
        const result = (0, akritesExternalContactDetail_1.toAkritesExternalContactDetail)(baseRow());
        (0, vitest_1.expect)(result.declaredRepositoryUrl).toBe('https://github.com/lodash/lodash.git');
        (0, vitest_1.expect)(result.resolvedRepositoryUrl).toBe('https://github.com/lodash/lodash');
        (0, vitest_1.expect)(result.repoMappingConfidence).toBe(0.9);
    });
    (0, vitest_1.it)('casts repoMappingConfidence from a numeric string (pg-promise numeric type)', () => {
        const result = (0, akritesExternalContactDetail_1.toAkritesExternalContactDetail)(baseRow({ repoMappingConfidence: '0.9' }));
        (0, vitest_1.expect)(result.repoMappingConfidence).toBe(0.9);
    });
    (0, vitest_1.it)('returns resolvedRepositoryUrl and repoMappingConfidence as null when there is no repo link', () => {
        const result = (0, akritesExternalContactDetail_1.toAkritesExternalContactDetail)(baseRow({ resolvedRepositoryUrl: null, repoMappingConfidence: null }));
        (0, vitest_1.expect)(result.resolvedRepositoryUrl).toBeNull();
        (0, vitest_1.expect)(result.repoMappingConfidence).toBeNull();
    });
    (0, vitest_1.it)('returns declaredRepositoryUrl as null when the package has no declared repository', () => {
        const result = (0, akritesExternalContactDetail_1.toAkritesExternalContactDetail)(baseRow({ declaredRepositoryUrl: null }));
        (0, vitest_1.expect)(result.declaredRepositoryUrl).toBeNull();
    });
});
//# sourceMappingURL=akritesExternalContactDetail.test.js.map