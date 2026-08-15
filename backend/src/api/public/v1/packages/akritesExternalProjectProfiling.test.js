"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const vitest_1 = require("vitest");
const akritesExternalProjectProfiling_1 = require("./akritesExternalProjectProfiling");
function baseRow(overrides = {}) {
    return {
        purl: 'pkg:npm/lodash',
        declared: true,
        methods: [
            {
                type: 'github-pvr',
                status: 'preferred',
                endpoint: 'https://github.com/o/r/security/advisories/new',
                condition: null,
                confidence: 'declared',
                provenance: { api: 'pvr-flag' },
            },
        ],
        guidelines: null,
        sources: [{ api: 'pvr-flag' }],
        bugBountyUrl: 'https://example.org/bug-bounty',
        assembledAt: '2026-07-29 16:17:37.374211+00',
        ...overrides,
    };
}
(0, vitest_1.describe)('toAkritesExternalProjectProfiling', () => {
    (0, vitest_1.it)('maps a declared protocol and normalizes assembledAt to ISO 8601', () => {
        const result = (0, akritesExternalProjectProfiling_1.toAkritesExternalProjectProfiling)(baseRow());
        (0, vitest_1.expect)(result.purl).toBe('pkg:npm/lodash');
        (0, vitest_1.expect)(result.declared).toBe(true);
        (0, vitest_1.expect)(result.methods).toHaveLength(1);
        (0, vitest_1.expect)(result.methods[0]).toMatchObject({
            type: 'github-pvr',
            status: 'preferred',
            confidence: 'declared',
        });
        (0, vitest_1.expect)(result.sources).toEqual([{ api: 'pvr-flag' }]);
        (0, vitest_1.expect)(result.bugBountyUrl).toBe('https://example.org/bug-bounty');
        (0, vitest_1.expect)(result.assembledAt).toBe('2026-07-29T16:17:37.374Z');
    });
    (0, vitest_1.it)('passes guidelines through when present', () => {
        const guidelines = { generalPrinciples: ['coordinate disclosure'], avoid: [], recommend: [] };
        const result = (0, akritesExternalProjectProfiling_1.toAkritesExternalProjectProfiling)(baseRow({ guidelines }));
        (0, vitest_1.expect)(result.guidelines).toEqual(guidelines);
    });
    (0, vitest_1.it)('defaults null/absent jsonb collections and unparseable assembledAt', () => {
        const result = (0, akritesExternalProjectProfiling_1.toAkritesExternalProjectProfiling)(baseRow({
            declared: false,
            methods: null,
            sources: null,
            bugBountyUrl: null,
            assembledAt: null,
        }));
        (0, vitest_1.expect)(result.declared).toBe(false);
        (0, vitest_1.expect)(result.methods).toEqual([]);
        (0, vitest_1.expect)(result.sources).toEqual([]);
        (0, vitest_1.expect)(result.guidelines).toBeNull();
        (0, vitest_1.expect)(result.bugBountyUrl).toBeNull();
        (0, vitest_1.expect)(result.assembledAt).toBeNull();
    });
    (0, vitest_1.it)('returns null for a non-null but unparseable assembledAt', () => {
        const result = (0, akritesExternalProjectProfiling_1.toAkritesExternalProjectProfiling)(baseRow({ assembledAt: 'not-a-timestamp' }));
        (0, vitest_1.expect)(result.assembledAt).toBeNull();
    });
});
//# sourceMappingURL=akritesExternalProjectProfiling.test.js.map