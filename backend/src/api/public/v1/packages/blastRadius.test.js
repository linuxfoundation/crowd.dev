"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const vitest_1 = require("vitest");
const blastRadius_1 = require("./blastRadius");
(0, vitest_1.describe)('blastRadiusJobRequestSchema', () => {
    (0, vitest_1.it)('accepts a minimal advisory-wide request and defaults force to false', () => {
        const result = blastRadius_1.blastRadiusJobRequestSchema.parse({
            advisoryId: 'GHSA-jf85-cpcp-j695',
            ecosystem: 'npm',
        });
        (0, vitest_1.expect)(result).toEqual({
            advisoryId: 'GHSA-jf85-cpcp-j695',
            ecosystem: 'npm',
            package: undefined,
            force: false,
        });
    });
    (0, vitest_1.it)('accepts a request scoped to a package with ecosystem and force set', () => {
        const result = blastRadius_1.blastRadiusJobRequestSchema.parse({
            advisoryId: 'GHSA-jf85-cpcp-j695',
            ecosystem: 'npm',
            package: 'pkg:npm/lodash',
            force: true,
        });
        (0, vitest_1.expect)(result).toEqual({
            advisoryId: 'GHSA-jf85-cpcp-j695',
            ecosystem: 'npm',
            package: 'pkg:npm/lodash',
            force: true,
        });
    });
    (0, vitest_1.it)('accepts explicit null for package (advisory-wide, explicit)', () => {
        const result = blastRadius_1.blastRadiusJobRequestSchema.parse({
            advisoryId: 'GHSA-jf85-cpcp-j695',
            ecosystem: 'npm',
            package: null,
        });
        (0, vitest_1.expect)(result.package).toBeNull();
    });
    (0, vitest_1.it)('rejects a missing advisoryId', () => {
        const result = blastRadius_1.blastRadiusJobRequestSchema.safeParse({ ecosystem: 'npm' });
        (0, vitest_1.expect)(result.success).toBe(false);
    });
    (0, vitest_1.it)('rejects an empty advisoryId', () => {
        const result = blastRadius_1.blastRadiusJobRequestSchema.safeParse({
            advisoryId: '   ',
            ecosystem: 'npm',
        });
        (0, vitest_1.expect)(result.success).toBe(false);
    });
    (0, vitest_1.it)('rejects an advisoryId that is not a GHSA or CVE identifier', () => {
        const result = blastRadius_1.blastRadiusJobRequestSchema.safeParse({
            advisoryId: 'foo',
            ecosystem: 'npm',
        });
        (0, vitest_1.expect)(result.success).toBe(false);
    });
    (0, vitest_1.it)('accepts a CVE-formatted advisoryId', () => {
        const result = blastRadius_1.blastRadiusJobRequestSchema.safeParse({
            advisoryId: 'CVE-2024-12345',
            ecosystem: 'npm',
        });
        (0, vitest_1.expect)(result.success).toBe(true);
    });
    (0, vitest_1.it)('rejects a missing ecosystem', () => {
        const result = blastRadius_1.blastRadiusJobRequestSchema.safeParse({ advisoryId: 'GHSA-jf85-cpcp-j695' });
        (0, vitest_1.expect)(result.success).toBe(false);
    });
    (0, vitest_1.it)('rejects null ecosystem', () => {
        const result = blastRadius_1.blastRadiusJobRequestSchema.safeParse({
            advisoryId: 'GHSA-jf85-cpcp-j695',
            ecosystem: null,
        });
        (0, vitest_1.expect)(result.success).toBe(false);
    });
    (0, vitest_1.it)('rejects an unsupported ecosystem', () => {
        const result = blastRadius_1.blastRadiusJobRequestSchema.safeParse({
            advisoryId: 'GHSA-jf85-cpcp-j695',
            ecosystem: 'homebrew',
        });
        (0, vitest_1.expect)(result.success).toBe(false);
    });
    (0, vitest_1.it)('accepts pypi as a supported ecosystem', () => {
        const result = blastRadius_1.blastRadiusJobRequestSchema.safeParse({
            advisoryId: 'GHSA-jf85-cpcp-j695',
            ecosystem: 'pypi',
        });
        (0, vitest_1.expect)(result.success).toBe(true);
    });
});
(0, vitest_1.describe)('toBlastRadiusJobEntry', () => {
    (0, vitest_1.it)('builds a pending job entry echoing the request fields', () => {
        const entry = (0, blastRadius_1.toBlastRadiusJobEntry)({
            analysisId: 'br_01h',
            advisoryId: 'GHSA-jf85-cpcp-j695',
            package: 'pkg:npm/lodash',
            ecosystem: 'npm',
        });
        (0, vitest_1.expect)(entry).toEqual({
            analysisId: 'br_01h',
            advisoryId: 'GHSA-jf85-cpcp-j695',
            package: 'pkg:npm/lodash',
            ecosystem: 'npm',
            status: 'pending',
        });
    });
    (0, vitest_1.it)('echoes a null package for an advisory-wide job', () => {
        const entry = (0, blastRadius_1.toBlastRadiusJobEntry)({
            analysisId: 'br_01h',
            advisoryId: 'GHSA-jf85-cpcp-j695',
            package: null,
            ecosystem: 'npm',
        });
        (0, vitest_1.expect)(entry.package).toBeNull();
        (0, vitest_1.expect)(entry.ecosystem).toBe('npm');
        (0, vitest_1.expect)(entry.status).toBe('pending');
    });
});
//# sourceMappingURL=blastRadius.test.js.map