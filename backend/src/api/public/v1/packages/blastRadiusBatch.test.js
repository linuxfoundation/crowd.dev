"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const vitest_1 = require("vitest");
const blastRadiusBatch_1 = require("./blastRadiusBatch");
(0, vitest_1.describe)('blastRadiusJobBatchRequestSchema', () => {
    (0, vitest_1.it)('accepts a batch of valid job requests', () => {
        const result = blastRadiusBatch_1.blastRadiusJobBatchRequestSchema.safeParse({
            jobs: [
                { advisoryId: 'GHSA-jf85-cpcp-j695', ecosystem: 'npm' },
                { advisoryId: 'CVE-2024-12345', ecosystem: 'npm', package: 'pkg:npm/lodash' },
            ],
        });
        (0, vitest_1.expect)(result.success).toBe(true);
    });
    (0, vitest_1.it)('rejects an empty jobs array', () => {
        const result = blastRadiusBatch_1.blastRadiusJobBatchRequestSchema.safeParse({ jobs: [] });
        (0, vitest_1.expect)(result.success).toBe(false);
    });
    (0, vitest_1.it)('rejects more than MAX_BLAST_RADIUS_JOBS_PER_BATCH jobs', () => {
        const jobs = Array.from({ length: blastRadiusBatch_1.MAX_BLAST_RADIUS_JOBS_PER_BATCH + 1 }, () => ({
            advisoryId: 'GHSA-jf85-cpcp-j695',
            ecosystem: 'npm',
        }));
        const result = blastRadiusBatch_1.blastRadiusJobBatchRequestSchema.safeParse({ jobs });
        (0, vitest_1.expect)(result.success).toBe(false);
    });
    (0, vitest_1.it)('rejects a batch containing one invalid job', () => {
        const result = blastRadiusBatch_1.blastRadiusJobBatchRequestSchema.safeParse({
            jobs: [
                { advisoryId: 'GHSA-jf85-cpcp-j695', ecosystem: 'npm' },
                { advisoryId: 'not-an-advisory-id', ecosystem: 'npm' },
            ],
        });
        (0, vitest_1.expect)(result.success).toBe(false);
    });
});
(0, vitest_1.describe)('blastRadiusJobPollBatchRequestSchema', () => {
    const validId = '3fa85f64-5717-4562-b3fc-2c963f66afa6';
    (0, vitest_1.it)('accepts a batch of valid analysisIds and defaults page/pageSize', () => {
        const result = blastRadiusBatch_1.blastRadiusJobPollBatchRequestSchema.parse({ analysisIds: [validId] });
        (0, vitest_1.expect)(result.page).toBe(1);
        (0, vitest_1.expect)(result.pageSize).toBe(20);
    });
    (0, vitest_1.it)('rejects an empty analysisIds array', () => {
        const result = blastRadiusBatch_1.blastRadiusJobPollBatchRequestSchema.safeParse({ analysisIds: [] });
        (0, vitest_1.expect)(result.success).toBe(false);
    });
    (0, vitest_1.it)('rejects a non-uuid analysisId', () => {
        const result = blastRadiusBatch_1.blastRadiusJobPollBatchRequestSchema.safeParse({ analysisIds: ['not-a-uuid'] });
        (0, vitest_1.expect)(result.success).toBe(false);
    });
    (0, vitest_1.it)('rejects more than MAX_BLAST_RADIUS_POLL_IDS_PER_BATCH analysisIds', () => {
        const analysisIds = Array.from({ length: blastRadiusBatch_1.MAX_BLAST_RADIUS_POLL_IDS_PER_BATCH + 1 }, () => validId);
        const result = blastRadiusBatch_1.blastRadiusJobPollBatchRequestSchema.safeParse({ analysisIds });
        (0, vitest_1.expect)(result.success).toBe(false);
    });
});
(0, vitest_1.describe)('paginateAnalysisIds', () => {
    (0, vitest_1.it)('slices the requested page out of the full analysisIds array', () => {
        const analysisIds = ['a', 'b', 'c', 'd', 'e'];
        const result = (0, blastRadiusBatch_1.paginateAnalysisIds)({ analysisIds, page: 2, pageSize: 2 });
        (0, vitest_1.expect)(result).toEqual({
            page: 2,
            pageSize: 2,
            total: 5,
            pagedAnalysisIds: ['c', 'd'],
        });
    });
    (0, vitest_1.it)('returns an empty page past the end of the array', () => {
        const result = (0, blastRadiusBatch_1.paginateAnalysisIds)({ analysisIds: ['a'], page: 2, pageSize: 20 });
        (0, vitest_1.expect)(result.pagedAnalysisIds).toEqual([]);
        (0, vitest_1.expect)(result.total).toBe(1);
    });
});
//# sourceMappingURL=blastRadiusBatch.test.js.map