"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const vitest_1 = require("vitest");
const getBlastRadiusJobBatch_1 = require("./getBlastRadiusJobBatch");
const { getAnalysisDetailsByIds, getVerdictResultsBatch, getDependentsExcludedByRangeCountBatch } = vitest_1.vi.hoisted(() => ({
    getAnalysisDetailsByIds: vitest_1.vi.fn(),
    getVerdictResultsBatch: vitest_1.vi.fn(),
    getDependentsExcludedByRangeCountBatch: vitest_1.vi.fn(),
}));
vitest_1.vi.mock('@crowd/data-access-layer/src/packages/blastRadius', () => ({
    getAnalysisDetailsByIds,
    getVerdictResultsBatch,
    getDependentsExcludedByRangeCountBatch,
}));
vitest_1.vi.mock('@/db/packagesDb', () => ({
    getPackagesQx: vitest_1.vi.fn().mockResolvedValue({}),
}));
const PENDING_ID = '11111111-1111-4111-8111-111111111111';
const DONE_ID = '22222222-2222-4222-8222-222222222222';
const MISSING_ID = '33333333-3333-4333-8333-333333333333';
function mockReqRes(body) {
    getAnalysisDetailsByIds.mockClear();
    getVerdictResultsBatch.mockClear();
    getDependentsExcludedByRangeCountBatch.mockClear();
    getVerdictResultsBatch.mockResolvedValue([]);
    getDependentsExcludedByRangeCountBatch.mockResolvedValue([]);
    const req = { body };
    const json = vitest_1.vi.fn();
    const status = vitest_1.vi.fn().mockReturnValue({ json });
    const res = { status, json };
    return { req, res, json };
}
(0, vitest_1.describe)('getBlastRadiusJobBatch', () => {
    (0, vitest_1.it)('returns found/not-found entries in request order with page metadata', async () => {
        getAnalysisDetailsByIds.mockResolvedValue([
            {
                id: PENDING_ID,
                advisory_osv_id: 'GHSA-jf85-cpcp-j695',
                package_name: null,
                ecosystem: 'npm',
                status: 'pending',
                error: null,
                candidates_considered: null,
                started_at: '2026-07-01T00:00:00.000Z',
                completed_at: null,
            },
        ]);
        const { req, res, json } = mockReqRes({
            analysisIds: [PENDING_ID, MISSING_ID],
        });
        await (0, getBlastRadiusJobBatch_1.getBlastRadiusJobBatch)(req, res);
        (0, vitest_1.expect)(json).toHaveBeenCalledWith(vitest_1.expect.objectContaining({
            page: 1,
            pageSize: vitest_1.expect.any(Number),
            total: 2,
            results: [
                vitest_1.expect.objectContaining({
                    requestedAnalysisId: PENDING_ID,
                    found: true,
                    analysis: vitest_1.expect.objectContaining({ analysisId: PENDING_ID, status: 'pending' }),
                }),
                { requestedAnalysisId: MISSING_ID, found: false, analysis: null },
            ],
        }));
    });
    (0, vitest_1.it)('only fetches verdicts/excluded counts for done analyses', async () => {
        const { req, res, json } = mockReqRes({ analysisIds: [PENDING_ID, DONE_ID] });
        getAnalysisDetailsByIds.mockResolvedValue([
            {
                id: PENDING_ID,
                advisory_osv_id: 'GHSA-jf85-cpcp-j695',
                package_name: null,
                ecosystem: 'npm',
                status: 'pending',
                error: null,
                candidates_considered: null,
                started_at: '2026-07-01T00:00:00.000Z',
                completed_at: null,
            },
            {
                id: DONE_ID,
                advisory_osv_id: 'GHSA-652q-gvq3-74qv',
                package_name: 'lodash',
                ecosystem: 'npm',
                status: 'done',
                error: null,
                candidates_considered: 10,
                started_at: '2026-07-01T00:00:00.000Z',
                completed_at: '2026-07-01T01:00:00.000Z',
            },
        ]);
        getVerdictResultsBatch.mockResolvedValue([
            {
                analysisId: DONE_ID,
                name: 'benchmark.js',
                version: '2.1.4',
                downloads: 500000,
                reachable_verdict: 'affected',
                confidence: 0.9,
                evidence: null,
                reasoning: 'uses merge',
            },
        ]);
        getDependentsExcludedByRangeCountBatch.mockResolvedValue([{ analysisId: DONE_ID, count: 8 }]);
        await (0, getBlastRadiusJobBatch_1.getBlastRadiusJobBatch)(req, res);
        (0, vitest_1.expect)(getVerdictResultsBatch).toHaveBeenCalledWith(vitest_1.expect.anything(), [DONE_ID]);
        (0, vitest_1.expect)(getDependentsExcludedByRangeCountBatch).toHaveBeenCalledWith(vitest_1.expect.anything(), [
            DONE_ID,
        ]);
        const [{ results }] = json.mock.calls[0];
        (0, vitest_1.expect)(results[0]).toMatchObject({ requestedAnalysisId: PENDING_ID, found: true });
        (0, vitest_1.expect)(results[0].analysis).toMatchObject({ status: 'pending', summary: null, results: null });
        (0, vitest_1.expect)(results[1]).toMatchObject({ requestedAnalysisId: DONE_ID, found: true });
        (0, vitest_1.expect)(results[1].analysis).toMatchObject({
            status: 'done',
            summary: vitest_1.expect.objectContaining({ dependentsExcludedUpfront: 8 }),
        });
    });
    (0, vitest_1.it)('matches an uppercase requestedAnalysisId against its (lowercase) row and echoes the original case', async () => {
        const uppercaseId = DONE_ID.toUpperCase();
        const { req, res, json } = mockReqRes({ analysisIds: [uppercaseId] });
        getAnalysisDetailsByIds.mockResolvedValue([
            {
                id: DONE_ID,
                advisory_osv_id: 'GHSA-652q-gvq3-74qv',
                package_name: 'lodash',
                ecosystem: 'npm',
                status: 'done',
                error: null,
                candidates_considered: 10,
                started_at: '2026-07-01T00:00:00.000Z',
                completed_at: '2026-07-01T01:00:00.000Z',
            },
        ]);
        getVerdictResultsBatch.mockResolvedValue([
            {
                analysisId: DONE_ID,
                name: 'benchmark.js',
                version: '2.1.4',
                downloads: 500000,
                reachable_verdict: 'affected',
                confidence: 0.9,
                evidence: null,
                reasoning: 'uses merge',
            },
        ]);
        getDependentsExcludedByRangeCountBatch.mockResolvedValue([{ analysisId: DONE_ID, count: 8 }]);
        await (0, getBlastRadiusJobBatch_1.getBlastRadiusJobBatch)(req, res);
        const [{ results }] = json.mock.calls[0];
        (0, vitest_1.expect)(results[0]).toMatchObject({ requestedAnalysisId: uppercaseId, found: true });
        (0, vitest_1.expect)(results[0].analysis).toMatchObject({
            status: 'done',
            summary: vitest_1.expect.objectContaining({ dependentsExcludedUpfront: 8 }),
        });
    });
    (0, vitest_1.it)('rejects a batch with a malformed uuid without querying the database', async () => {
        const { req, res } = mockReqRes({ analysisIds: [PENDING_ID, 'not-a-uuid'] });
        await (0, vitest_1.expect)((0, getBlastRadiusJobBatch_1.getBlastRadiusJobBatch)(req, res)).rejects.toThrow();
        (0, vitest_1.expect)(getAnalysisDetailsByIds).not.toHaveBeenCalled();
    });
    (0, vitest_1.it)('rejects a batch with more than 100 analysisIds', async () => {
        const analysisIds = Array.from({ length: 101 }, (_, i) => `44444444-4444-4444-8444-${String(i).padStart(12, '0')}`);
        const { req, res } = mockReqRes({ analysisIds });
        await (0, vitest_1.expect)((0, getBlastRadiusJobBatch_1.getBlastRadiusJobBatch)(req, res)).rejects.toThrow();
        (0, vitest_1.expect)(getAnalysisDetailsByIds).not.toHaveBeenCalled();
    });
});
//# sourceMappingURL=getBlastRadiusJobBatch.test.js.map