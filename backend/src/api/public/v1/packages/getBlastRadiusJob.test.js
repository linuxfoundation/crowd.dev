"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const vitest_1 = require("vitest");
const getBlastRadiusJob_1 = require("./getBlastRadiusJob");
const { getAnalysisDetail, getVerdictResults, getDependentsExcludedByRangeCount } = vitest_1.vi.hoisted(() => ({
    getAnalysisDetail: vitest_1.vi.fn(),
    getVerdictResults: vitest_1.vi.fn(),
    getDependentsExcludedByRangeCount: vitest_1.vi.fn(),
}));
vitest_1.vi.mock('@crowd/data-access-layer/src/packages/blastRadius', () => ({
    getAnalysisDetail,
    getVerdictResults,
    getDependentsExcludedByRangeCount,
}));
vitest_1.vi.mock('@/db/packagesDb', () => ({
    getPackagesQx: vitest_1.vi.fn().mockResolvedValue({}),
}));
const ANALYSIS_ID = '11111111-1111-4111-8111-111111111111';
function mockReqRes(params) {
    getAnalysisDetail.mockClear();
    getVerdictResults.mockClear();
    getDependentsExcludedByRangeCount.mockClear();
    const req = { params };
    const json = vitest_1.vi.fn();
    const status = vitest_1.vi.fn().mockReturnValue({ json });
    const res = { status, json };
    return { req, res, status, json };
}
(0, vitest_1.describe)('getBlastRadiusJob', () => {
    (0, vitest_1.it)('returns a pending analysis with no results/summary', async () => {
        getAnalysisDetail.mockResolvedValue({
            id: ANALYSIS_ID,
            advisory_osv_id: 'GHSA-jf85-cpcp-j695',
            package_name: null,
            ecosystem: 'npm',
            status: 'pending',
            error: null,
            candidates_considered: null,
            started_at: '2026-07-01T00:00:00.000Z',
            completed_at: null,
        });
        const { req, res, json } = mockReqRes({ analysisId: ANALYSIS_ID });
        await (0, getBlastRadiusJob_1.getBlastRadiusJob)(req, res);
        (0, vitest_1.expect)(getVerdictResults).not.toHaveBeenCalled();
        (0, vitest_1.expect)(getDependentsExcludedByRangeCount).not.toHaveBeenCalled();
        (0, vitest_1.expect)(json).toHaveBeenCalledWith(vitest_1.expect.objectContaining({
            analysisId: ANALYSIS_ID,
            status: 'pending',
            summary: null,
            results: null,
        }));
    });
    (0, vitest_1.it)('returns summary and results for a done analysis', async () => {
        getAnalysisDetail.mockResolvedValue({
            id: ANALYSIS_ID,
            advisory_osv_id: 'GHSA-jf85-cpcp-j695',
            package_name: 'lodash',
            ecosystem: 'npm',
            status: 'done',
            error: null,
            candidates_considered: 10,
            started_at: '2026-07-01T00:00:00.000Z',
            completed_at: '2026-07-01T01:00:00.000Z',
        });
        getVerdictResults.mockResolvedValue([
            {
                name: 'benchmark.js',
                version: '2.1.4',
                downloads: 500000,
                reachable_verdict: 'affected',
                confidence: 0.9,
                evidence: [{ file: 'index.js', line: 10, snippet: 'require("lodash").merge' }],
                reasoning: 'uses merge',
            },
            {
                name: 'other-pkg',
                version: '1.0.0',
                downloads: 100,
                reachable_verdict: 'not_affected',
                confidence: 0.5,
                evidence: null,
                reasoning: 'unused',
            },
        ]);
        getDependentsExcludedByRangeCount.mockResolvedValue(8);
        const { req, res, json } = mockReqRes({ analysisId: ANALYSIS_ID });
        await (0, getBlastRadiusJob_1.getBlastRadiusJob)(req, res);
        (0, vitest_1.expect)(json).toHaveBeenCalledWith(vitest_1.expect.objectContaining({
            status: 'done',
            summary: vitest_1.expect.objectContaining({
                totalDependentsInRange: 10,
                dependentsAnalyzed: 2,
                dependentsExcludedUpfront: 8,
                dependentsAffected: 1,
                affectedPercentage: 50,
                affectedDependents: ['pkg:npm/benchmark.js'],
            }),
            results: [
                vitest_1.expect.objectContaining({
                    dependent: 'pkg:npm/benchmark.js',
                    affected: true,
                    verdict: 'affected',
                    confidence: 'high',
                }),
                vitest_1.expect.objectContaining({
                    dependent: 'pkg:npm/other-pkg',
                    affected: false,
                    verdict: 'not_affected',
                    confidence: 'medium',
                }),
            ],
        }));
    });
    (0, vitest_1.it)('404s when the analysis does not exist', async () => {
        getAnalysisDetail.mockResolvedValue(null);
        const { req, res } = mockReqRes({ analysisId: ANALYSIS_ID });
        await (0, vitest_1.expect)((0, getBlastRadiusJob_1.getBlastRadiusJob)(req, res)).rejects.toThrow();
    });
    (0, vitest_1.it)('rejects a non-uuid analysisId', async () => {
        const { req, res } = mockReqRes({ analysisId: 'not-a-uuid' });
        await (0, vitest_1.expect)((0, getBlastRadiusJob_1.getBlastRadiusJob)(req, res)).rejects.toThrow();
        (0, vitest_1.expect)(getAnalysisDetail).not.toHaveBeenCalled();
    });
});
//# sourceMappingURL=getBlastRadiusJob.test.js.map