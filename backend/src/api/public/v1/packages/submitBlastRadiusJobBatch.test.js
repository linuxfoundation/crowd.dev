"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const vitest_1 = require("vitest");
const submitBlastRadiusJobBatch_1 = require("./submitBlastRadiusJobBatch");
const { start, createAnalysis, failAnalysis } = vitest_1.vi.hoisted(() => ({
    start: vitest_1.vi.fn().mockResolvedValue(undefined),
    createAnalysis: vitest_1.vi.fn().mockResolvedValue(undefined),
    failAnalysis: vitest_1.vi.fn().mockResolvedValue(undefined),
}));
vitest_1.vi.mock('@/db/packagesTemporal', () => ({
    getPackagesTemporalClient: vitest_1.vi.fn().mockResolvedValue({ workflow: { start } }),
}));
vitest_1.vi.mock('@/db/packagesDb', () => ({
    getPackagesQx: vitest_1.vi.fn().mockResolvedValue({}),
}));
vitest_1.vi.mock('@crowd/data-access-layer/src/packages/blastRadius', () => ({
    createAnalysis,
    failAnalysis,
}));
function mockReqRes(body) {
    start.mockClear();
    createAnalysis.mockClear();
    failAnalysis.mockClear();
    const req = { body };
    const json = vitest_1.vi.fn();
    const status = vitest_1.vi.fn().mockReturnValue({ json });
    const res = { status };
    return { req, res, start, status, json };
}
(0, vitest_1.describe)('submitBlastRadiusJobBatch', () => {
    (0, vitest_1.it)('starts one workflow per job and responds 202 with results in request order', async () => {
        const { req, res, status, json } = mockReqRes({
            jobs: [
                { advisoryId: 'GHSA-jf85-cpcp-j695', ecosystem: 'npm' },
                { advisoryId: 'GHSA-652q-gvq3-74qv', package: 'pkg:npm/lodash', ecosystem: 'npm' },
            ],
        });
        await (0, submitBlastRadiusJobBatch_1.submitBlastRadiusJobBatch)(req, res);
        (0, vitest_1.expect)(createAnalysis).toHaveBeenCalledTimes(2);
        (0, vitest_1.expect)(start).toHaveBeenCalledTimes(2);
        (0, vitest_1.expect)(status).toHaveBeenCalledWith(202);
        const [{ results }] = json.mock.calls[0];
        (0, vitest_1.expect)(results).toHaveLength(2);
        (0, vitest_1.expect)(results[0]).toMatchObject({
            advisoryId: 'GHSA-jf85-cpcp-j695',
            package: null,
            ecosystem: 'npm',
            status: 'pending',
        });
        (0, vitest_1.expect)(results[1]).toMatchObject({
            advisoryId: 'GHSA-652q-gvq3-74qv',
            package: 'pkg:npm/lodash',
            ecosystem: 'npm',
            status: 'pending',
        });
        (0, vitest_1.expect)(typeof results[0].analysisId).toBe('string');
        (0, vitest_1.expect)(typeof results[1].analysisId).toBe('string');
    });
    (0, vitest_1.it)('isolates a per-job workflow.start failure to that job only', async () => {
        const { req, res, json } = mockReqRes({
            jobs: [
                { advisoryId: 'GHSA-jf85-cpcp-j695', ecosystem: 'npm' },
                { advisoryId: 'GHSA-652q-gvq3-74qv', ecosystem: 'npm' },
            ],
        });
        start.mockResolvedValueOnce(undefined).mockRejectedValueOnce(new Error('temporal unreachable'));
        await (0, submitBlastRadiusJobBatch_1.submitBlastRadiusJobBatch)(req, res);
        const [{ results }] = json.mock.calls[0];
        (0, vitest_1.expect)(results).toHaveLength(2);
        (0, vitest_1.expect)(results[0]).toMatchObject({ advisoryId: 'GHSA-jf85-cpcp-j695', status: 'pending' });
        (0, vitest_1.expect)(results[1]).toMatchObject({ advisoryId: 'GHSA-652q-gvq3-74qv', status: 'failed' });
        (0, vitest_1.expect)(failAnalysis).toHaveBeenCalledTimes(1);
        const [, , errorMessage] = failAnalysis.mock.calls[0];
        (0, vitest_1.expect)(errorMessage).toBe('temporal unreachable');
    });
    (0, vitest_1.it)('rejects a batch containing an unsupported ecosystem without submitting any job', async () => {
        const { req, res, start } = mockReqRes({
            jobs: [
                { advisoryId: 'GHSA-jf85-cpcp-j695', ecosystem: 'npm' },
                { advisoryId: 'GHSA-652q-gvq3-74qv', ecosystem: 'homebrew' },
            ],
        });
        await (0, vitest_1.expect)((0, submitBlastRadiusJobBatch_1.submitBlastRadiusJobBatch)(req, res)).rejects.toThrow();
        (0, vitest_1.expect)(start).not.toHaveBeenCalled();
        (0, vitest_1.expect)(createAnalysis).not.toHaveBeenCalled();
    });
    (0, vitest_1.it)('accepts a batch containing a pypi job', async () => {
        const { req, res, start } = mockReqRes({
            jobs: [{ advisoryId: 'GHSA-652q-gvq3-74qv', ecosystem: 'pypi' }],
        });
        await (0, submitBlastRadiusJobBatch_1.submitBlastRadiusJobBatch)(req, res);
        (0, vitest_1.expect)(start).toHaveBeenCalledTimes(1);
    });
    (0, vitest_1.it)('rejects a batch with more than 20 jobs without submitting any job', async () => {
        const jobs = Array.from({ length: 21 }, () => ({
            advisoryId: 'GHSA-jf85-cpcp-j695',
            ecosystem: 'npm',
        }));
        const { req, res, start } = mockReqRes({ jobs });
        await (0, vitest_1.expect)((0, submitBlastRadiusJobBatch_1.submitBlastRadiusJobBatch)(req, res)).rejects.toThrow();
        (0, vitest_1.expect)(start).not.toHaveBeenCalled();
        (0, vitest_1.expect)(createAnalysis).not.toHaveBeenCalled();
    });
    (0, vitest_1.it)('rejects an empty jobs array', async () => {
        const { req, res, start } = mockReqRes({ jobs: [] });
        await (0, vitest_1.expect)((0, submitBlastRadiusJobBatch_1.submitBlastRadiusJobBatch)(req, res)).rejects.toThrow();
        (0, vitest_1.expect)(start).not.toHaveBeenCalled();
    });
});
//# sourceMappingURL=submitBlastRadiusJobBatch.test.js.map