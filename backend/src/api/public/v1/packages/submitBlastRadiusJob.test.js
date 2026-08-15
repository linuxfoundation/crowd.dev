"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const vitest_1 = require("vitest");
const submitBlastRadiusJob_1 = require("./submitBlastRadiusJob");
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
(0, vitest_1.describe)('submitBlastRadiusJob', () => {
    (0, vitest_1.it)('starts analyzeBlastRadius on the blast-radius-worker task queue and responds 202 pending', async () => {
        const { req, res, start, status, json } = mockReqRes({
            advisoryId: 'GHSA-jf85-cpcp-j695',
            ecosystem: 'npm',
        });
        await (0, submitBlastRadiusJob_1.submitBlastRadiusJob)(req, res);
        (0, vitest_1.expect)(createAnalysis).toHaveBeenCalledTimes(1);
        (0, vitest_1.expect)(createAnalysis.mock.calls[0][1]).toMatchObject({
            advisoryOsvId: 'GHSA-jf85-cpcp-j695',
            packageName: null,
            ecosystem: 'npm',
            force: false,
        });
        (0, vitest_1.expect)(start).toHaveBeenCalledTimes(1);
        const [workflowType, options] = start.mock.calls[0];
        (0, vitest_1.expect)(workflowType).toBe('analyzeBlastRadius');
        (0, vitest_1.expect)(options.taskQueue).toBe('blast-radius-worker');
        (0, vitest_1.expect)(options.workflowId).toMatch(/^blast-radius-analysis\//);
        (0, vitest_1.expect)(options.args[0]).toMatchObject({
            advisoryId: 'GHSA-jf85-cpcp-j695',
            package: null,
            ecosystem: 'npm',
            force: false,
        });
        (0, vitest_1.expect)(typeof options.args[0].analysisId).toBe('string');
        (0, vitest_1.expect)(status).toHaveBeenCalledWith(202);
        (0, vitest_1.expect)(json).toHaveBeenCalledWith(vitest_1.expect.objectContaining({
            advisoryId: 'GHSA-jf85-cpcp-j695',
            package: null,
            ecosystem: 'npm',
            status: 'pending',
        }));
    });
    (0, vitest_1.it)('passes package/ecosystem/force through to the workflow args and response', async () => {
        const { req, res, start, json } = mockReqRes({
            advisoryId: 'GHSA-jf85-cpcp-j695',
            package: 'pkg:npm/lodash',
            ecosystem: 'npm',
            force: true,
        });
        await (0, submitBlastRadiusJob_1.submitBlastRadiusJob)(req, res);
        const [, options] = start.mock.calls[0];
        (0, vitest_1.expect)(options.args[0]).toMatchObject({
            package: 'pkg:npm/lodash',
            ecosystem: 'npm',
            force: true,
        });
        (0, vitest_1.expect)(json).toHaveBeenCalledWith(vitest_1.expect.objectContaining({ package: 'pkg:npm/lodash', ecosystem: 'npm' }));
    });
    (0, vitest_1.it)('rejects a request missing advisoryId without starting a workflow', async () => {
        const { req, res, start } = mockReqRes({ ecosystem: 'npm' });
        await (0, vitest_1.expect)((0, submitBlastRadiusJob_1.submitBlastRadiusJob)(req, res)).rejects.toThrow();
        (0, vitest_1.expect)(start).not.toHaveBeenCalled();
        (0, vitest_1.expect)(createAnalysis).not.toHaveBeenCalled();
    });
    (0, vitest_1.it)('rejects an unsupported ecosystem without starting a workflow', async () => {
        const { req, res, start } = mockReqRes({
            advisoryId: 'GHSA-jf85-cpcp-j695',
            ecosystem: 'homebrew',
        });
        await (0, vitest_1.expect)((0, submitBlastRadiusJob_1.submitBlastRadiusJob)(req, res)).rejects.toThrow(/not supported/);
        (0, vitest_1.expect)(start).not.toHaveBeenCalled();
        (0, vitest_1.expect)(createAnalysis).not.toHaveBeenCalled();
    });
    (0, vitest_1.it)('starts a workflow for a pypi ecosystem request', async () => {
        const { req, res, start } = mockReqRes({
            advisoryId: 'GHSA-jf85-cpcp-j695',
            ecosystem: 'pypi',
        });
        await (0, submitBlastRadiusJob_1.submitBlastRadiusJob)(req, res);
        (0, vitest_1.expect)(start).toHaveBeenCalledTimes(1);
        const [, options] = start.mock.calls[0];
        (0, vitest_1.expect)(options.args[0]).toMatchObject({
            advisoryId: 'GHSA-jf85-cpcp-j695',
            ecosystem: 'pypi',
        });
    });
    (0, vitest_1.it)('rejects a missing ecosystem without starting a workflow', async () => {
        const { req, res, start } = mockReqRes({ advisoryId: 'GHSA-jf85-cpcp-j695' });
        await (0, vitest_1.expect)((0, submitBlastRadiusJob_1.submitBlastRadiusJob)(req, res)).rejects.toThrow(/not supported/);
        (0, vitest_1.expect)(start).not.toHaveBeenCalled();
        (0, vitest_1.expect)(createAnalysis).not.toHaveBeenCalled();
    });
    (0, vitest_1.it)('rejects an advisoryId that is not a GHSA or CVE identifier without starting a workflow', async () => {
        const { req, res, start } = mockReqRes({ advisoryId: 'foo', ecosystem: 'npm' });
        await (0, vitest_1.expect)((0, submitBlastRadiusJob_1.submitBlastRadiusJob)(req, res)).rejects.toThrow();
        (0, vitest_1.expect)(start).not.toHaveBeenCalled();
        (0, vitest_1.expect)(createAnalysis).not.toHaveBeenCalled();
    });
    (0, vitest_1.it)('marks the analysis failed and rethrows when workflow.start fails', async () => {
        const { req, res } = mockReqRes({
            advisoryId: 'GHSA-jf85-cpcp-j695',
            ecosystem: 'npm',
        });
        start.mockRejectedValueOnce(new Error('temporal unreachable'));
        await (0, vitest_1.expect)((0, submitBlastRadiusJob_1.submitBlastRadiusJob)(req, res)).rejects.toThrow('temporal unreachable');
        (0, vitest_1.expect)(failAnalysis).toHaveBeenCalledTimes(1);
        const [, input, errorMessage] = failAnalysis.mock.calls[0];
        (0, vitest_1.expect)(input).toMatchObject({
            advisoryOsvId: 'GHSA-jf85-cpcp-j695',
            packageName: null,
            ecosystem: 'npm',
            force: false,
        });
        (0, vitest_1.expect)(errorMessage).toBe('temporal unreachable');
    });
});
//# sourceMappingURL=submitBlastRadiusJob.test.js.map