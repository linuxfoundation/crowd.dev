"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const vitest_1 = require("vitest");
const temporal_1 = require("@crowd/temporal");
const ingestAkritesExternalContactDetail_1 = require("./ingestAkritesExternalContactDetail");
const { execute, getContactDetailsByPurls } = vitest_1.vi.hoisted(() => ({
    execute: vitest_1.vi.fn(),
    getContactDetailsByPurls: vitest_1.vi.fn(),
}));
vitest_1.vi.mock('@/db/packagesTemporal', () => ({
    getPackagesTemporalClient: vitest_1.vi.fn().mockResolvedValue({ workflow: { execute } }),
}));
vitest_1.vi.mock('@/db/packagesDb', () => ({
    getPackagesQx: vitest_1.vi.fn().mockResolvedValue({}),
}));
vitest_1.vi.mock('@crowd/data-access-layer', () => ({
    getContactDetailsByPurls,
}));
function baseRow(overrides = {}) {
    return {
        purl: 'pkg:npm/lodash',
        name: 'lodash',
        ecosystem: 'npm',
        securityPolicyUrl: null,
        vulnerabilityReportingUrl: null,
        bugBountyUrl: null,
        pvrEnabled: null,
        declaredRepositoryUrl: null,
        resolvedRepositoryUrl: null,
        repoMappingConfidence: null,
        contactsLastRefreshed: null,
        securityContacts: [],
        ...overrides,
    };
}
function mockReqRes(body) {
    execute.mockClear();
    getContactDetailsByPurls.mockClear();
    const req = { body };
    const json = vitest_1.vi.fn();
    const status = vitest_1.vi.fn().mockReturnValue({ json });
    const res = { status, json };
    return { req, res, status, json };
}
(0, vitest_1.describe)('ingestAkritesExternalContactDetail', () => {
    (0, vitest_1.it)('returns the existing contact detail without triggering the workflow when already ingested', async () => {
        getContactDetailsByPurls.mockResolvedValue([
            baseRow({ contactsLastRefreshed: '2024-01-01T00:00:00.000Z' }),
        ]);
        const { req, res, json } = mockReqRes({ purl: 'pkg:npm/lodash' });
        await (0, ingestAkritesExternalContactDetail_1.ingestAkritesExternalContactDetail)(req, res);
        (0, vitest_1.expect)(execute).not.toHaveBeenCalled();
        (0, vitest_1.expect)(getContactDetailsByPurls).toHaveBeenCalledTimes(1);
        (0, vitest_1.expect)(json).toHaveBeenCalledWith(vitest_1.expect.objectContaining({ purl: 'pkg:npm/lodash' }));
    });
    (0, vitest_1.it)('executes ingestSecurityContactsForPurlWorkflow and returns the re-read contact detail when never ingested', async () => {
        execute.mockResolvedValue({ found: true, repoId: 'repo-1' });
        getContactDetailsByPurls
            .mockResolvedValueOnce([
            baseRow({
                resolvedRepositoryUrl: 'https://github.com/lodash/lodash',
                contactsLastRefreshed: null,
            }),
        ])
            .mockResolvedValueOnce([baseRow({ contactsLastRefreshed: '2024-01-01T00:00:00.000Z' })]);
        const { req, res, json } = mockReqRes({ purl: 'pkg:npm/lodash' });
        await (0, ingestAkritesExternalContactDetail_1.ingestAkritesExternalContactDetail)(req, res);
        (0, vitest_1.expect)(execute).toHaveBeenCalledTimes(1);
        const [workflowType, options] = execute.mock.calls[0];
        (0, vitest_1.expect)(workflowType).toBe('ingestSecurityContactsForPurlWorkflow');
        (0, vitest_1.expect)(options.taskQueue).toBe('security-contacts-worker');
        (0, vitest_1.expect)(options.workflowId).toMatch(/^security-contacts-ondemand:[0-9a-f]{64}$/);
        (0, vitest_1.expect)(options.workflowIdConflictPolicy).toBe(temporal_1.WorkflowIdConflictPolicy.USE_EXISTING);
        (0, vitest_1.expect)(options.workflowIdReusePolicy).toBe(temporal_1.WorkflowIdReusePolicy.ALLOW_DUPLICATE);
        (0, vitest_1.expect)(options.args).toEqual(['pkg:npm/lodash']);
        (0, vitest_1.expect)(getContactDetailsByPurls).toHaveBeenCalledTimes(2);
        (0, vitest_1.expect)(getContactDetailsByPurls).toHaveBeenCalledWith(vitest_1.expect.anything(), ['pkg:npm/lodash']);
        (0, vitest_1.expect)(json).toHaveBeenCalledWith(vitest_1.expect.objectContaining({ purl: 'pkg:npm/lodash' }));
    });
    (0, vitest_1.it)('derives the same deterministic workflowId for the same purl', async () => {
        execute.mockResolvedValue({ found: true });
        getContactDetailsByPurls.mockResolvedValue([
            baseRow({
                resolvedRepositoryUrl: 'https://github.com/lodash/lodash',
                contactsLastRefreshed: null,
            }),
        ]);
        const { req: req1, res: res1 } = mockReqRes({ purl: 'pkg:npm/lodash' });
        await (0, ingestAkritesExternalContactDetail_1.ingestAkritesExternalContactDetail)(req1, res1);
        const id1 = execute.mock.calls[0][1].workflowId;
        const { req: req2, res: res2 } = mockReqRes({ purl: 'pkg:npm/lodash' });
        await (0, ingestAkritesExternalContactDetail_1.ingestAkritesExternalContactDetail)(req2, res2);
        const id2 = execute.mock.calls[0][1].workflowId;
        (0, vitest_1.expect)(id1).toBe(id2);
    });
    (0, vitest_1.it)('throws NotFoundError without executing the workflow when the purl is unknown', async () => {
        getContactDetailsByPurls.mockResolvedValue([]);
        const { req, res } = mockReqRes({ purl: 'pkg:npm/left-pad' });
        await (0, vitest_1.expect)((0, ingestAkritesExternalContactDetail_1.ingestAkritesExternalContactDetail)(req, res)).rejects.toThrow();
        (0, vitest_1.expect)(execute).not.toHaveBeenCalled();
        (0, vitest_1.expect)(getContactDetailsByPurls).toHaveBeenCalledTimes(1);
    });
    (0, vitest_1.it)('throws NotFoundError without executing the workflow when the package has no linked repo', async () => {
        getContactDetailsByPurls.mockResolvedValue([
            baseRow({ resolvedRepositoryUrl: null, contactsLastRefreshed: null }),
        ]);
        const { req, res } = mockReqRes({ purl: 'pkg:npm/left-pad' });
        await (0, vitest_1.expect)((0, ingestAkritesExternalContactDetail_1.ingestAkritesExternalContactDetail)(req, res)).rejects.toThrow();
        (0, vitest_1.expect)(execute).not.toHaveBeenCalled();
        (0, vitest_1.expect)(getContactDetailsByPurls).toHaveBeenCalledTimes(1);
    });
    (0, vitest_1.it)('throws NotFoundError when the workflow reports no linked repo', async () => {
        execute.mockResolvedValue({ found: false });
        getContactDetailsByPurls.mockResolvedValue([
            baseRow({
                resolvedRepositoryUrl: 'https://github.com/example/left-pad',
                contactsLastRefreshed: null,
            }),
        ]);
        const { req, res } = mockReqRes({ purl: 'pkg:npm/left-pad' });
        await (0, vitest_1.expect)((0, ingestAkritesExternalContactDetail_1.ingestAkritesExternalContactDetail)(req, res)).rejects.toThrow();
        (0, vitest_1.expect)(getContactDetailsByPurls).toHaveBeenCalledTimes(1);
    });
    (0, vitest_1.it)('throws NotFoundError when the re-read finds no row', async () => {
        execute.mockResolvedValue({ found: true });
        getContactDetailsByPurls
            .mockResolvedValueOnce([
            baseRow({
                resolvedRepositoryUrl: 'https://github.com/lodash/lodash',
                contactsLastRefreshed: null,
            }),
        ])
            .mockResolvedValueOnce([]);
        const { req, res } = mockReqRes({ purl: 'pkg:npm/lodash' });
        await (0, vitest_1.expect)((0, ingestAkritesExternalContactDetail_1.ingestAkritesExternalContactDetail)(req, res)).rejects.toThrow();
    });
    (0, vitest_1.it)('rejects a request missing purl without executing a workflow', async () => {
        const { req, res } = mockReqRes({});
        await (0, vitest_1.expect)((0, ingestAkritesExternalContactDetail_1.ingestAkritesExternalContactDetail)(req, res)).rejects.toThrow();
        (0, vitest_1.expect)(execute).not.toHaveBeenCalled();
    });
});
//# sourceMappingURL=ingestAkritesExternalContactDetail.test.js.map