"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.submitBlastRadiusJobBatch = submitBlastRadiusJobBatch;
const common_1 = require("@crowd/common");
const blastRadiusDal = __importStar(require("@crowd/data-access-layer/src/packages/blastRadius"));
const types_1 = require("@crowd/types");
const packagesDb_1 = require("@/db/packagesDb");
const packagesTemporal_1 = require("@/db/packagesTemporal");
const validation_1 = require("@/utils/validation");
const blastRadius_1 = require("./blastRadius");
const blastRadiusBatch_1 = require("./blastRadiusBatch");
// 2a bulk — submit multiple blast-radius analysis jobs in one request, one per
// array entry. Same lifecycle as the single-job submit, just looped: each entry
// gets its own analysisId, its own pending row, and its own Temporal workflow
// start. Unlike the read-only batch endpoints (packages/advisories/contacts),
// this multiplies workflow starts per request, so the batch size is capped much
// lower (see MAX_BLAST_RADIUS_JOBS_PER_BATCH) and the route stays behind the same
// strict blastRadiusRateLimiter as the single-job route.
//
// A per-job failure (e.g. workflow.start throwing) does not fail the whole
// batch — that job's entry comes back status: 'failed' and the rest still
// submit, matching the partial-result shape of the other batch endpoints.
async function submitBlastRadiusJobBatch(req, res) {
    const { jobs } = (0, validation_1.validateOrThrow)(blastRadiusBatch_1.blastRadiusJobBatchRequestSchema, req.body);
    const qx = await (0, packagesDb_1.getPackagesQx)();
    const results = await Promise.all(jobs.map((body) => submitOneJob(qx, body)));
    res.status(202).json({ results });
}
async function submitOneJob(qx, body) {
    var _a;
    const jobPackage = (_a = body.package) !== null && _a !== void 0 ? _a : null;
    const jobEcosystem = body.ecosystem;
    const analysisId = (0, common_1.generateUUIDv4)();
    const analysisInput = {
        id: analysisId,
        advisoryOsvId: body.advisoryId,
        packageName: jobPackage,
        ecosystem: jobEcosystem,
        force: body.force,
    };
    try {
        // Create the pending row synchronously, before starting the workflow — see the
        // same comment on submitBlastRadiusJob for why (avoids a poll-race 404). This is
        // inside the try too — unlike the single-job submit, a createAnalysis failure
        // must not reject the whole batch's Promise.all, only this job's entry.
        await blastRadiusDal.createAnalysis(qx, analysisInput);
        // Acquired per job (inside the try), not once up front — getPackagesTemporalClient
        // caches its connection in a module-level singleton, so this is cheap once
        // connected, but a first-ever connection failure must fail this job's entry only,
        // not reject the whole batch before any per-job try/catch is in play.
        const packagesTemporal = await (0, packagesTemporal_1.getPackagesTemporalClient)();
        await packagesTemporal.workflow.start('analyzeBlastRadius', {
            taskQueue: 'blast-radius-worker',
            workflowId: `${types_1.TemporalWorkflowId.BLAST_RADIUS_ANALYSIS}/${analysisId}`,
            retry: { maximumAttempts: 1 },
            args: [
                {
                    analysisId,
                    advisoryId: body.advisoryId,
                    package: jobPackage,
                    ecosystem: jobEcosystem,
                    force: body.force,
                },
            ],
        });
        return (0, blastRadius_1.toBlastRadiusJobEntry)({
            analysisId,
            advisoryId: body.advisoryId,
            package: jobPackage,
            ecosystem: jobEcosystem,
        });
    }
    catch (err) {
        // Unlike the single-job submit, this does not rethrow — one job's workflow
        // failing to start must not take the rest of the batch down with it. Same
        // reasoning applies to failAnalysis itself: if marking the row failed also
        // fails (e.g. transient DB error), that must not reject this job's promise
        // and take Promise.all (and the whole batch response) down with it.
        const errorMessage = err instanceof Error ? err.message : String(err);
        try {
            await blastRadiusDal.failAnalysis(qx, analysisInput, errorMessage);
        }
        catch (_b) {
            // best-effort — the job's entry below still reports status: 'failed'
        }
        return {
            analysisId,
            advisoryId: body.advisoryId,
            package: jobPackage,
            ecosystem: jobEcosystem,
            status: 'failed',
        };
    }
}
//# sourceMappingURL=submitBlastRadiusJobBatch.js.map