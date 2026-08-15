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
exports.submitBlastRadiusJob = submitBlastRadiusJob;
const common_1 = require("@crowd/common");
const blastRadiusDal = __importStar(require("@crowd/data-access-layer/src/packages/blastRadius"));
const types_1 = require("@crowd/types");
const packagesDb_1 = require("@/db/packagesDb");
const packagesTemporal_1 = require("@/db/packagesTemporal");
const validation_1 = require("@/utils/validation");
const blastRadius_1 = require("./blastRadius");
// 2a — submit a blast-radius analysis job. Always exactly one job per request.
// Every submission gets a fresh analysisId and status pending.
async function submitBlastRadiusJob(req, res) {
    var _a;
    const body = (0, validation_1.validateOrThrow)(blastRadius_1.blastRadiusJobRequestSchema, req.body);
    const jobPackage = (_a = body.package) !== null && _a !== void 0 ? _a : null;
    const jobEcosystem = body.ecosystem;
    const analysisId = (0, common_1.generateUUIDv4)();
    // Create the pending row synchronously, before starting the workflow — otherwise a
    // client that polls GET /jobs/:analysisId immediately after this 202 can race
    // blastRadiusStart's own createAnalysis call and get a 404 for a job that was, in
    // fact, accepted. blastRadiusStart's createAnalysis upserts the same row, so this
    // is safe to run again from the workflow.
    const qx = await (0, packagesDb_1.getPackagesQx)();
    await blastRadiusDal.createAnalysis(qx, {
        id: analysisId,
        advisoryOsvId: body.advisoryId,
        packageName: jobPackage,
        ecosystem: jobEcosystem,
        force: body.force,
    });
    // blast-radius-worker polls the packages Temporal namespace, not the API's default
    // one (req.temporal) — starting it there would leave the workflow queued forever.
    const packagesTemporal = await (0, packagesTemporal_1.getPackagesTemporalClient)();
    try {
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
    }
    catch (err) {
        // Without this, a workflow.start failure (Temporal unreachable, task queue
        // misconfigured, etc.) leaves the row created above stuck 'pending' forever —
        // no workflow ever runs to mark it failed, so poll never reaches a terminal state.
        const errorMessage = err instanceof Error ? err.message : String(err);
        await blastRadiusDal.failAnalysis(qx, {
            id: analysisId,
            advisoryOsvId: body.advisoryId,
            packageName: jobPackage,
            ecosystem: jobEcosystem,
            force: body.force,
        }, errorMessage);
        throw err;
    }
    // 202, not the shared ok() helper (200) — the contract accepts the job, it does
    // not return a completed result.
    res.status(202).json((0, blastRadius_1.toBlastRadiusJobEntry)({
        analysisId,
        advisoryId: body.advisoryId,
        package: jobPackage,
        ecosystem: jobEcosystem,
    }));
}
//# sourceMappingURL=submitBlastRadiusJob.js.map