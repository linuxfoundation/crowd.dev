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
exports.getBlastRadiusJobBatch = getBlastRadiusJobBatch;
const blastRadiusDal = __importStar(require("@crowd/data-access-layer/src/packages/blastRadius"));
const packagesDb_1 = require("@/db/packagesDb");
const api_1 = require("@/utils/api");
const validation_1 = require("@/utils/validation");
const blastRadiusAnalysis_1 = require("./blastRadiusAnalysis");
const blastRadiusBatch_1 = require("./blastRadiusBatch");
// 2b bulk — poll multiple blast-radius analyses in one request. Same
// found/not-found echo shape as the other batch endpoints (packages, advisories,
// contacts): an unknown analysisId comes back { found: false, analysis: null }
// instead of 404ing the whole request. Read-only, so it stays behind the
// regular rateLimiter, not the strict blastRadiusRateLimiter.
async function getBlastRadiusJobBatch(req, res) {
    const { page, pageSize, total, pagedAnalysisIds } = (0, blastRadiusBatch_1.paginateAnalysisIds)((0, validation_1.validateOrThrow)(blastRadiusBatch_1.blastRadiusJobPollBatchRequestSchema, req.body));
    const qx = await (0, packagesDb_1.getPackagesQx)();
    const analysisRows = await blastRadiusDal.getAnalysisDetailsByIds(qx, pagedAnalysisIds);
    // Postgres normalizes uuid columns to lowercase on read, but a requested id can be
    // any case (schema only validates uuid shape) — normalize the lookup key so an
    // uppercase requestedAnalysisId still matches its (lowercase) row.
    const analysisById = new Map(analysisRows.map((row) => [row.id.toLowerCase(), row]));
    const doneIds = analysisRows.filter((row) => row.status === 'done').map((row) => row.id);
    const [verdictRows, excludedByRangeCounts] = await Promise.all([
        blastRadiusDal.getVerdictResultsBatch(qx, doneIds),
        blastRadiusDal.getDependentsExcludedByRangeCountBatch(qx, doneIds),
    ]);
    const verdictsByAnalysisId = new Map();
    for (const row of verdictRows) {
        const bucket = verdictsByAnalysisId.get(row.analysisId);
        if (bucket) {
            bucket.push(row);
        }
        else {
            verdictsByAnalysisId.set(row.analysisId, [row]);
        }
    }
    const excludedByRangeCountByAnalysisId = new Map(excludedByRangeCounts.map(({ analysisId, count }) => [analysisId, count]));
    const results = pagedAnalysisIds.map((requestedAnalysisId) => {
        var _a, _b;
        const analysis = analysisById.get(requestedAnalysisId.toLowerCase());
        if (!analysis) {
            return { requestedAnalysisId, found: false, analysis: null };
        }
        return {
            requestedAnalysisId,
            found: true,
            analysis: (0, blastRadiusAnalysis_1.toBlastRadiusAnalysis)(analysis, (_a = verdictsByAnalysisId.get(analysis.id)) !== null && _a !== void 0 ? _a : [], (_b = excludedByRangeCountByAnalysisId.get(analysis.id)) !== null && _b !== void 0 ? _b : 0),
        };
    });
    (0, api_1.ok)(res, { page, pageSize, total, results });
}
//# sourceMappingURL=getBlastRadiusJobBatch.js.map