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
exports.getBlastRadiusJob = getBlastRadiusJob;
const zod_1 = require("zod");
const common_1 = require("@crowd/common");
const blastRadiusDal = __importStar(require("@crowd/data-access-layer/src/packages/blastRadius"));
const packagesDb_1 = require("@/db/packagesDb");
const api_1 = require("@/utils/api");
const validation_1 = require("@/utils/validation");
const blastRadiusAnalysis_1 = require("./blastRadiusAnalysis");
const paramsSchema = zod_1.z.object({
    analysisId: zod_1.z.uuid(),
});
// 2b — poll a blast-radius analysis job. results/summary are only populated once
// status is 'done' — see toBlastRadiusAnalysis.
async function getBlastRadiusJob(req, res) {
    const { analysisId } = (0, validation_1.validateOrThrow)(paramsSchema, req.params);
    const qx = await (0, packagesDb_1.getPackagesQx)();
    const analysis = await blastRadiusDal.getAnalysisDetail(qx, analysisId);
    if (!analysis) {
        throw new common_1.NotFoundError();
    }
    const done = analysis.status === 'done';
    const verdictRows = done ? await blastRadiusDal.getVerdictResults(qx, analysisId) : [];
    const excludedByRangeCount = done
        ? await blastRadiusDal.getDependentsExcludedByRangeCount(qx, analysisId)
        : 0;
    (0, api_1.ok)(res, (0, blastRadiusAnalysis_1.toBlastRadiusAnalysis)(analysis, verdictRows, excludedByRangeCount));
}
//# sourceMappingURL=getBlastRadiusJob.js.map