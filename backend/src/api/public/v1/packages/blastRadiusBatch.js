"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.blastRadiusJobPollBatchRequestSchema = exports.blastRadiusJobBatchRequestSchema = exports.MAX_BLAST_RADIUS_POLL_IDS_PER_BATCH = exports.MAX_BLAST_RADIUS_JOBS_PER_BATCH = void 0;
exports.paginateAnalysisIds = paginateAnalysisIds;
const zod_1 = require("zod");
const blastRadius_1 = require("./blastRadius");
const purl_1 = require("./purl");
// Read-only batches (packages/advisories/contacts) cap at 100 — cheap indexed
// lookups. Batch submit multiplies Temporal workflow starts (and their LLM
// reachability cost) per request, so it gets a much lower cap, independent of
// that constant. 20 is the hard limit agreed with Joana after the cost test
// (2026-07-23) — 10 is the recommended/default batch size for callers, not a
// separate enforced value, since `jobs` is an explicit client-provided array.
exports.MAX_BLAST_RADIUS_JOBS_PER_BATCH = 20;
// Polling is read-only, same cost profile as the other batches, so it reuses
// their 100 cap.
exports.MAX_BLAST_RADIUS_POLL_IDS_PER_BATCH = 100;
exports.blastRadiusJobBatchRequestSchema = zod_1.z.object({
    jobs: zod_1.z
        .array(blastRadius_1.blastRadiusJobRequestSchema)
        .min(1)
        .max(exports.MAX_BLAST_RADIUS_JOBS_PER_BATCH, `Maximum ${exports.MAX_BLAST_RADIUS_JOBS_PER_BATCH} jobs per request`),
});
// Unlike the read batches (purls that may or may not resolve to a package), every
// requested job produces a response entry — there is no "not found" case, so the
// response is a plain array in request order, not a found/not-found wrapper.
const analysisIdSchema = zod_1.z.uuid();
exports.blastRadiusJobPollBatchRequestSchema = zod_1.z.object({
    analysisIds: zod_1.z
        .array(analysisIdSchema)
        .min(1)
        .max(exports.MAX_BLAST_RADIUS_POLL_IDS_PER_BATCH, `Maximum ${exports.MAX_BLAST_RADIUS_POLL_IDS_PER_BATCH} analysisIds per request`),
    page: zod_1.z.coerce.number().int().min(1).default(1),
    pageSize: zod_1.z.coerce
        .number()
        .int()
        .min(1)
        .max(exports.MAX_BLAST_RADIUS_POLL_IDS_PER_BATCH)
        .default(purl_1.DEFAULT_BATCH_PAGE_SIZE),
});
// Mirrors paginatePurls (purl.ts): slice the requested page out of the full
// analysisIds array. No normalization step — analysisIds are UUIDs, unlike purls.
function paginateAnalysisIds(body) {
    const { analysisIds, page, pageSize } = body;
    const start = (page - 1) * pageSize;
    return {
        page,
        pageSize,
        total: analysisIds.length,
        pagedAnalysisIds: analysisIds.slice(start, start + pageSize),
    };
}
//# sourceMappingURL=blastRadiusBatch.js.map