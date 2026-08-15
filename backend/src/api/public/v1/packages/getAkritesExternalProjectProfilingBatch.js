"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getAkritesExternalProjectProfilingBatch = getAkritesExternalProjectProfilingBatch;
const data_access_layer_1 = require("@crowd/data-access-layer");
const packagesDb_1 = require("@/db/packagesDb");
const api_1 = require("@/utils/api");
const validation_1 = require("@/utils/validation");
const akritesExternalProjectProfiling_1 = require("./akritesExternalProjectProfiling");
const purl_1 = require("./purl");
const bodySchema = (0, purl_1.paginatedPurlsBodySchema)();
async function getAkritesExternalProjectProfilingBatch(req, res) {
    const { page, pageSize, total, pagedPurls, normalizedPurls } = (0, purl_1.paginatePurls)((0, validation_1.validateOrThrow)(bodySchema, req.body));
    const qx = await (0, packagesDb_1.getPackagesQx)();
    const rows = await (0, data_access_layer_1.getReportingProtocolsByPurls)(qx, normalizedPurls);
    const byPurl = new Map(rows.map((r) => [r.purl, r]));
    const results = pagedPurls.map((requestedPurl, i) => {
        const row = byPurl.get(normalizedPurls[i]);
        return {
            requestedPurl,
            found: row !== undefined,
            profiling: row ? (0, akritesExternalProjectProfiling_1.toAkritesExternalProjectProfiling)(row) : null,
        };
    });
    (0, api_1.ok)(res, { page, pageSize, total, results });
}
//# sourceMappingURL=getAkritesExternalProjectProfilingBatch.js.map