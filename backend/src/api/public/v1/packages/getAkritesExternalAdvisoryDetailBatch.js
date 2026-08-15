"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getAkritesExternalAdvisoryDetailBatch = getAkritesExternalAdvisoryDetailBatch;
const data_access_layer_1 = require("@crowd/data-access-layer");
const packagesDb_1 = require("@/db/packagesDb");
const api_1 = require("@/utils/api");
const validation_1 = require("@/utils/validation");
const akritesExternalAdvisoryDetail_1 = require("./akritesExternalAdvisoryDetail");
const purl_1 = require("./purl");
const bodySchema = (0, purl_1.paginatedPurlsBodySchema)();
async function getAkritesExternalAdvisoryDetailBatch(req, res) {
    const { page, pageSize, total, pagedPurls, normalizedPurls } = (0, purl_1.paginatePurls)((0, validation_1.validateOrThrow)(bodySchema, req.body));
    const qx = await (0, packagesDb_1.getPackagesQx)();
    const rows = await (0, data_access_layer_1.getAdvisoriesByPurls)(qx, normalizedPurls);
    // Group the flat rows by purl. A found package always has >= 1 row (a null-osvId
    // sentinel when it has no advisories), so map presence == package found.
    const byPurl = new Map();
    for (const row of rows) {
        const existing = byPurl.get(row.purl);
        if (existing)
            existing.push(row);
        else
            byPurl.set(row.purl, [row]);
    }
    const results = pagedPurls.map((requestedPurl, i) => {
        const purlRows = byPurl.get(normalizedPurls[i]);
        return {
            requestedPurl,
            found: purlRows !== undefined,
            advisories: purlRows ? (0, akritesExternalAdvisoryDetail_1.toAkritesExternalAdvisoryDetail)(purlRows[0].purl, purlRows) : null,
        };
    });
    (0, api_1.ok)(res, { page, pageSize, total, results });
}
//# sourceMappingURL=getAkritesExternalAdvisoryDetailBatch.js.map