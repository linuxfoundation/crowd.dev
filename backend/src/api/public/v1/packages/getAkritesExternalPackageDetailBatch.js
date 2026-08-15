"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getAkritesExternalPackageDetailBatch = getAkritesExternalPackageDetailBatch;
const data_access_layer_1 = require("@crowd/data-access-layer");
const packagesDb_1 = require("@/db/packagesDb");
const api_1 = require("@/utils/api");
const validation_1 = require("@/utils/validation");
const akritesExternalPackageDetail_1 = require("./akritesExternalPackageDetail");
const purl_1 = require("./purl");
const bodySchema = (0, purl_1.paginatedPurlsBodySchema)();
async function getAkritesExternalPackageDetailBatch(req, res) {
    const { page, pageSize, total, pagedPurls, normalizedPurls } = (0, purl_1.paginatePurls)((0, validation_1.validateOrThrow)(bodySchema, req.body));
    const qx = await (0, packagesDb_1.getPackagesQx)();
    const rows = await (0, data_access_layer_1.getPackageDetailsByPurls)(qx, normalizedPurls);
    const byPurl = new Map(rows.map((r) => [r.purl, r]));
    const results = pagedPurls.map((requestedPurl, i) => {
        const row = byPurl.get(normalizedPurls[i]);
        return {
            requestedPurl,
            found: row !== undefined,
            package: row ? (0, akritesExternalPackageDetail_1.toAkritesExternalPackageDetail)(row) : null,
        };
    });
    (0, api_1.ok)(res, { page, pageSize, total, results });
}
//# sourceMappingURL=getAkritesExternalPackageDetailBatch.js.map