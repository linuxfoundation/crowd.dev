"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getAkritesExternalAdvisoryDetail = getAkritesExternalAdvisoryDetail;
const common_1 = require("@crowd/common");
const data_access_layer_1 = require("@crowd/data-access-layer");
const packagesDb_1 = require("@/db/packagesDb");
const api_1 = require("@/utils/api");
const validation_1 = require("@/utils/validation");
const akritesExternalAdvisoryDetail_1 = require("./akritesExternalAdvisoryDetail");
const purl_1 = require("./purl");
async function getAkritesExternalAdvisoryDetail(req, res) {
    const { purl } = (0, validation_1.validateOrThrow)(purl_1.purlQuerySchema, req.query);
    const qx = await (0, packagesDb_1.getPackagesQx)();
    const rows = await (0, data_access_layer_1.getAdvisoriesByPurls)(qx, [purl]);
    // No rows at all means the package itself doesn't exist. A package that exists but
    // has no advisories still yields one (null-osvId) sentinel row, so it 200s with [].
    if (rows.length === 0) {
        throw new common_1.NotFoundError();
    }
    (0, api_1.ok)(res, (0, akritesExternalAdvisoryDetail_1.toAkritesExternalAdvisoryDetail)(purl, rows));
}
//# sourceMappingURL=getAkritesExternalAdvisoryDetail.js.map