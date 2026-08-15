"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getAkritesExternalProjectProfiling = getAkritesExternalProjectProfiling;
const common_1 = require("@crowd/common");
const data_access_layer_1 = require("@crowd/data-access-layer");
const packagesDb_1 = require("@/db/packagesDb");
const api_1 = require("@/utils/api");
const validation_1 = require("@/utils/validation");
const akritesExternalProjectProfiling_1 = require("./akritesExternalProjectProfiling");
const purl_1 = require("./purl");
async function getAkritesExternalProjectProfiling(req, res) {
    const { purl } = (0, validation_1.validateOrThrow)(purl_1.purlQuerySchema, req.query);
    const qx = await (0, packagesDb_1.getPackagesQx)();
    const row = await (0, data_access_layer_1.getReportingProtocolByPurl)(qx, purl);
    if (!row) {
        throw new common_1.NotFoundError();
    }
    (0, api_1.ok)(res, (0, akritesExternalProjectProfiling_1.toAkritesExternalProjectProfiling)(row));
}
//# sourceMappingURL=getAkritesExternalProjectProfiling.js.map