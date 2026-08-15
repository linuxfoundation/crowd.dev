"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getPackageHistory = getPackageHistory;
const common_1 = require("@crowd/common");
const data_access_layer_1 = require("@crowd/data-access-layer");
const packagesDb_1 = require("@/db/packagesDb");
const api_1 = require("@/utils/api");
const validation_1 = require("@/utils/validation");
const purl_1 = require("./purl");
async function getPackageHistory(req, res) {
    const { purl } = (0, validation_1.validateOrThrow)(purl_1.purlQuerySchema, req.query);
    const qx = await (0, packagesDb_1.getPackagesQx)();
    const pkg = await (0, data_access_layer_1.getPackageDetailByPurl)(qx, purl);
    if (!pkg) {
        throw new common_1.NotFoundError();
    }
    if (!pkg.stewardshipId) {
        (0, api_1.ok)(res, { events: [], total: 0 });
        return;
    }
    const events = await (0, data_access_layer_1.listPackageHistory)(qx, pkg.stewardshipId);
    (0, api_1.ok)(res, { events, total: events.length });
}
//# sourceMappingURL=getPackageHistory.js.map