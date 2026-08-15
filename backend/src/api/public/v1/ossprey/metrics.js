"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.metricsHandler = metricsHandler;
const data_access_layer_1 = require("@crowd/data-access-layer");
const packagesDb_1 = require("@/db/packagesDb");
const api_1 = require("@/utils/api");
async function metricsHandler(req, res) {
    const qx = await (0, packagesDb_1.getPackagesQx)();
    const metrics = await (0, data_access_layer_1.getOsspreyMetrics)(qx);
    (0, api_1.ok)(res, metrics);
}
//# sourceMappingURL=metrics.js.map