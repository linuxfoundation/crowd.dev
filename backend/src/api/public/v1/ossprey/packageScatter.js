"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.packageScatterHandler = packageScatterHandler;
const zod_1 = require("zod");
const data_access_layer_1 = require("@crowd/data-access-layer");
const packagesDb_1 = require("@/db/packagesDb");
const api_1 = require("@/utils/api");
const validation_1 = require("@/utils/validation");
const types_1 = require("../packages/types");
const statusEnum = zod_1.z.enum(types_1.STEWARDSHIP_STATUS_VALUES);
function normalizeToArray(v) {
    if (v === undefined)
        return undefined;
    if (Array.isArray(v))
        return v;
    if (typeof v === 'string' && v.includes(','))
        return v
            .split(',')
            .map((s) => s.trim())
            .filter(Boolean);
    return [v];
}
const scatterQuerySchema = zod_1.z.object({
    status: zod_1.z.preprocess(normalizeToArray, zod_1.z.array(statusEnum).min(1)).optional(),
    ecosystem: zod_1.z.string().min(1).optional(),
});
async function packageScatterHandler(req, res) {
    const { status, ecosystem } = (0, validation_1.validateOrThrow)(scatterQuerySchema, req.query);
    const qx = await (0, packagesDb_1.getPackagesQx)();
    const points = await (0, data_access_layer_1.listPackagesForScatter)(qx, { status, ecosystem });
    (0, api_1.ok)(res, { points, total: points.length });
}
//# sourceMappingURL=packageScatter.js.map