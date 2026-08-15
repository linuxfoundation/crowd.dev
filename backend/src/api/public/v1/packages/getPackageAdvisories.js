"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getPackageAdvisories = getPackageAdvisories;
const zod_1 = require("zod");
const common_1 = require("@crowd/common");
const data_access_layer_1 = require("@crowd/data-access-layer");
const packagesDb_1 = require("@/db/packagesDb");
const api_1 = require("@/utils/api");
const validation_1 = require("@/utils/validation");
const purl_1 = require("./purl");
const DEFAULT_PAGE_SIZE = 20;
const MAX_PAGE_SIZE = 100;
const SEVERITY_VALUES = ['critical', 'high', 'moderate', 'low'];
const RESOLUTION_VALUES = ['open', 'patched'];
function toStringArray(v) {
    if (!v)
        return undefined;
    const vals = Array.isArray(v) ? v : [v];
    return vals
        .flatMap((s) => String(s).split(','))
        .map((s) => s.trim())
        .filter(Boolean);
}
const querySchema = purl_1.purlQuerySchema.extend({
    page: zod_1.z.coerce.number().int().min(1).default(1),
    pageSize: zod_1.z.coerce.number().int().min(1).max(MAX_PAGE_SIZE).default(DEFAULT_PAGE_SIZE),
    severity: zod_1.z.preprocess(toStringArray, zod_1.z.array(zod_1.z.enum(SEVERITY_VALUES)).optional()),
    resolution: zod_1.z.preprocess(toStringArray, zod_1.z.array(zod_1.z.enum(RESOLUTION_VALUES)).optional()),
    critical: zod_1.z
        .preprocess((v) => {
        if (v === 'true')
            return true;
        if (v === 'false')
            return false;
        return v;
    }, zod_1.z.boolean().optional())
        .optional(),
});
async function getPackageAdvisories(req, res) {
    const { purl, page, pageSize, severity, resolution, critical } = (0, validation_1.validateOrThrow)(querySchema, req.query);
    const qx = await (0, packagesDb_1.getPackagesQx)();
    const pkg = await (0, data_access_layer_1.getPackageDetailByPurl)(qx, purl);
    if (!pkg) {
        throw new common_1.NotFoundError();
    }
    const { rows, total } = await (0, data_access_layer_1.getAdvisoriesByPackageId)(qx, pkg.id, {
        page,
        pageSize,
        severities: severity,
        resolutions: resolution,
        critical,
    });
    (0, api_1.ok)(res, {
        page,
        pageSize,
        total,
        advisories: rows.map((a) => ({
            osvId: a.osvId,
            severity: a.severity,
            resolution: a.resolution,
            isCritical: a.isCritical,
        })),
    });
}
//# sourceMappingURL=getPackageAdvisories.js.map