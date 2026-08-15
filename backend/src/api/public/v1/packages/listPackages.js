"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.listPackages = listPackages;
const zod_1 = require("zod");
const data_access_layer_1 = require("@crowd/data-access-layer");
const packagesDb_1 = require("@/db/packagesDb");
const api_1 = require("@/utils/api");
const validation_1 = require("@/utils/validation");
const purl_1 = require("./purl");
const types_1 = require("./types");
const DEFAULT_PAGE_SIZE = 20;
const MAX_PAGE_SIZE = 100;
const booleanQueryParam = zod_1.z.preprocess((v) => v === 'true', zod_1.z.boolean()).default(false);
const LIFECYCLE_SET = new Set(types_1.LIFECYCLE_VALUES);
const vulnSeverityValues = ['any', 'high', 'critical', 'none'];
const querySchema = zod_1.z.object({
    page: zod_1.z.coerce.number().int().min(1).default(1),
    pageSize: zod_1.z.coerce.number().int().min(1).max(MAX_PAGE_SIZE).default(DEFAULT_PAGE_SIZE),
    ecosystem: zod_1.z.string().trim().optional(),
    lifecycle: zod_1.z.enum(types_1.LIFECYCLE_VALUES).optional(),
    name: zod_1.z.string().trim().optional(),
    purl: purl_1.purlFilterSchema,
    status: zod_1.z.enum(types_1.STEWARDSHIP_STATUS_VALUES).optional(),
    healthBand: zod_1.z.enum(types_1.HEALTH_BAND_VALUES).optional(),
    vulnSeverity: zod_1.z.enum(vulnSeverityValues).optional(),
    busFactor1Only: booleanQueryParam,
    staleOnly: booleanQueryParam,
    unstewardedOnly: booleanQueryParam,
    sortBy: zod_1.z.enum(['name', 'health', 'impact', 'openVulns', 'risk']).default('name'),
    sortDir: zod_1.z.enum(['asc', 'desc']).default('asc'),
});
async function listPackages(req, res) {
    const { page, pageSize, ecosystem, lifecycle, name, purl, status, healthBand, vulnSeverity, busFactor1Only, staleOnly, unstewardedOnly, sortBy, sortDir, } = (0, validation_1.validateOrThrow)(querySchema, req.query);
    const filterOpts = {
        ecosystem,
        lifecycle,
        name,
        purl,
        healthBand,
        vulnSeverity,
        staleOnly,
        unstewardedOnly,
        busFactor1Only,
    };
    const qx = await (0, packagesDb_1.getPackagesQx)();
    const [{ rows, total }, statusCounts] = await Promise.all([
        (0, data_access_layer_1.listPackagesForApi)(qx, {
            page,
            pageSize,
            status,
            sortBy,
            sortDir,
            ...filterOpts,
            includeStewards: true,
        }),
        (0, data_access_layer_1.getPackageStatusCounts)(qx, filterOpts),
    ]);
    const packages = rows.map((r) => {
        var _a, _b, _c, _d;
        return ({
            purl: r.purl,
            name: r.name,
            ecosystem: r.ecosystem,
            health: {
                score: (_a = r.healthScore) !== null && _a !== void 0 ? _a : (r.scorecardScore != null ? Math.round(r.scorecardScore * 10) : null),
                label: r.healthLabel != null && types_1.HEALTH_BAND_SET.has(r.healthLabel)
                    ? r.healthLabel
                    : (0, data_access_layer_1.computeHealthBand)(r.scorecardScore),
            },
            impact: r.criticalityScore != null ? Math.round(r.criticalityScore * 100) : null,
            lifecycle: r.lifecycleLabel != null && LIFECYCLE_SET.has(r.lifecycleLabel) ? r.lifecycleLabel : null,
            maintainerBusFactor: r.maintainerCount,
            openVulns: r.openVulns,
            stewardshipId: (_b = r.stewardshipId) !== null && _b !== void 0 ? _b : null,
            stewardship: ((_c = r.stewardshipStatus) !== null && _c !== void 0 ? _c : 'unassigned'),
            stewards: (_d = r.stewards) !== null && _d !== void 0 ? _d : [],
        });
    });
    (0, api_1.ok)(res, {
        page,
        pageSize,
        total,
        statusCounts,
        filters: {
            ecosystem: ecosystem !== null && ecosystem !== void 0 ? ecosystem : null,
            lifecycle: lifecycle !== null && lifecycle !== void 0 ? lifecycle : null,
            name: name !== null && name !== void 0 ? name : null,
            purl: purl !== null && purl !== void 0 ? purl : null,
            status: status !== null && status !== void 0 ? status : null,
            healthBand: healthBand !== null && healthBand !== void 0 ? healthBand : null,
            vulnSeverity: vulnSeverity !== null && vulnSeverity !== void 0 ? vulnSeverity : null,
            busFactor1Only,
            staleOnly,
            unstewardedOnly,
        },
        sort: { by: sortBy, dir: sortDir },
        packages,
    });
}
//# sourceMappingURL=listPackages.js.map