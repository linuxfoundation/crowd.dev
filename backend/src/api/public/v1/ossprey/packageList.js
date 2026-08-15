"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.packageListHandler = packageListHandler;
const zod_1 = require("zod");
const data_access_layer_1 = require("@crowd/data-access-layer");
const packagesDb_1 = require("@/db/packagesDb");
const api_1 = require("@/utils/api");
const validation_1 = require("@/utils/validation");
const purl_1 = require("../packages/purl");
const types_1 = require("../packages/types");
const MAX_PAGE_SIZE = 250;
const LIFECYCLE_SET = new Set(types_1.LIFECYCLE_VALUES);
const boolParam = zod_1.z.preprocess((v) => v === 'true', zod_1.z.boolean()).default(false);
const querySchema = zod_1.z
    .object({
    page: zod_1.z.coerce.number().int().min(1).default(1),
    pageSize: zod_1.z.coerce.number().int().min(1).max(MAX_PAGE_SIZE).default(25),
    ecosystem: zod_1.z.string().trim().optional(),
    lifecycle: zod_1.z.enum(types_1.LIFECYCLE_VALUES).optional(),
    name: zod_1.z.string().trim().optional(),
    purl: purl_1.purlFilterSchema,
    status: zod_1.z
        .enum([
        'unassigned',
        'open',
        'assessing',
        'active',
        'needs_attention',
        'escalated',
        'blocked',
        'inactive',
    ])
        .optional(),
    healthBand: zod_1.z.enum(types_1.HEALTH_BAND_VALUES).optional(),
    vulnSeverity: zod_1.z.enum(['any', 'high', 'critical', 'none']).optional(),
    staleOnly: boolParam,
    unstewardedOnly: boolParam,
    busFactor1Only: boolParam,
    sortBy: zod_1.z.enum(['name', 'risk', 'impact', 'openVulns', 'health']).default('risk'),
    sortDir: zod_1.z.enum(['asc', 'desc']).optional(),
})
    .transform((data) => {
    var _a;
    return ({
        ...data,
        sortDir: (_a = data.sortDir) !== null && _a !== void 0 ? _a : (data.sortBy === 'name' || data.sortBy === 'health' ? 'asc' : 'desc'),
    });
});
async function packageListHandler(req, res) {
    const params = (0, validation_1.validateOrThrow)(querySchema, req.query);
    const filterOpts = {
        ecosystem: params.ecosystem,
        lifecycle: params.lifecycle,
        name: params.name,
        purl: params.purl,
        healthBand: params.healthBand,
        vulnSeverity: params.vulnSeverity,
        staleOnly: params.staleOnly,
        unstewardedOnly: params.unstewardedOnly,
        busFactor1Only: params.busFactor1Only,
    };
    const qx = await (0, packagesDb_1.getPackagesQx)();
    const [{ rows, total }, statusCounts] = await Promise.all([
        (0, data_access_layer_1.listPackagesForApi)(qx, { ...params, includeStewards: true, includeLastActivity: true }),
        (0, data_access_layer_1.getPackageStatusCounts)(qx, filterOpts),
    ]);
    (0, api_1.ok)(res, {
        rows: rows.map((r) => {
            var _a, _b, _c, _d, _e, _f;
            return ({
                purl: r.purl,
                name: r.name,
                ecosystem: r.ecosystem,
                criticalityScore: r.criticalityScore,
                impact: r.criticalityScore != null ? Math.round(r.criticalityScore * 100) : null,
                stewardshipId: (_a = r.stewardshipId) !== null && _a !== void 0 ? _a : null,
                stewardshipStatus: (_b = r.stewardshipStatus) !== null && _b !== void 0 ? _b : null,
                openVulns: r.openVulns,
                maxVulnSeverity: (_c = r.maxVulnSeverity) !== null && _c !== void 0 ? _c : null,
                maintainerCount: r.maintainerCount,
                scorecardScore: r.scorecardScore,
                health: {
                    score: (_d = r.healthScore) !== null && _d !== void 0 ? _d : (r.scorecardScore != null ? Math.round(r.scorecardScore * 10) : null),
                    label: r.healthLabel != null && types_1.HEALTH_BAND_SET.has(r.healthLabel)
                        ? r.healthLabel
                        : (0, data_access_layer_1.computeHealthBand)(r.scorecardScore),
                },
                lifecycle: r.lifecycleLabel != null && LIFECYCLE_SET.has(r.lifecycleLabel) ? r.lifecycleLabel : null,
                latestReleaseAt: r.latestReleaseAt ? r.latestReleaseAt.toISOString() : null,
                lastActivity: r.lastActivityAt
                    ? {
                        type: r.lastActivityType,
                        content: (0, data_access_layer_1.translateActivityContent)((_e = r.lastActivityContent) !== null && _e !== void 0 ? _e : null, r.lastActivityType, r.lastActivityMetadata),
                        at: r.lastActivityAt.toISOString(),
                    }
                    : null,
                stewards: (_f = r.stewards) !== null && _f !== void 0 ? _f : [],
            });
        }),
        total,
        page: params.page,
        pageSize: params.pageSize,
        statusCounts,
    });
}
//# sourceMappingURL=packageList.js.map