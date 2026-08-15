"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getMyPackagesHandler = getMyPackagesHandler;
const zod_1 = require("zod");
const data_access_layer_1 = require("@crowd/data-access-layer");
const packagesDb_1 = require("@/db/packagesDb");
const api_1 = require("@/utils/api");
const validation_1 = require("@/utils/validation");
const querySchema = zod_1.z.object({
    page: zod_1.z.coerce.number().int().min(1).default(1),
    pageSize: zod_1.z.coerce.number().int().min(1).max(100).default(25),
    status: zod_1.z.enum(['assessing', 'active', 'needs_attention', 'escalated', 'blocked']).optional(),
    search: zod_1.z.string().trim().optional(),
    ecosystem: zod_1.z.string().trim().optional(),
    healthBand: zod_1.z.enum(['healthy', 'fair', 'concerning', 'critical']).optional(),
    vulnSeverity: zod_1.z.enum(['high', 'critical']).optional(),
    sortBy: zod_1.z.enum(['risk', 'health', 'vulns', 'name', 'last_activity']).default('risk'),
    sortDir: zod_1.z.enum(['asc', 'desc']).default('desc'),
});
async function getMyPackagesHandler(req, res) {
    const params = (0, validation_1.validateOrThrow)(querySchema, req.query);
    const qx = await (0, packagesDb_1.getPackagesQx)();
    const { rows, total, statusCounts } = await (0, data_access_layer_1.listMyPackages)(qx, {
        userId: req.actor.id,
        ...params,
    });
    (0, api_1.ok)(res, {
        data: rows.map((r) => ({
            purl: r.purl,
            name: r.name,
            ecosystem: r.ecosystem,
            lifecycle: r.lifecycle,
            healthScore: r.scorecardScore != null ? Math.round(r.scorecardScore * 10) : null,
            healthBand: (0, data_access_layer_1.computeHealthBand)(r.scorecardScore),
            openVulns: r.openVulns,
            vulnSeverity: r.maxVulnSeverity,
            lastActivityDescription: r.lastActivityDescription,
            lastActivityAt: r.lastActivityAt ? r.lastActivityAt.toISOString() : null,
            stewardshipId: r.stewardshipId,
            stewardshipStatus: r.stewardshipStatus,
            myRole: r.myRole,
        })),
        meta: {
            total,
            page: params.page,
            pageSize: params.pageSize,
            statusCounts,
        },
    });
}
//# sourceMappingURL=getMyPackages.js.map