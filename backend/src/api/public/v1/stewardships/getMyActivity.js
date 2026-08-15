"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getMyActivityHandler = getMyActivityHandler;
const zod_1 = require("zod");
const data_access_layer_1 = require("@crowd/data-access-layer");
const packagesDb_1 = require("@/db/packagesDb");
const api_1 = require("@/utils/api");
const validation_1 = require("@/utils/validation");
const VALID_STATUSES = [
    'assessing',
    'active',
    'needs_attention',
    'escalated',
    'blocked',
    'unassigned',
    'open',
    'inactive',
];
const querySchema = zod_1.z.object({
    page: zod_1.z.coerce.number().int().min(1).default(1),
    pageSize: zod_1.z.coerce.number().int().min(1).max(100).default(3),
    status: zod_1.z
        .string()
        .optional()
        .transform((v) => {
        if (!v)
            return undefined;
        const parts = v
            .split(',')
            .map((s) => s.trim())
            .filter(Boolean);
        return parts.length > 0 ? parts : undefined;
    })
        .pipe(zod_1.z.array(zod_1.z.enum(VALID_STATUSES)).optional()),
});
const SUGGESTED_ACTIONS = {
    needs_attention: 'Review & respond',
    blocked: 'Resolve blocker',
    escalated: 'Add escalation context',
    assessing: 'Continue assessment',
    active: 'View stewardship',
};
async function getMyActivityHandler(req, res) {
    const params = (0, validation_1.validateOrThrow)(querySchema, req.query);
    const qx = await (0, packagesDb_1.getPackagesQx)();
    const { rows, total } = await (0, data_access_layer_1.listMyActivity)(qx, {
        userId: req.actor.id,
        page: params.page,
        pageSize: params.pageSize,
        status: params.status,
    });
    (0, api_1.ok)(res, {
        data: rows.map((r) => {
            var _a;
            return ({
                stewardshipId: r.stewardshipId,
                packageName: r.packageName,
                purl: r.packagePurl,
                packageEcosystem: r.packageEcosystem,
                stewardshipStatus: r.stewardshipStatus,
                activityType: r.activityType,
                description: r.content,
                actor: r.actor,
                createdAt: r.createdAt,
                suggestedAction: (_a = SUGGESTED_ACTIONS[r.currentStewardshipStatus]) !== null && _a !== void 0 ? _a : null,
            });
        }),
        meta: {
            total,
            page: params.page,
            pageSize: params.pageSize,
        },
    });
}
//# sourceMappingURL=getMyActivity.js.map