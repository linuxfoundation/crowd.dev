"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.activityFeedHandler = activityFeedHandler;
const zod_1 = require("zod");
const data_access_layer_1 = require("@crowd/data-access-layer");
const packagesDb_1 = require("@/db/packagesDb");
const api_1 = require("@/utils/api");
const validation_1 = require("@/utils/validation");
const querySchema = zod_1.z.object({
    page: zod_1.z.coerce.number().int().min(1).default(1),
    pageSize: zod_1.z.coerce.number().int().min(1).max(100).default(25),
});
async function activityFeedHandler(req, res) {
    const { page, pageSize } = (0, validation_1.validateOrThrow)(querySchema, req.query);
    const qx = await (0, packagesDb_1.getPackagesQx)();
    const { rows, total } = await (0, data_access_layer_1.listStewardshipActivity)(qx, { page, pageSize });
    (0, api_1.ok)(res, {
        rows: rows.map((r) => ({
            id: r.id,
            stewardshipId: r.stewardshipId,
            packagePurl: r.packagePurl,
            packageName: r.packageName,
            packageEcosystem: r.packageEcosystem,
            actor: r.actor,
            actorType: r.actorType,
            activityType: r.activityType,
            content: r.content,
            metadata: r.metadata,
            stewardshipStatus: r.stewardshipStatus,
            createdAt: r.createdAt,
        })),
        total,
        page,
        pageSize,
    });
}
//# sourceMappingURL=activityFeed.js.map