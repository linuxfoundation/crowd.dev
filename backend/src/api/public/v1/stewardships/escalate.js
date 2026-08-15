"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.escalateHandler = escalateHandler;
const zod_1 = require("zod");
const common_1 = require("@crowd/common");
const data_access_layer_1 = require("@crowd/data-access-layer");
const packagesDb_1 = require("@/db/packagesDb");
const api_1 = require("@/utils/api");
const validation_1 = require("@/utils/validation");
const actorSchema_1 = require("./actorSchema");
const paramsSchema = zod_1.z.object({
    id: zod_1.z.coerce.number().int().positive(),
});
const bodySchema = zod_1.z.object({
    resolutionPath: zod_1.z.enum(data_access_layer_1.ESCALATION_RESOLUTION_PATHS),
    notes: zod_1.z.string().trim().min(1).optional(),
    actor: actorSchema_1.actorInputSchema,
});
async function escalateHandler(req, res) {
    var _a, _b, _c;
    const { id } = (0, validation_1.validateOrThrow)(paramsSchema, req.params);
    const { resolutionPath, notes, actor } = (0, validation_1.validateOrThrow)(bodySchema, req.body);
    const qx = await (0, packagesDb_1.getPackagesQx)();
    const stewardship = await (0, data_access_layer_1.escalateStewardship)(qx, id, {
        resolutionPath,
        notes,
        actorUserId: req.actor.id,
        actorUsername: (_a = actor.username) !== null && _a !== void 0 ? _a : null,
        actorDisplayName: (_b = actor.displayName) !== null && _b !== void 0 ? _b : null,
        actorAvatarUrl: (_c = actor.avatarUrl) !== null && _c !== void 0 ? _c : null,
    });
    if (!stewardship) {
        throw new common_1.NotFoundError(`Stewardship not found: ${id}`);
    }
    (0, api_1.ok)(res, { stewardship });
}
//# sourceMappingURL=escalate.js.map