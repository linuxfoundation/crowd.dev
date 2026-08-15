"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.updateStatusHandler = updateStatusHandler;
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
const bodySchema = zod_1.z
    .object({
    status: zod_1.z.enum(data_access_layer_1.STEWARDSHIP_UPDATABLE_STATUSES),
    inactiveReason: zod_1.z.enum(data_access_layer_1.INACTIVE_REASONS).optional(),
    notes: zod_1.z.string().trim().min(1).optional(),
    actor: actorSchema_1.actorInputSchema,
})
    .refine((d) => d.status !== 'inactive' || !!d.inactiveReason, {
    message: 'inactiveReason is required when status is inactive',
    path: ['inactiveReason'],
});
async function updateStatusHandler(req, res) {
    var _a, _b, _c;
    const { id } = (0, validation_1.validateOrThrow)(paramsSchema, req.params);
    const { status, inactiveReason, notes, actor } = (0, validation_1.validateOrThrow)(bodySchema, req.body);
    const qx = await (0, packagesDb_1.getPackagesQx)();
    const stewardship = await (0, data_access_layer_1.updateStewardshipStatus)(qx, id, {
        status,
        inactiveReason,
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
//# sourceMappingURL=updateStatus.js.map