"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.assignStewardHandler = assignStewardHandler;
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
    steward: zod_1.z
        .object({
        userId: zod_1.z.string().trim().min(1),
        username: zod_1.z.string().trim().min(1).optional().nullable(),
        displayName: zod_1.z.string().trim().min(1).optional().nullable(),
        role: zod_1.z.enum(['lead', 'co_steward']),
    })
        .refine((d) => (d.username == null) === (d.displayName == null), {
        message: 'username and displayName must both be provided or both be absent',
        path: ['displayName'],
    }),
    note: zod_1.z.string().trim().min(1).optional(),
    moveToAssessing: zod_1.z.boolean().optional().default(false),
    actor: actorSchema_1.actorInputSchema,
});
async function assignStewardHandler(req, res) {
    var _a, _b, _c;
    const { id } = (0, validation_1.validateOrThrow)(paramsSchema, req.params);
    const { steward, note, moveToAssessing, actor } = (0, validation_1.validateOrThrow)(bodySchema, req.body);
    const qx = await (0, packagesDb_1.getPackagesQx)();
    const result = await (0, data_access_layer_1.assignSteward)(qx, id, {
        userId: steward.userId,
        username: steward.username,
        displayName: steward.displayName,
        role: steward.role,
        note,
        assignedBy: req.actor.id,
        actorUsername: (_a = actor.username) !== null && _a !== void 0 ? _a : null,
        actorDisplayName: (_b = actor.displayName) !== null && _b !== void 0 ? _b : null,
        actorAvatarUrl: (_c = actor.avatarUrl) !== null && _c !== void 0 ? _c : null,
        moveToAssessing,
    });
    if (!result) {
        throw new common_1.NotFoundError(`Stewardship not found: ${id}`);
    }
    (0, api_1.ok)(res, { stewardship: result.stewardship, stewards: result.stewards });
}
//# sourceMappingURL=assignSteward.js.map