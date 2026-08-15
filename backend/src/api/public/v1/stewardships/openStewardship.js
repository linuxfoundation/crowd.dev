"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.openStewardship = openStewardship;
const zod_1 = require("zod");
const common_1 = require("@crowd/common");
const data_access_layer_1 = require("@crowd/data-access-layer");
const packagesDb_1 = require("@/db/packagesDb");
const api_1 = require("@/utils/api");
const validation_1 = require("@/utils/validation");
const purl_1 = require("../packages/purl");
const actorSchema_1 = require("./actorSchema");
const bodySchema = zod_1.z.object({
    purl: purl_1.purlFieldSchema,
    actor: actorSchema_1.actorInputSchema,
});
async function openStewardship(req, res) {
    var _a, _b, _c;
    const { purl, actor } = (0, validation_1.validateOrThrow)(bodySchema, req.body);
    const qx = await (0, packagesDb_1.getPackagesQx)();
    const stewardship = await (0, data_access_layer_1.openStewardshipByPurl)(qx, purl, req.actor.id, (_a = actor.username) !== null && _a !== void 0 ? _a : null, (_b = actor.displayName) !== null && _b !== void 0 ? _b : null, (_c = actor.avatarUrl) !== null && _c !== void 0 ? _c : null);
    if (!stewardship) {
        throw new common_1.NotFoundError(`Package not found: ${purl}`);
    }
    (0, api_1.ok)(res, { stewardship });
}
//# sourceMappingURL=openStewardship.js.map