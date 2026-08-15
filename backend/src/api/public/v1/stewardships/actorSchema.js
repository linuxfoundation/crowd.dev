"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.actorInputSchema = void 0;
const zod_1 = require("zod");
exports.actorInputSchema = zod_1.z.object({
    username: zod_1.z.string().trim().min(1).optional().nullable(),
    displayName: zod_1.z.string().trim().min(1).optional().nullable(),
    avatarUrl: zod_1.z.string().url().optional().nullable(),
});
//# sourceMappingURL=actorSchema.js.map