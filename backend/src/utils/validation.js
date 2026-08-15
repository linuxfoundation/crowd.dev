"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.validateOrThrow = validateOrThrow;
const common_1 = require("@crowd/common");
function validateOrThrow(schema, data) {
    const result = schema.safeParse(data);
    if (!result.success) {
        const messages = result.error.issues.map((issue) => {
            const path = issue.path.length ? `${issue.path.join('.')}: ` : '';
            return `${path}${issue.message}`;
        });
        throw new common_1.BadRequestError(messages.join('; '));
    }
    return result.data;
}
//# sourceMappingURL=validation.js.map