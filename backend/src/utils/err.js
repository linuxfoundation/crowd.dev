"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.isMemberIdentityDbConflict = isMemberIdentityDbConflict;
exports.rethrowDbConflict = rethrowDbConflict;
const common_1 = require("@crowd/common");
const DB_CONFLICT_MAP = {
    uix_memberIdentities_platform_value_type_verified: (context) => new common_1.ConflictError('Identity already exists on another member', context),
    uix_memberIdentities_platform_type_lower_value_verified: (context) => new common_1.ConflictError('Identity already exists on another member', context),
};
function isMemberIdentityDbConflict(error) {
    var _a;
    return ((_a = (0, common_1.getDbConstraint)(error)) !== null && _a !== void 0 ? _a : '') in DB_CONFLICT_MAP;
}
function rethrowDbConflict(error, context) {
    var _a;
    const factory = DB_CONFLICT_MAP[(_a = (0, common_1.getDbConstraint)(error)) !== null && _a !== void 0 ? _a : ''];
    if (factory) {
        throw factory(context);
    }
    throw error;
}
//# sourceMappingURL=err.js.map