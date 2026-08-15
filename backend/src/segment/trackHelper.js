"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.default = getTenatUser;
const sequelizeRepository_1 = __importDefault(require("../database/repositories/sequelizeRepository"));
function getTenatUser(userId, options) {
    let userIdOut;
    if (!userId) {
        // Get current user
        userIdOut = sequelizeRepository_1.default.getCurrentUser({
            ...options,
        }).id;
    }
    else {
        userIdOut = userId;
    }
    const tenantIdOut = sequelizeRepository_1.default.getCurrentTenant({ ...options }).id;
    return {
        userIdOut,
        tenantIdOut,
    };
}
//# sourceMappingURL=trackHelper.js.map