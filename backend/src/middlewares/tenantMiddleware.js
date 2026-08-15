"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.tenantMiddleware = tenantMiddleware;
const common_1 = require("@crowd/common");
const tenantService_1 = __importDefault(require("../services/tenantService"));
async function tenantMiddleware(req, res, next) {
    try {
        const tenantId = common_1.DEFAULT_TENANT_ID;
        const tenant = await new tenantService_1.default(req).findById(tenantId);
        req.currentTenant = tenant;
        next();
    }
    catch (error) {
        next(error);
    }
}
//# sourceMappingURL=tenantMiddleware.js.map