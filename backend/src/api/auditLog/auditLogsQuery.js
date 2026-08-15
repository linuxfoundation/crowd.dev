"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const auditLogsService_1 = __importDefault(require("@/services/auditLogsService"));
const permissions_1 = __importDefault(require("../../security/permissions"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.auditLogRead);
    const auditLogsService = new auditLogsService_1.default(req);
    const payload = await auditLogsService.query(req.body);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=auditLogsQuery.js.map