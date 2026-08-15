"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const dashboardService_1 = __importDefault(require("@/services/dashboardService"));
const permissions_1 = __importDefault(require("../../security/permissions"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.memberRead);
    const payload = await new dashboardService_1.default(req).get(req.query);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=dashboardGet.js.map