"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const organizationService_1 = __importDefault(require("@/services/organizationService"));
const permissions_1 = __importDefault(require("../../security/permissions"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.organizationEdit);
    const payload = await new organizationService_1.default(req).unmerge(req.params.organizationId, req.body);
    await req.responseHandler.success(req, res, payload, 200);
};
//# sourceMappingURL=organizationUnmerge.js.map