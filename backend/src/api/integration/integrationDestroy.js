"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const permissions_1 = __importDefault(require("../../security/permissions"));
const integrationService_1 = __importDefault(require("../../services/integrationService"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.integrationDestroy);
    await new integrationService_1.default(req).destroyAll(req.query.ids);
    const payload = true;
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=integrationDestroy.js.map