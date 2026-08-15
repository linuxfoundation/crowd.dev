"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const permissions_1 = __importDefault(require("../../../security/permissions"));
const integrationService_1 = __importDefault(require("../../../services/integrationService"));
const permissionChecker_1 = __importDefault(require("../../../services/user/permissionChecker"));
exports.default = async (req, res) => {
    var _a;
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.tenantEdit);
    const integrationData = {
        ...req.body,
        remotes: ((_a = req.body.remotes) === null || _a === void 0 ? void 0 : _a.map((remote) => ({ url: remote, forkedFrom: null }))) || [],
    };
    const payload = await new integrationService_1.default(req).gitConnectOrUpdate(integrationData);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=gitAuthenticate.js.map