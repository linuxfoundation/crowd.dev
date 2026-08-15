"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const permissions_1 = __importDefault(require("../../../security/permissions"));
const integrationService_1 = __importDefault(require("../../../services/integrationService"));
const permissionChecker_1 = __importDefault(require("../../../services/user/permissionChecker"));
/**
 * GET /integration/:id/repositories
 * Unified endpoint to get repository mappings for any code platform integration
 * (github, gitlab, git, gerrit)
 */
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.tenantEdit);
    const payload = await new integrationService_1.default(req).getIntegrationRepositories(req.params.id);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=getIntegrationRepositories.js.map