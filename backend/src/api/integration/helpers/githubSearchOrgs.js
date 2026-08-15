"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const common_services_1 = require("@crowd/common_services");
const permissions_1 = __importDefault(require("@/security/permissions"));
const permissionChecker_1 = __importDefault(require("@/services/user/permissionChecker"));
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.integrationEdit);
    const payload = await common_services_1.GithubIntegrationService.findOrgs(req.query.query, req.query.limit, req.query.offset);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=githubSearchOrgs.js.map