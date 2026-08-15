"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const incomingWebhookRepository_1 = __importDefault(require("../../../database/repositories/incomingWebhookRepository"));
const sequelizeRepository_1 = __importDefault(require("../../../database/repositories/sequelizeRepository"));
const permissions_1 = __importDefault(require("../../../security/permissions"));
const permissionChecker_1 = __importDefault(require("../../../services/user/permissionChecker"));
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.tenantEdit);
    const options = await sequelizeRepository_1.default.getDefaultIRepositoryOptions();
    const repo = new incomingWebhookRepository_1.default(options);
    const isWebhooksReceived = await repo.checkWebhooksExistForIntegration(req.body.integrationId);
    await req.responseHandler.success(req, res, { isWebhooksReceived });
};
//# sourceMappingURL=discourseTestWebhook.js.map