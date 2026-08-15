"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const common_1 = require("@crowd/common");
const permissions_1 = __importDefault(require("../../../security/permissions"));
const checkAPIKey_1 = require("../../../serverless/integrations/usecases/devto/checkAPIKey");
const getOrganization_1 = require("../../../serverless/integrations/usecases/devto/getOrganization");
const getUser_1 = require("../../../serverless/integrations/usecases/devto/getUser");
const permissionChecker_1 = __importDefault(require("../../../services/user/permissionChecker"));
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHasAny([
        permissions_1.default.values.integrationCreate,
        permissions_1.default.values.integrationEdit,
    ]);
    if (req.query.username) {
        const result = await (0, getUser_1.getUserByUsername)(req.query.username, req.query.apiKey);
        await req.responseHandler.success(req, res, result);
    }
    else if (req.query.organization) {
        const result = await (0, getOrganization_1.getOrganization)(req.query.organization, req.query.apiKey);
        await req.responseHandler.success(req, res, result);
    }
    else if (req.query.apiKey) {
        // validating the api key
        const result = await (0, checkAPIKey_1.checkAPIKey)(req.query.apiKey);
        await req.responseHandler.success(req, res, result);
    }
    else {
        // throw bad request since we don't have either of the query params
        await req.responseHandler.error(req, res, new common_1.Error400(req.language));
    }
};
//# sourceMappingURL=devtoValidators.js.map