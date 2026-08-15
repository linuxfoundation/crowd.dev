"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const collectionService_1 = require("@/services/collectionService");
const permissions_1 = __importDefault(require("../../../security/permissions"));
const permissionChecker_1 = __importDefault(require("../../../services/user/permissionChecker"));
/**
 * POST /collections/insights-projects/query
 * @summary Query insights projects
 * @tag Collections
 * @security Bearer
 * @description Query insights projects with filters and pagination
 * @bodyContent {InsightsProjectsQuery} application/json
 * @response 200 - Ok
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.collectionRead);
    const service = new collectionService_1.CollectionService(req);
    const payload = await service.queryInsightsProjects(req.body);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=insightsProjectsQuery.js.map